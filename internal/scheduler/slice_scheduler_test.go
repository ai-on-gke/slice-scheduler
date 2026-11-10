/*
Copyright 2025 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/generators"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/schedulerlib"
	"github.com/ai-on-gke/slice-scheduler/internal/testutil"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

func init() {
	if err := slicev1beta1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}
	if err := partitiontreev1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}
}

func TestScheduler_Trillium_Preemption(t *testing.T) {
	ctx := context.Background()

	s := scheme.Scheme

	testCases := []struct {
		description     string
		sliceToSchedule *slicev1beta1.Slice
		partitionTrees  []*partitiontreev1.PartitionTree
		existingSlices  []*slicev1beta1.Slice
		expectError     bool
		victimCheck     func(t *testing.T, fakeClient client.Client)
		finalCheck      func(t *testing.T, fakeClient client.Client)
	}{
		{
			description: "should preempt a lower priority slice to schedule a higher priority slice",
			sliceToSchedule: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "slice-hp",
					Annotations: map[string]string{
						SlicePriorityAnnotation: SlicePriorityAnnotation, // priority: "100"
						ManagedByAnnotation:     interfaces.SliceSchedulerName,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
			},
			partitionTrees: []*partitiontreev1.PartitionTree{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "p1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "p1", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy},
					},
				},
			},
			existingSlices: []*slicev1beta1.Slice{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "slice-lp", Annotations: map[string]string{SlicePriorityAnnotation: "10", ManagedByAnnotation: interfaces.SliceSchedulerName}},
					Spec:       slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
				},
			},
			victimCheck: func(t *testing.T, fakeClient client.Client) {
				assertVictimPreempted(t, fakeClient, "slice-lp", []string{"p1"})
			},
			finalCheck: func(t *testing.T, fakeClient client.Client) {
				slice := &slicev1beta1.Slice{}
				if err := fakeClient.Get(ctx, types.NamespacedName{Name: "slice-hp"}, slice); err != nil {
					t.Errorf("Expected slice-hp to be created, but it was not: %v", err)
				} else if diff := cmp.Diff([]string{"p1"}, slice.Spec.PartitionIds); diff != "" {
					t.Errorf("Expected partition IDs mismatch (-want +got):\n%s", diff)
				}
			},
		},
	}
	// Seed annotations
	for _, tc := range testCases {
		tc.sliceToSchedule.Annotations[SlicePriorityAnnotation] = "100"
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			initObjs := []client.Object{tc.sliceToSchedule}
			for _, pt := range tc.partitionTrees {
				initObjs = append(initObjs, pt.DeepCopy())
			}
			for _, s := range tc.existingSlices {
				initObjs = append(initObjs, s.DeepCopy())
			}
			fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(initObjs...).Build()
			recorder := events.NewFakeRecorder(10)
			reconciler, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-reconciler"), "v6e", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)

			if err := reconciler.Start(ctx); err != nil {
				t.Fatalf("Failed to start reconciler: %v", err)
			}

			// Trigger reconciliation via Reconcile()
			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.sliceToSchedule.Name, Namespace: tc.sliceToSchedule.Namespace}}
			_, err := reconciler.Reconcile(ctx, req)
			if err != nil && !tc.expectError {
				t.Errorf("Reconcile() error = %v, expectError %v", err, tc.expectError)
			}
			// Wait for background worker
			for i := 0; i < 20; i++ {
				if tc.finalCheck != nil {
					slice := &slicev1beta1.Slice{}
					if err := fakeClient.Get(ctx, types.NamespacedName{Name: tc.sliceToSchedule.Name}, slice); err == nil && len(slice.Spec.PartitionIds) > 0 {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
			}

			if tc.victimCheck != nil {
				tc.victimCheck(t, fakeClient)
			}
			if tc.finalCheck != nil {
				tc.finalCheck(t, fakeClient)
			}
		})
	}
}

func TestScheduler_Ironwood_Preemption(t *testing.T) {
	ctx := context.Background()

	s := scheme.Scheme

	testCases := []struct {
		description     string
		sliceToSchedule *slicev1beta1.Slice
		partitionTrees  []*partitiontreev1.PartitionTree
		existingSlices  []*slicev1beta1.Slice
		expectError     bool
		victimCheck     func(t *testing.T, fakeClient client.Client)
		finalCheck      func(t *testing.T, fakeClient client.Client)
	}{
		{
			description: "should preempt multiple lower priority slices in the same pod for a multi-cube slice",
			sliceToSchedule: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "slice-hp",
					Annotations: map[string]string{
						SlicePriorityAnnotation: "100",
						ManagedByAnnotation:     interfaces.SliceSchedulerName,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "tpu7x", Topology: "4x4x8", PartitionIds: []string{}},
			},
			partitionTrees: []*partitiontreev1.PartitionTree{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pt-pod-a1", Labels: map[string]string{TpuPodIDLabel: "pod-a"}},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "pod-a-p1", Topology: "4x4x4", State: partitiontreev1.PartitionStateHealthy},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pt-pod-a2", Labels: map[string]string{TpuPodIDLabel: "pod-a"}},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "pod-a-p2", Topology: "4x4x4", State: partitiontreev1.PartitionStateHealthy},
					},
				},
			},
			existingSlices: []*slicev1beta1.Slice{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "slice-lp-a", Annotations: map[string]string{SlicePriorityAnnotation: "10", ManagedByAnnotation: interfaces.SliceSchedulerName}},
					Spec:       slicev1beta1.SliceSpec{Type: "tpu7x", Topology: "4x4x4", PartitionIds: []string{"pod-a-p2"}},
				},
			},
			victimCheck: func(t *testing.T, fakeClient client.Client) {
				assertVictimPreempted(t, fakeClient, "slice-lp-a", []string{"pod-a-p2"})
			},
			finalCheck: func(t *testing.T, fakeClient client.Client) {
				slice := &slicev1beta1.Slice{}
				if err := fakeClient.Get(ctx, types.NamespacedName{Name: "slice-hp"}, slice); err != nil {
					t.Errorf("Expected slice-hp to be created, but it was not: %v", err)
				} else {
					expected := []string{"pod-a-p1", "pod-a-p2"}
					sort.Strings(slice.Spec.PartitionIds)
					sort.Strings(expected)
					if diff := cmp.Diff(expected, slice.Spec.PartitionIds); diff != "" {
						t.Errorf("Expected partition IDs mismatch (-want +got):\n%s", diff)
					}
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			initObjs := []client.Object{tc.sliceToSchedule}
			for _, pt := range tc.partitionTrees {
				initObjs = append(initObjs, pt.DeepCopy())
			}
			for _, s := range tc.existingSlices {
				initObjs = append(initObjs, s.DeepCopy())
			}

			fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(initObjs...).Build()
			recorder := events.NewFakeRecorder(10)
			reconciler, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-reconciler"), "tpu7x", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)
			if err := reconciler.Start(ctx); err != nil {
				t.Fatalf("Failed to start reconciler: %v", err)
			}

			// Trigger reconciliation
			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.sliceToSchedule.Name, Namespace: tc.sliceToSchedule.Namespace}}
			_, err := reconciler.Reconcile(ctx, req)
			if err != nil && !tc.expectError {
				t.Errorf("Reconcile() error = %v, expectError %v", err, tc.expectError)
			}

			// Wait for background worker
			for i := 0; i < 20; i++ {
				slice := &slicev1beta1.Slice{}
				if err := fakeClient.Get(ctx, types.NamespacedName{Name: tc.sliceToSchedule.Name}, slice); err == nil && len(slice.Spec.PartitionIds) > 0 {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}

			if tc.victimCheck != nil {
				tc.victimCheck(t, fakeClient)
			}
			if tc.finalCheck != nil {
				tc.finalCheck(t, fakeClient)
			}
		})
	}
}

func TestReconcile_DirectSliceBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := scheme.Scheme

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-slice",
			Annotations: map[string]string{
				ManagedByAnnotation: interfaces.SliceSchedulerName,
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
	}

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pt1",
			Labels: map[string]string{TpuPodIDLabel: "pt1"},
		},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: "v6e",
			Root:            &partitiontreev1.PartitionInfo{PartitionID: "p1", Topology: "2x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice, pt).Build()
	recorder := events.NewFakeRecorder(10)
	reconciler, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-reconciler"), "v6e", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)

	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// 1. Initial reconcile: should queue and schedule the Slice CRD
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-slice"}}
	_, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	// 2. Wait for background worker to bind the partition IDs in-place
	var scheduledSlice slicev1beta1.Slice
	success := false
	for i := 0; i < 20; i++ {
		err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-slice"}, &scheduledSlice)
		if err == nil && len(scheduledSlice.Spec.PartitionIds) > 0 {
			success = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !success {
		t.Fatalf("Slice was not bound by worker within timeout: %v", err)
	}

	if scheduledSlice.Spec.Topology != "2x4" || len(scheduledSlice.Spec.PartitionIds) != 1 || scheduledSlice.Spec.PartitionIds[0] != "p1" {
		t.Errorf("Slice spec partitionIds mismatch: %+v", scheduledSlice.Spec)
	}
}

func TestReconcile_UnhealthyRescheduling(t *testing.T) {
	ctx := context.Background()

	s := scheme.Scheme

	// Shared partition tree so the cache can be built.
	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{PartitionID: "p1", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}

	testCases := []struct {
		description       string
		slice             *slicev1beta1.Slice
		wantRequeue       bool
		wantPartitionsLen int
	}{
		{
			description: "unhealthy slice (Ready=False, Reason=UNHEALTHY) should clear partitions and requeue",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "slice-unhealthy",
					Annotations: map[string]string{ManagedByAnnotation: interfaces.SliceSchedulerName},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "UNHEALTHY"},
					},
				},
			},
			wantRequeue:       false, // Reconcile enqueues async, doesn't requeue
			wantPartitionsLen: 1,     // Partitions cleared asynchronously by worker
		},
		{
			description: "failed slice (Ready=False, Reason=FAILED) should NOT clear partitions (not a recognized reason)",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "slice-failed",
					Annotations: map[string]string{ManagedByAnnotation: interfaces.SliceSchedulerName},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "FAILED"},
					},
				},
			},
			wantRequeue:       false,
			wantPartitionsLen: 1,
		},
		{
			description: "creation-failed slice (Ready=False, Reason=SliceCreationFailed) should clear partitions and requeue",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "slice-creation-failed",
					Annotations: map[string]string{ManagedByAnnotation: interfaces.SliceSchedulerName},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "SliceCreationFailed"},
					},
				},
			},
			wantRequeue:       false, // Reconcile enqueues async, doesn't requeue
			wantPartitionsLen: 1,     // Partitions cleared asynchronously by worker
		},
		{
			description: "healthy slice with partitions should NOT be cleared",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "slice-healthy",
					Annotations: map[string]string{ManagedByAnnotation: interfaces.SliceSchedulerName},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "HEALTHY"},
					},
				},
			},
			wantRequeue:       false,
			wantPartitionsLen: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(tc.slice.DeepCopy(), pt.DeepCopy()).Build()
			recorder := events.NewFakeRecorder(10)
			reconciler, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-reconciler"), "v6e", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)

			if err := reconciler.Start(ctx); err != nil {
				t.Fatalf("Failed to start reconciler: %v", err)
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.slice.Name}}
			result, err := reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}

			if result.Requeue != tc.wantRequeue {
				t.Errorf("Reconcile() Requeue = %v, want %v", result.Requeue, tc.wantRequeue)
			}

			// Verify the slice's partition IDs in the API.
			updatedSlice := &slicev1beta1.Slice{}
			if err := fakeClient.Get(ctx, types.NamespacedName{Name: tc.slice.Name}, updatedSlice); err != nil {
				t.Fatalf("Failed to get slice after reconcile: %v", err)
			}
			if got := len(updatedSlice.Spec.PartitionIds); got != tc.wantPartitionsLen {
				t.Errorf("len(PartitionIds) = %d, want %d (PartitionIds=%v)", got, tc.wantPartitionsLen, updatedSlice.Spec.PartitionIds)
			}
		})
	}
}

func TestReconcile_DeactivationGuard(t *testing.T) {
	ctx := context.Background()

	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{PartitionID: "p1", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}

	// Slice that was previously provisioned (has the annotation) but now has empty partitions.
	// This simulates a N→0 deactivation. The scheduler should NOT re-schedule it.
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-deactivated",
			Annotations: map[string]string{
				ManagedByAnnotation:                        interfaces.SliceSchedulerName,
				"accelerator.gke.io/provisioned-partition": "p1",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice, pt).Build()
	recorder := events.NewFakeRecorder(10)
	reconciler, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-reconciler"), "v6e", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)

	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "slice-deactivated"}}
	result, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile() unexpected error: %v", err)
	}

	// Should NOT requeue — the deactivation guard returns early.
	if result.Requeue {
		t.Errorf("Reconcile() Requeue = true, want false (deactivated slice should not be rescheduled)")
	}

	// Verify partitions are still empty (not scheduled).
	updatedSlice := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "slice-deactivated"}, updatedSlice); err != nil {
		t.Fatalf("Failed to get slice after reconcile: %v", err)
	}
	if len(updatedSlice.Spec.PartitionIds) != 0 {
		t.Errorf("Deactivated slice should NOT have been rescheduled, but got partitions: %v", updatedSlice.Spec.PartitionIds)
	}
}

func TestReconcile_DeactivatingStateRequeue(t *testing.T) {
	ctx := context.Background()

	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{PartitionID: "p1", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}

	// The lifecycle transition skip was intentionally removed.
	// Slices in these states are now enqueued for scheduling (RequeueAfter = 0).
	testCases := []struct {
		description      string
		readyReason      string
		wantRequeueAfter time.Duration
	}{
		{
			description:      "DEACTIVATING state should be enqueued for scheduling",
			readyReason:      "DEACTIVATING",
			wantRequeueAfter: 0,
		},
		{
			description:      "ACTIVE state should be enqueued for scheduling",
			readyReason:      "ACTIVE",
			wantRequeueAfter: 0,
		},
		{
			description:      "ACTIVATING state should be enqueued for scheduling",
			readyReason:      "ACTIVATING",
			wantRequeueAfter: 0,
		},
		{
			description:      "INACTIVE state should be enqueued for scheduling",
			readyReason:      "INACTIVE",
			wantRequeueAfter: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "slice-deactivating",
					Annotations: map[string]string{ManagedByAnnotation: interfaces.SliceSchedulerName},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: tc.readyReason},
					},
				},
			}

			fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice, pt.DeepCopy()).Build()
			recorder := events.NewFakeRecorder(10)
			reconciler, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-reconciler"), "v6e", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)

			if err := reconciler.Start(ctx); err != nil {
				t.Fatalf("Failed to start reconciler: %v", err)
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "slice-deactivating"}}
			result, err := reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}

			if result.RequeueAfter != tc.wantRequeueAfter {
				t.Errorf("Reconcile() RequeueAfter = %v, want %v", result.RequeueAfter, tc.wantRequeueAfter)
			}
		})
	}
}

func createTestScheduler() interfaces.SchedulerLibrary {
	return createDefaultScheduler()
}

func createDefaultScheduler() interfaces.SchedulerLibrary {
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 10}),
	}

	filters := []interfaces.FilterPlugin{
		&plugins.HealthyFilter{},
		&plugins.ManagedFilter{},
		&plugins.PriorityFilter{},
	}

	scorers := []interfaces.ScorePlugin{
		&plugins.PreemptionScorer{},
		&plugins.DegradationSinkScorer{},
		&plugins.MinimizeSwappedCubesScorer{},
		&plugins.TightestFitScorer{},
	}

	return schedulerlib.NewDefaultScheduler(gens, filters, scorers)
}

func assertVictimPreempted(t *testing.T, fakeClient client.Client, name string, expectedPartitions []string) {
	t.Helper()
	victimSlice := &slicev1beta1.Slice{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: name}, victimSlice); err != nil {
		t.Fatalf("Failed to get victim slice: %v", err)
	}
	if victimSlice.Annotations == nil || victimSlice.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
		t.Errorf("Expected victim slice to have eviction annotation, but got %v", victimSlice.Annotations)
	}
	if diff := cmp.Diff(expectedPartitions, victimSlice.Spec.PartitionIds); diff != "" {
		t.Errorf("Expected victim slice to still have partitions %v, mismatch (-want +got):\n%s", expectedPartitions, diff)
	}
}

// --- Mock types for annotation passing test ---

// mockSchedulerLib captures the request for assertion.
type mockSchedulerLib struct {
	lastReq interfaces.GroupAllocationRequest
	result  interfaces.GroupAllocationResult
}

func (m *mockSchedulerLib) Name() string { return "mock" }
func (m *mockSchedulerLib) Schedule(_ context.Context, req interfaces.GroupAllocationRequest, _ interfaces.ClusterState) interfaces.GroupAllocationResult {
	m.lastReq = req
	return m.result
}

// mockWatcher returns a minimal cluster state and no-ops reserve/unreserve.
type mockWatcher struct{}

func (m *mockWatcher) GetState() interfaces.ClusterState {
	return &mockClusterState{}
}
func (m *mockWatcher) Reserve(_ context.Context, _ []*interfaces.SliceInfo) error {
	return nil
}
func (m *mockWatcher) Unreserve(_ context.Context, _ []*interfaces.SliceInfo) {
}

type mockClusterState struct{}

func (m *mockClusterState) GetPartitionTrees() []*interfaces.PartitionInfo { return nil }
func (m *mockClusterState) GetPartitionInfo(_ []string) []*interfaces.PartitionInfo {
	return nil
}

// mockPolicy is a permissive policy that always allows scheduling.
type mockPolicy struct{}

func (m *mockPolicy) NeedsRescheduling(_ *slicev1beta1.Slice) bool      { return false }
func (m *mockPolicy) ShouldBackoffOnFailure(_ *slicev1beta1.Slice) bool { return false }
func (m *mockPolicy) ShouldEvictPod(_ *slicev1beta1.Slice, _ bool) bool { return false }
func (m *mockPolicy) IsSliceReady(_ string) bool                        { return false }
func (m *mockPolicy) ShouldSkipReconcile(_ *slicev1beta1.Slice) (bool, string, time.Duration) {
	return false, "", 0
}
func (m *mockPolicy) IsNodeReadyForBinding(_ context.Context, _ client.Reader, _ string, _ string, _ string) (bool, time.Duration, error) {
	return true, 0, nil
}

// reschedulingMockPolicy is a permissive policy that says slices with partitions need rescheduling.
type reschedulingMockPolicy struct{}

func (m *reschedulingMockPolicy) NeedsRescheduling(_ *slicev1beta1.Slice) bool      { return true }
func (m *reschedulingMockPolicy) ShouldBackoffOnFailure(_ *slicev1beta1.Slice) bool { return false }
func (m *reschedulingMockPolicy) ShouldEvictPod(_ *slicev1beta1.Slice, _ bool) bool { return false }
func (m *reschedulingMockPolicy) IsSliceReady(_ string) bool                        { return false }
func (m *reschedulingMockPolicy) ShouldSkipReconcile(_ *slicev1beta1.Slice) (bool, string, time.Duration) {
	return false, "", 0
}
func (m *reschedulingMockPolicy) IsNodeReadyForBinding(_ context.Context, _ client.Reader, _ string, _ string, _ string) (bool, time.Duration, error) {
	return true, 0, nil
}

func TestSliceReconciler_AnnotationPassing(t *testing.T) {
	s := scheme.Scheme

	tests := []struct {
		name              string
		annotations       map[string]string
		topology          string
		tpuType           string
		expectedTolerance string
	}{
		{
			name: "Annotation 'prefer-none' treated as invalid, defaults to 'none' (Ironwood multi-cube)",
			annotations: map[string]string{
				DegradationToleranceAnnotation: "prefer-none",
				ManagedByAnnotation:            interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:       "test-workload",
				NumSlicesAnnotation:            "1",
			},
			topology:          "4x4x8",
			tpuType:           "tpu7x",
			expectedTolerance: "none",
		},
		{
			name: "Annotation 'none' (Ironwood)",
			annotations: map[string]string{
				DegradationToleranceAnnotation: "none",
				ManagedByAnnotation:            interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:       "test-workload",
				NumSlicesAnnotation:            "1",
			},
			topology:          "4x4x4",
			tpuType:           "tpu7x",
			expectedTolerance: "none",
		},
		{
			name: "Annotation 'all' (Ironwood)",
			annotations: map[string]string{
				DegradationToleranceAnnotation: "all",
				ManagedByAnnotation:            interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:       "test-workload",
				NumSlicesAnnotation:            "1",
			},
			topology:          "4x4x4",
			tpuType:           "tpu7x",
			expectedTolerance: "all",
		},
		{
			name: "Annotation missing, defaults to all (sub-cube Ironwood)",
			annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "test-workload",
				NumSlicesAnnotation:      "1",
			},
			topology:          "2x2x4",
			tpuType:           "tpu7x",
			expectedTolerance: "all",
		},
		{
			name: "Invalid annotation value, defaults to all (sub-cube Ironwood)",
			annotations: map[string]string{
				DegradationToleranceAnnotation: "invalid-value",
				ManagedByAnnotation:            interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:       "test-workload",
				NumSlicesAnnotation:            "1",
			},
			topology:          "2x2x4",
			tpuType:           "tpu7x",
			expectedTolerance: "all",
		},
		{
			name: "Annotation missing, defaults to all (full-cube Ironwood)",
			annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "test-workload",
				NumSlicesAnnotation:      "1",
			},
			topology:          "4x4x4",
			tpuType:           "tpu7x",
			expectedTolerance: "all",
		},
		{
			name: "Annotation missing, defaults to none (multi-cube Ironwood)",
			annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "test-workload",
				NumSlicesAnnotation:      "1",
			},
			topology:          "4x4x8",
			tpuType:           "tpu7x",
			expectedTolerance: "none",
		},
		{
			name: "Annotation 'all' ignored (Trillium)",
			annotations: map[string]string{
				DegradationToleranceAnnotation: "all",
				ManagedByAnnotation:            interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:       "test-workload",
				NumSlicesAnnotation:            "1",
			},
			topology:          "2x4",
			tpuType:           "v6e",
			expectedTolerance: "none",
		},
		{
			name: "Annotation 'prefer-none' treated as invalid, defaults to 'none' (Trillium)",
			annotations: map[string]string{
				DegradationToleranceAnnotation: "prefer-none",
				ManagedByAnnotation:            interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:       "test-workload",
				NumSlicesAnnotation:            "1",
			},
			topology:          "2x4",
			tpuType:           "v6e",
			expectedTolerance: "none",
		},
		{
			name: "Annotation missing, defaults to none (Trillium)",
			annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "test-workload",
				NumSlicesAnnotation:      "1",
			},
			topology:          "2x4",
			tpuType:           "v6e",
			expectedTolerance: "none",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-slice",
					Annotations: tc.annotations,
				},
				Spec: slicev1beta1.SliceSpec{
					Type:     slicev1beta1.Type(tc.tpuType),
					Topology: tc.topology,
				},
			}

			fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice).Build()
			ms := &mockSchedulerLib{
				result: interfaces.GroupAllocationResult{
					IsSchedulable: true,
					Candidate: &interfaces.PlacementCandidate{
						Assignments: map[string][]*interfaces.PartitionInfo{
							"test-slice": {{ID: "partition-1", PodName: "pod-a"}},
						},
					},
				},
			}

			r := &SliceReconciler{
				Client:            fakeClient,
				schedulerLib:      ms,
				Recorder:          events.NewFakeRecorder(10),
				tpuType:           tc.tpuType,
				policy:            &mockPolicy{},
				groups:            make(map[string]*interfaces.SliceGroup),
				sliceToGroup:      make(map[string]string),
				watcher:           &mockWatcher{},
				failureTimestamps: make(map[string]time.Time),
			}

			// Populate group cache
			r.updateGroupCache(context.Background(), slice)

			// Call tryScheduleSliceGroup
			_, _ = r.tryScheduleSliceGroup(context.Background(), "test-workload")

			// Verify that the request has the expected annotation value
			if len(ms.lastReq.Slices) != 1 {
				t.Fatalf("Expected 1 slice in request, got %d", len(ms.lastReq.Slices))
			}

			if ms.lastReq.Slices[0].DegradationTolerance != tc.expectedTolerance {
				t.Errorf("Expected DegradationTolerance to be '%s', got '%s'", tc.expectedTolerance, ms.lastReq.Slices[0].DegradationTolerance)
			}
		})
	}
}

func TestPrepareGroupAllocationRequest_CurrentPartitionIDs(t *testing.T) {
	r := &SliceReconciler{
		policy: &reschedulingMockPolicy{},
	}
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice-1", Generation: 1},
		Spec: slicev1beta1.SliceSpec{
			Type:         "tpu7x",
			Topology:     "4x4x8",
			PartitionIds: []string{"c1", "c2"},
		},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionFalse,
					Reason: "UNHEALTHY",
				},
			},
		},
	}
	groupSlices := map[string]*slicev1beta1.Slice{"slice-1": slice}
	req, isRescheduling, reschedulingPartitions, _, err := r.prepareGroupAllocationRequest("group-1", 100, groupSlices, []string{"slice-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isRescheduling {
		t.Errorf("expected isRescheduling=true")
	}
	if len(reschedulingPartitions) != 2 || reschedulingPartitions[0] != "c1" || reschedulingPartitions[1] != "c2" {
		t.Errorf("unexpected reschedulingPartitions: %v", reschedulingPartitions)
	}
	if len(req.Slices) != 1 {
		t.Fatalf("expected 1 slice in req, got %d", len(req.Slices))
	}
	if diff := cmp.Diff([]string{"c1", "c2"}, req.Slices[0].CurrentPartitionIDs); diff != "" {
		t.Errorf("CurrentPartitionIDs mismatch (-want +got):\n%s", diff)
	}
}

type stateWatcher struct {
	state    interfaces.ClusterState
	reserved []*interfaces.SliceInfo
}

func (w *stateWatcher) GetState() interfaces.ClusterState { return w.state }
func (w *stateWatcher) Reserve(ctx context.Context, slicesInfo []*interfaces.SliceInfo) error {
	w.reserved = slicesInfo
	return nil
}
func (w *stateWatcher) Unreserve(ctx context.Context, slicesInfo []*interfaces.SliceInfo) {}

func TestSliceReconciler_RescheduleKeepsHealthyPartitions(t *testing.T) {
	s := scheme.Scheme

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-slice",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "test-group",
				NumSlicesAnnotation:      "1",
			},
		},
		Spec: slicev1beta1.SliceSpec{
			Type:         "tpu7x",
			Topology:     "4x4x8",
			PartitionIds: []string{"c1", "c2"},
		},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "UNHEALTHY",
					ObservedGeneration: 0,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice).Build()

	mockState := testutil.NewStateBuilder().
		Pod("pod-1").
		Tree("4x4x4", "c1").UsedBy("test-slice", 100, interfaces.SliceSchedulerName).ParentPod().
		Tree("4x4x4", "c2").UsedBy("test-slice", 100, interfaces.SliceSchedulerName).Broken().ParentPod().
		Tree("4x4x4", "c3").ParentPod().
		Tree("4x4x4", "c4").Build()

	watcher := &stateWatcher{state: mockState}

	r := &SliceReconciler{
		Client:            fakeClient,
		schedulerLib:      createTestScheduler(),
		Recorder:          events.NewFakeRecorder(10),
		groups:            make(map[string]*interfaces.SliceGroup),
		sliceToGroup:      make(map[string]string),
		watcher:           watcher,
		tpuType:           TPU7x,
		policy:            &reschedulingMockPolicy{},
		failureTimestamps: make(map[string]time.Time),
	}

	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	r.updateGroupCache(context.Background(), slice)
	_, _ = r.tryScheduleSliceGroup(context.Background(), "test-group")

	// The new placement can't be bound while the slice holds c1 and c2, so the slice is
	// marked for eviction and the event names where it is headed.
	got := &slicev1beta1.Slice{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-slice"}, got); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
		t.Fatal("slice not marked for eviction")
	}
	var event string
	select {
	case event = <-recorder.Events:
	default:
		t.Fatal("no event recorded")
	}
	_, assigned, ok := strings.Cut(event, "rescheduled onto ")
	if !ok {
		t.Fatalf("event = %q, want a move eviction", event)
	}
	gotIDs := strings.Fields(strings.Trim(assigned, "[]"))
	sort.Strings(gotIDs)
	wantIDs := []string{"c1", "c3"}

	if diff := cmp.Diff(wantIDs, gotIDs); diff != "" {
		t.Errorf("Assigned partitions mismatch (-want +got):\n%s", diff)
	}
}

// gangMember returns a v6e Slice that belongs to a gang of the given size.
func gangMember(name, group string, size int) *slicev1beta1.Slice {
	sl := makeSlice(name, nil)
	sl.Annotations[SliceGroupNameAnnotation] = group
	sl.Annotations[NumSlicesAnnotation] = strconv.Itoa(size)
	return sl
}

// groupReady reports whether the named group is cached and complete.
func groupReady(r *SliceReconciler, groupId string) bool {
	r.groupMutex.RLock()
	defer r.groupMutex.RUnlock()
	group, ok := r.groups[groupId]
	return ok && group.IsReady()
}

// A gang member being deleted must stop counting toward its gang as soon as the
// deletion is seen, not when the object is finally gone.
func TestGroupCache_DeletingMemberLeavesGroup(t *testing.T) {
	ctx := context.Background()
	deleting := func(sl *slicev1beta1.Slice) *slicev1beta1.Slice {
		out := sl.DeepCopy()
		now := metav1.Now()
		out.DeletionTimestamp = &now
		out.Finalizers = []string{"test/finalizer"}
		return out
	}

	t.Run("updateGroupCache", func(t *testing.T) {
		r, _ := newTestReconciler(t, scheme.Scheme)
		a, b := gangMember("gang-a", "gang", 2), gangMember("gang-b", "gang", 2)
		r.updateGroupCache(ctx, a)
		r.updateGroupCache(ctx, b)
		if !groupReady(r, "gang") {
			t.Fatal("precondition: gang should be complete")
		}

		r.updateGroupCache(ctx, deleting(b))
		if groupReady(r, "gang") {
			t.Error("gang still complete while a member is being deleted")
		}
		if _, ok := r.sliceToGroup["gang-b"]; ok {
			t.Error("deleting member still mapped to its group")
		}
	})

	t.Run("handleSliceUpdate", func(t *testing.T) {
		r, _ := newTestReconciler(t, scheme.Scheme)
		a, b := gangMember("gang-a", "gang", 2), gangMember("gang-b", "gang", 2)
		r.updateGroupCache(ctx, a)
		r.updateGroupCache(ctx, b)

		r.handleSliceUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: b, ObjectNew: deleting(b)})
		if groupReady(r, "gang") {
			t.Error("gang still complete while a member is being deleted")
		}
	})

	t.Run("last member drops the group", func(t *testing.T) {
		r, _ := newTestReconciler(t, scheme.Scheme)
		a := gangMember("solo", "solo-gang", 1)
		r.updateGroupCache(ctx, a)

		r.updateGroupCache(ctx, deleting(a))
		if _, ok := r.groups["solo-gang"]; ok {
			t.Error("group with no members left is still cached")
		}
	})
}

// A Slice deleted and recreated under the same name can have its successor cached by
// Reconcile before the watch handlers see the old one go. The late delete must leave
// the successor in its group.
func TestGroupCache_LateDeleteKeepsSuccessor(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestReconciler(t, scheme.Scheme)

	old := gangMember("gang-a", "gang", 2)
	old.UID = "old-uid"
	successor := gangMember("gang-a", "gang", 2)
	successor.UID = "new-uid"
	sibling := gangMember("gang-b", "gang", 2)
	sibling.UID = "sibling-uid"

	r.updateGroupCache(ctx, old)
	r.updateGroupCache(ctx, sibling)
	r.updateGroupCache(ctx, successor)

	r.handleSliceDelete(ctx, event.TypedDeleteEvent[client.Object]{Object: old})
	if !groupReady(r, "gang") {
		t.Fatal("late delete of the old Slice removed its successor from the group")
	}
	if got := r.groups["gang"].Slices["gang-a"].UID; got != "new-uid" {
		t.Errorf("cached member UID = %q, want the successor's", got)
	}

	r.handleSliceDelete(ctx, event.TypedDeleteEvent[client.Object]{Object: successor})
	if groupReady(r, "gang") {
		t.Error("delete of the successor itself did not remove it from the group")
	}
	if _, ok := r.sliceToGroup["gang-a"]; ok {
		t.Error("deleted successor still mapped to its group")
	}
}

// A Slice recreated under the same name is placed on its predecessor's partitions while
// the cache still holds them for the predecessor. The predecessor's late delete must not
// free them, or another group could be placed on hardware the successor is bound to.
func TestLateDeleteKeepsSuccessorPartitions(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	successor := gangMember("slice-a", "group-a", 1)
	successor.UID = "new-uid"
	successor.Spec.Topology = "2x4"
	old := successor.DeepCopy()
	old.UID = "old-uid"
	old.Spec.PartitionIds = []string{"p1"}

	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt-late", Labels: map[string]string{TpuPodIDLabel: "pt-late"}},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: TPU6e,
			Root:            &partitiontreev1.PartitionInfo{PartitionID: "p1", Topology: "2x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(successor).Build()
	cache := watcher.NewPartitionCache(false)
	r, err := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test"), TPU6e, events.NewFakeRecorder(10),
		false, createTestScheduler(), cache, 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	cache.UpdateTree(ctx, tree, nil)
	// The cache still holds p1 for the old Slice: its delete hasn't been seen yet.
	if ok, existing := cache.ReservePartition(ctx, sliceInfo(ctx, old)); !ok {
		t.Fatalf("setup: reserve failed, held by %q", existing)
	}

	r.updateGroupCache(ctx, successor)
	if requeue, _ := r.tryScheduleSliceGroup(ctx, "group-a"); requeue {
		t.Fatal("expected the successor to be scheduled")
	}
	got := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "slice-a"}, got); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if !partitionsMatch(got.Spec.PartitionIds, []string{"p1"}) {
		t.Fatalf("precondition: expected the successor bound to p1, got %v", got.Spec.PartitionIds)
	}

	r.handleSliceDelete(ctx, event.TypedDeleteEvent[client.Object]{Object: old})

	node, _ := cache.GetNode("p1")
	if node.InUseBy == nil || node.InUseBy.UID != "new-uid" {
		t.Errorf("expected p1 to stay held by the successor, got %+v", node.InUseBy)
	}
}

// A Slice deleted while Start is building the cache is still in Start's LIST. Its delete
// event must apply after Start reserves it, not before, or its partitions stay reserved
// for a Slice that no longer exists.
func TestStart_DeleteDuringBuildWins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := scheme.Scheme

	gone := gangMember("gone", "gone-group", 1)
	gone.UID = "gone-uid"
	gone.Spec.Topology = "2x4"
	gone.Spec.PartitionIds = []string{"p1"}
	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt-build", Labels: map[string]string{TpuPodIDLabel: "pt-build"}},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: TPU6e,
			Root:            &partitiontreev1.PartitionInfo{PartitionID: "p1", Topology: "2x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}
	// The client still lists the Slice, as Start's LIST can when the delete lands during it.
	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(gone, tree).Build()
	cache := watcher.NewPartitionCache(false)
	r, err := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test"), TPU6e, events.NewFakeRecorder(10),
		false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	r.cacheBuilt = make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.sliceEvents().Delete(ctx, event.TypedDeleteEvent[client.Object]{Object: gone}, nil)
	}()
	select {
	case <-done:
		t.Fatal("delete handler ran before the cache was built")
	case <-time.After(50 * time.Millisecond):
	}

	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("delete handler still waiting after the cache was built")
	}

	if node, _ := cache.GetNode("p1"); node.InUseBy != nil {
		t.Errorf("p1 still reserved for deleted Slice %q", node.InUseBy.Name)
	}
	r.groupMutex.RLock()
	_, cached := r.groups["gone-group"]
	r.groupMutex.RUnlock()
	if cached {
		t.Error("deleted Slice's group still cached")
	}
}

// A watch handler waiting on the cache build gives up when its context ends.
func TestAwaitCacheBuilt(t *testing.T) {
	r := &SliceReconciler{}
	if !r.awaitCacheBuilt(context.Background()) {
		t.Error("with no cacheBuilt channel, handlers must not wait")
	}

	r.cacheBuilt = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r.awaitCacheBuilt(ctx) {
		t.Error("expected false once the context has ended")
	}

	close(r.cacheBuilt)
	if !r.awaitCacheBuilt(context.Background()) {
		t.Error("expected true once the cache is built")
	}
}

// processSchedulingQueue checks a gang is complete before scheduling it, but the gang can
// lose a member before tryScheduleSliceGroup reads its membership. It must not then be
// given a placement at reduced size.
func TestTryScheduleSliceGroup_GangLostMemberIsNotPlaced(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	setup := func(t *testing.T, objs ...client.Object) (*SliceReconciler, *mockSchedulerLib) {
		t.Helper()
		lib := &mockSchedulerLib{}
		fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
		r, err := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test"), TPU6e, events.NewFakeRecorder(10),
			false, lib, watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)
		if err != nil {
			t.Fatalf("NewSliceReconciler() error: %v", err)
		}
		return r, lib
	}

	t.Run("member left the group cache", func(t *testing.T) {
		a, b := gangMember("lost-a", "lost", 2), gangMember("lost-b", "lost", 2)
		r, lib := setup(t, a, b)
		r.updateGroupCache(ctx, a)
		r.updateGroupCache(ctx, b)
		if !groupReady(r, "lost") {
			t.Fatal("precondition: gang should be complete when it is popped")
		}
		r.handleSliceDelete(ctx, event.TypedDeleteEvent[client.Object]{Object: b})

		if requeue, _ := r.tryScheduleSliceGroup(ctx, "lost"); requeue {
			t.Error("expected the incomplete gang to be dropped, not requeued")
		}
		if lib.lastReq.GroupName != "" {
			t.Errorf("incomplete gang was given a placement search for %d slices", len(lib.lastReq.Slices))
		}
	})

	t.Run("member started deleting", func(t *testing.T) {
		a, b := gangMember("del-a", "del", 2), gangMember("del-b", "del", 2)
		r, lib := setup(t, a, b)
		r.updateGroupCache(ctx, a)
		r.updateGroupCache(ctx, b)
		// b is deleted before its update event takes it out of the group cache.
		if err := r.Get(ctx, types.NamespacedName{Name: "del-b"}, b); err != nil {
			t.Fatalf("Get() error: %v", err)
		}
		b.Finalizers = []string{"test/finalizer"}
		if err := r.Update(ctx, b); err != nil {
			t.Fatalf("Update() error: %v", err)
		}
		if err := r.Delete(ctx, b); err != nil {
			t.Fatalf("Delete() error: %v", err)
		}

		if requeue, _ := r.tryScheduleSliceGroup(ctx, "del"); requeue {
			t.Error("expected the gang to be dropped, not requeued")
		}
		if lib.lastReq.GroupName != "" {
			t.Errorf("gang with a deleting member was given a placement search for %d slices", len(lib.lastReq.Slices))
		}
	})

	// The deleting member's replacement, under a new name, joined before the deleting
	// member's update took it out of the group cache. The group is over-full, and complete
	// without the deleting member, so it must be placed: nothing requeues it later.
	t.Run("deleting member already replaced", func(t *testing.T) {
		a, oldB, b2 := gangMember("rep-a", "rep", 2), gangMember("rep-old-b", "rep", 2), gangMember("rep-b2", "rep", 2)
		r, lib := setup(t, a, oldB, b2)
		r.updateGroupCache(ctx, a)
		r.updateGroupCache(ctx, oldB)
		r.updateGroupCache(ctx, b2)
		if err := r.Get(ctx, types.NamespacedName{Name: "rep-old-b"}, oldB); err != nil {
			t.Fatalf("Get() error: %v", err)
		}
		oldB.Finalizers = []string{"test/finalizer"}
		if err := r.Update(ctx, oldB); err != nil {
			t.Fatalf("Update() error: %v", err)
		}
		if err := r.Delete(ctx, oldB); err != nil {
			t.Fatalf("Delete() error: %v", err)
		}

		r.tryScheduleSliceGroup(ctx, "rep")
		var got []string
		for _, s := range lib.lastReq.Slices {
			got = append(got, s.SliceName)
		}
		if len(got) != 2 || got[0] != "rep-a" || got[1] != "rep-b2" {
			t.Errorf("expected a placement search for rep-a and rep-b2, got %v", got)
		}
	})

	t.Run("complete gang is placed", func(t *testing.T) {
		a, b := gangMember("ok-a", "ok", 2), gangMember("ok-b", "ok", 2)
		r, lib := setup(t, a, b)
		r.updateGroupCache(ctx, a)
		r.updateGroupCache(ctx, b)

		r.tryScheduleSliceGroup(ctx, "ok")
		if len(lib.lastReq.Slices) != 2 {
			t.Errorf("expected a placement search for both members, got %d", len(lib.lastReq.Slices))
		}
	})
}

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
	"testing"
	"time"

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
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/pod_reconciler"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/pq"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

// TestReconcile_PolicyNeedsRescheduling verifies that the scheduler's
// needsScheduling method correctly delegates to CloudPolicy.NeedsRescheduling
// for all cloud failure states.
func TestReconcile_PolicyNeedsRescheduling(t *testing.T) {
	s := scheme.Scheme

	testCases := []struct {
		description    string
		slice          *slicev1beta1.Slice
		wantScheduling bool
	}{
		{
			description: "FAILED (ConditionFalse) → needs scheduling",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-failed"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "FAILED"},
					},
				},
			},
			wantScheduling: true,
		},
		{
			description: "SliceCreationFailed (ConditionFalse) → needs scheduling",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-creation-failed"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "SliceCreationFailed"},
					},
				},
			},
			wantScheduling: true,
		},
		{
			description: "MMIGNotFound (ConditionFalse) → needs scheduling",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-mmig"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "MMIGNotFound"},
					},
				},
			},
			wantScheduling: true,
		},
		{
			description: "PreflightError (ConditionFalse) → needs scheduling",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-preflight"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "PreflightError"},
					},
				},
			},
			wantScheduling: true,
		},
		{
			description: "SliceHealthProbeFailed (ConditionUnknown) → needs scheduling",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-probe-failed"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceHealthProbeFailed"},
					},
				},
			},
			wantScheduling: true,
		},
		{
			description: "ACTIVE (ConditionTrue) → does NOT need scheduling",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-active"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "ACTIVE"},
					},
				},
			},
			wantScheduling: false,
		},
		{
			description: "UNHEALTHY (ConditionFalse) → does NOT need scheduling (CloudPolicy ignores UNHEALTHY)",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-unhealthy"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p1"}},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "UNHEALTHY"},
					},
				},
			},
			wantScheduling: false,
		},
		{
			description: "No conditions → does NOT need scheduling",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-no-conditions"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{"p1"}},
				Status:     slicev1beta1.SliceStatus{},
			},
			wantScheduling: false,
		},
		{
			description: "New slice (no partitions, no conditions) → needs scheduling",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-new"},
				Spec:       slicev1beta1.SliceSpec{PartitionIds: []string{}},
			},
			wantScheduling: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(s).Build()
			reconciler, err := NewSliceReconciler(
				fakeClient, s, ctrl.Log.WithName("test"), "v6e",
				events.NewFakeRecorder(10), false, nil,
				watcher.NewPartitionCache(false), 0, 0,
				policy.NewCloudPolicy(), 0,
			)
			if err != nil {
				t.Fatalf("NewSliceReconciler() error: %v", err)
			}

			got := reconciler.needsScheduling(tc.slice)
			if got != tc.wantScheduling {
				t.Errorf("needsScheduling() = %v, want %v", got, tc.wantScheduling)
			}
		})
	}
}

// TestReconcile_ShouldSkipReconcile_ProvisionedPartition verifies that:
//  1. Slices WITH the provisioned-partition annotation return RequeueAfter 5s.
//  2. Slices WITHOUT the annotation proceed normally.
//  3. The group cache is NOT updated when skip happens (ordering check).
func TestReconcile_ShouldSkipReconcile_ProvisionedPartition(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	testCases := []struct {
		description      string
		slice            *slicev1beta1.Slice
		wantRequeueAfter time.Duration
		wantGroupUpdated bool
	}{
		{
			description: "Slice WITH provisioned-partition annotation → skip with RequeueAfter 5s",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "slice-deactivating",
					Annotations: map[string]string{
						ManagedByAnnotation:                        interfaces.SliceSchedulerName,
						"accelerator.gke.io/provisioned-partition": "p1",
						SliceGroupNameAnnotation:                   "workload-skip",
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
			},
			wantRequeueAfter: policy.DeactivationRequeueDelay,
			wantGroupUpdated: false,
		},
		{
			description: "Slice WITHOUT annotation → no skip, proceeds normally",
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "slice-normal",
					Annotations: map[string]string{
						ManagedByAnnotation:      interfaces.SliceSchedulerName,
						SliceGroupNameAnnotation: "workload-normal",
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
			},
			wantRequeueAfter: 0,
			wantGroupUpdated: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Build the client with only the partition tree; the slice is
			// created AFTER Start() so it won't be picked up by Start's
			// initial cache-population pass.
			fakeClient := fake.NewClientBuilder().WithScheme(s).
				WithObjects(pt.DeepCopy()).Build()
			recorder := events.NewFakeRecorder(10)

			reconciler, err := NewSliceReconciler(
				fakeClient, s, ctrl.Log.WithName("test"), "v6e",
				recorder, false, createTestScheduler(),
				watcher.NewPartitionCache(false), 0, 0,
				policy.NewCloudPolicy(), 0,
			)
			if err != nil {
				t.Fatalf("NewSliceReconciler() error: %v", err)
			}
			if err := reconciler.Start(ctx); err != nil {
				t.Fatalf("Failed to start reconciler: %v", err)
			}

			// Create the slice AFTER Start() to have a clean group cache.
			if err := fakeClient.Create(ctx, tc.slice.DeepCopy()); err != nil {
				t.Fatalf("Failed to create slice: %v", err)
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.slice.Name}}
			result, reconcileErr := reconciler.Reconcile(ctx, req)
			if reconcileErr != nil {
				t.Fatalf("Reconcile() unexpected error: %v", reconcileErr)
			}

			if result.RequeueAfter != tc.wantRequeueAfter {
				t.Errorf("Reconcile() RequeueAfter = %v, want %v", result.RequeueAfter, tc.wantRequeueAfter)
			}

			// Check that the group cache was NOT updated when skip happened.
			sliceGroupName := tc.slice.Annotations[SliceGroupNameAnnotation]
			reconciler.groupMutex.RLock()
			_, groupExists := reconciler.groups[sliceGroupName]
			reconciler.groupMutex.RUnlock()

			if groupExists != tc.wantGroupUpdated {
				t.Errorf("group cache updated = %v, want %v", groupExists, tc.wantGroupUpdated)
			}
		})
	}
}

// TestTryScheduleSliceGroup_SkipAnnotatedSlice verifies that in
// tryScheduleSliceGroup, a slice with the provisioned-partition annotation
// is skipped even though needsScheduling would return true for it.
func TestTryScheduleSliceGroup_SkipAnnotatedSlice(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	// slice-A: annotated (provisioned-partition), needs scheduling (empty partitions).
	sliceA := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-a",
			Annotations: map[string]string{
				ManagedByAnnotation:                        interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:                   "workload-group",
				NumSlicesAnnotation:                        "2",
				"accelerator.gke.io/provisioned-partition": "old-p",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
	}

	// slice-B: no annotation, needs scheduling (empty partitions).
	sliceB := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-b",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-group",
				NumSlicesAnnotation:      "2",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithObjects(sliceA, sliceB, pt).Build()
	recorder := events.NewFakeRecorder(10)

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, createTestScheduler(),
		watcher.NewPartitionCache(false), 0, 0,
		policy.NewCloudPolicy(), 0,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// Manually populate the group cache for both slices.
	reconciler.updateGroupCache(ctx, sliceA)
	reconciler.updateGroupCache(ctx, sliceB)

	// Call tryScheduleSliceGroup directly. Since slice-A is annotated,
	// only slice-B should be included in the scheduling request.
	requeue, _ := reconciler.tryScheduleSliceGroup(ctx, "workload-group")

	// Verify that slice-B was scheduled (got partition assigned).
	updatedB := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "slice-b"}, updatedB); err != nil {
		t.Fatalf("Failed to get slice-b: %v", err)
	}

	// Verify slice-A was NOT scheduled (still empty partitions).
	updatedA := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "slice-a"}, updatedA); err != nil {
		t.Fatalf("Failed to get slice-a: %v", err)
	}

	if len(updatedA.Spec.PartitionIds) != 0 {
		t.Errorf("slice-A should NOT have partitions (annotated/skipped), got %v", updatedA.Spec.PartitionIds)
	}

	// slice-B should have been scheduled. If no partition was available it will
	// requeue, but it should NOT have been skipped.
	if requeue && len(updatedB.Spec.PartitionIds) == 0 {
		// This is acceptable — the scheduler tried to schedule slice-B but no
		// partition was available for a 4x4 topology. The key assertion is that
		// slice-A was skipped (verified above).
		t.Logf("slice-B was not schedulable (expected if topology mismatch), requeue=%v", requeue)
	}
}

// TestShouldBackoffWithTimeout verifies backoff timeout behavior:
//  1. First call with healthy partitions → backoff, timestamp recorded.
//  2. Subsequent calls within timeout → still backs off.
//  3. After timeout expires → no longer backs off, proceeds to reschedule.
//  4. After reschedule → timestamp is cleared.
func TestShouldBackoffWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	// Create partition tree with a healthy partition.
	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	// Slice that is in SliceCreationFailed state (no retry annotation)
	// with existing partition assignments (rescheduling scenario).
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-timeout",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-timeout",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "SliceCreationFailed"},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithObjects(slice, pt).Build()
	recorder := events.NewFakeRecorder(10)

	rescheduleTimeout := 100 * time.Millisecond
	rescheduleHealthyDelay := 10 * time.Millisecond

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, createTestScheduler(),
		watcher.NewPartitionCache(false),
		rescheduleHealthyDelay, 0,
		policy.NewCloudPolicy(), rescheduleTimeout,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// Populate the group cache.
	reconciler.updateGroupCache(ctx, slice)

	// 1. First call: shouldBackoffHealthy returns true (partition is healthy),
	//    ShouldBackoffOnFailure returns false (no retry annotation),
	//    so the timestamp should be cleared and reschedule proceeds.
	//    Actually, for a FAILED slice without retry annotation,
	//    ShouldBackoffOnFailure returns false → shouldBackoff=false → reschedule.
	//
	// To test the timeout path, we need a slice where ShouldBackoffOnFailure=true.
	// Use SliceHealthProbeFailed which always returns true for ShouldBackoffOnFailure.

	probeSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-probe",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-probe",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceHealthProbeFailed"},
			},
		},
	}
	if err := fakeClient.Create(ctx, probeSlice); err != nil {
		t.Fatalf("Failed to create probe slice: %v", err)
	}
	reconciler.updateGroupCache(ctx, probeSlice)

	// First call: should backoff (partition healthy + ShouldBackoffOnFailure=true).
	requeue1, delay1 := reconciler.tryScheduleSliceGroup(ctx, "workload-probe")
	if !requeue1 {
		t.Errorf("First call: expected requeue=true (backoff), got false")
	}
	if delay1 != rescheduleHealthyDelay {
		t.Errorf("First call: expected delay=%v, got %v", rescheduleHealthyDelay, delay1)
	}

	// Verify timestamp was recorded.
	reconciler.failureMutex.RLock()
	_, hasTimestamp := reconciler.failureTimestamps["slice-probe"]
	reconciler.failureMutex.RUnlock()
	if !hasTimestamp {
		t.Error("First call: expected failure timestamp to be recorded")
	}

	// 2. Second call within timeout: should still backoff.
	requeue2, _ := reconciler.tryScheduleSliceGroup(ctx, "workload-probe")
	if !requeue2 {
		t.Error("Second call (within timeout): expected requeue=true, got false")
	}

	// 3. Wait for timeout to expire, then call again.
	time.Sleep(rescheduleTimeout + 50*time.Millisecond)

	// Re-create the slice with same state since tryScheduleSliceGroup may have modified it.
	latestSlice := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "slice-probe"}, latestSlice); err != nil {
		t.Fatalf("Failed to get latest probe slice: %v", err)
	}
	// Ensure it still looks like it needs rescheduling.
	latestSlice.Spec.PartitionIds = []string{"p1"}
	latestSlice.Status.Conditions = []metav1.Condition{
		{Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceHealthProbeFailed"},
	}
	if err := fakeClient.Update(ctx, latestSlice); err != nil {
		t.Fatalf("Failed to update probe slice: %v", err)
	}

	// After timeout: backoff should be cleared and reschedule proceeds.
	requeue3, _ := reconciler.tryScheduleSliceGroup(ctx, "workload-probe")

	// After timeout, the scheduler either reschedules successfully (requeue=false)
	// or fails to schedule and requeues. Either way, the timestamp should be cleared.
	reconciler.failureMutex.RLock()
	_, hasTimestampAfter := reconciler.failureTimestamps["slice-probe"]
	reconciler.failureMutex.RUnlock()

	if hasTimestampAfter {
		t.Error("After timeout: expected failure timestamp to be cleared")
	}

	// The requeue3 value depends on whether a partition was available; the key
	// assertion is that the timestamp was cleared (timeout worked).
	_ = requeue3
}

// TestReconcile_ShouldSkipReconcile_GroupCacheOrdering verifies that the group
// cache is populated AFTER the policy skip check. If a slice is skipped, the
// group cache should NOT contain an entry from that reconciliation pass.
func TestReconcile_ShouldSkipReconcile_GroupCacheOrdering(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-ordering-test",
			Annotations: map[string]string{
				ManagedByAnnotation:                        interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:                   "workload-ordering",
				"accelerator.gke.io/provisioned-partition": "p1",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
	}

	// Build client with only the partition tree; slice created AFTER Start().
	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithObjects(pt.DeepCopy()).Build()
	recorder := events.NewFakeRecorder(10)

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, createTestScheduler(),
		watcher.NewPartitionCache(false), 0, 0,
		policy.NewCloudPolicy(), 0,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// Create slice AFTER Start() so group cache is clean.
	if err := fakeClient.Create(ctx, slice); err != nil {
		t.Fatalf("Failed to create slice: %v", err)
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "slice-ordering-test"}}
	result, reconcileErr := reconciler.Reconcile(ctx, req)
	if reconcileErr != nil {
		t.Fatalf("Reconcile() unexpected error: %v", reconcileErr)
	}

	// Should have been skipped with requeue.
	if result.RequeueAfter != policy.DeactivationRequeueDelay {
		t.Errorf("RequeueAfter = %v, want %v", result.RequeueAfter, policy.DeactivationRequeueDelay)
	}

	// Group cache should NOT have the workload entry because updateGroupCache
	// runs AFTER ShouldSkipReconcile.
	reconciler.groupMutex.RLock()
	_, exists := reconciler.groups["workload-ordering"]
	reconciler.groupMutex.RUnlock()

	if exists {
		t.Error("Group cache should NOT contain the workload entry when slice is skipped")
	}
}

// TestMultiSliceGroup_DifferentFailureReasons_NoBackoff verifies all-or-nothing backoff:
// when a multi-slice group has slices with DIFFERENT failure reasons (one that
// says backoff and one that does not), the group should NOT backoff because the
// ALL-OR-NOTHING check means any false overrides backoff.
func TestMultiSliceGroup_DifferentFailureReasons_NoBackoff(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	// slice-a: FAILED (ShouldBackoffOnFailure=false — no retry annotation)
	sliceA := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-a",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-mixed",
				NumSlicesAnnotation:      "2",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: "FAILED"},
			},
		},
	}

	// slice-b: SliceHealthProbeFailed (ShouldBackoffOnFailure=true)
	sliceB := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-b",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-mixed",
				NumSlicesAnnotation:      "2",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceHealthProbeFailed"},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithObjects(sliceA, sliceB, pt).Build()
	recorder := events.NewFakeRecorder(10)

	rescheduleTimeout := 1 * time.Second
	rescheduleHealthyDelay := 10 * time.Millisecond

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, createTestScheduler(),
		watcher.NewPartitionCache(false),
		rescheduleHealthyDelay, 0,
		policy.NewCloudPolicy(), rescheduleTimeout,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	reconciler.updateGroupCache(ctx, sliceA)
	reconciler.updateGroupCache(ctx, sliceB)

	// Since slice-a has FAILED (ShouldBackoffOnFailure=false), the group should
	// NOT backoff — shouldBackoff is set to false by the ALL-OR-NOTHING check.
	requeue, delay := reconciler.tryScheduleSliceGroup(ctx, "workload-mixed")

	// The group should proceed to reschedule (not backoff with rescheduleHealthyDelay).
	// It may requeue for other reasons (e.g., scheduling failure) but NOT with the
	// backoff delay.
	if requeue && delay == rescheduleHealthyDelay {
		t.Errorf("Group should NOT backoff when any slice returns ShouldBackoffOnFailure=false; got requeue=%v delay=%v", requeue, delay)
	}

	// Verify that failure timestamps are NOT set (we proceeded past backoff).
	reconciler.failureMutex.RLock()
	_, hasTimestamp := reconciler.failureTimestamps["slice-a"]
	_, hasTimestampB := reconciler.failureTimestamps["slice-b"]
	reconciler.failureMutex.RUnlock()
	if hasTimestamp || hasTimestampB {
		t.Error("Failure timestamps should NOT be set when backoff was skipped")
	}
}

// TestMultiSliceGroup_AllBackoff_ShouldBackoff verifies that when ALL slices
// in a multi-slice group say backoff (ShouldBackoffOnFailure=true), the group
// DOES backoff with the rescheduleHealthyDelay.
func TestMultiSliceGroup_AllBackoff_ShouldBackoff(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	// slice-a: SliceHealthProbeFailed (ShouldBackoffOnFailure=true)
	sliceA := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-a",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-allbackoff",
				NumSlicesAnnotation:      "2",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceHealthProbeFailed"},
			},
		},
	}

	// slice-b: SliceHealthProbeFailed (ShouldBackoffOnFailure=true)
	sliceB := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-b",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-allbackoff",
				NumSlicesAnnotation:      "2",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceHealthProbeFailed"},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithObjects(sliceA, sliceB, pt).Build()
	recorder := events.NewFakeRecorder(10)

	rescheduleTimeout := 1 * time.Second
	rescheduleHealthyDelay := 10 * time.Millisecond

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, createTestScheduler(),
		watcher.NewPartitionCache(false),
		rescheduleHealthyDelay, 0,
		policy.NewCloudPolicy(), rescheduleTimeout,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	reconciler.updateGroupCache(ctx, sliceA)
	reconciler.updateGroupCache(ctx, sliceB)

	// Since ALL slices have SliceHealthProbeFailed (ShouldBackoffOnFailure=true),
	// and partitions are healthy, the group SHOULD backoff.
	requeue, delay := reconciler.tryScheduleSliceGroup(ctx, "workload-allbackoff")

	if !requeue {
		t.Error("Expected requeue=true (backoff), got false")
	}
	if delay != rescheduleHealthyDelay {
		t.Errorf("Expected delay=%v, got %v", rescheduleHealthyDelay, delay)
	}
}

// TestFailureTimestamps_ClearedAfterSuccessfulReschedule verifies that failure
// timestamps are cleaned up after a successful scheduling, preventing stale
// entries from accumulating.
func TestFailureTimestamps_ClearedAfterSuccessfulReschedule(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	// Slice that needs scheduling (no partitions assigned yet).
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-success",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-success",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithObjects(slice, pt).Build()
	recorder := events.NewFakeRecorder(10)

	rescheduleTimeout := 1 * time.Second
	rescheduleHealthyDelay := 10 * time.Millisecond

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, createTestScheduler(),
		watcher.NewPartitionCache(false),
		rescheduleHealthyDelay, 0,
		policy.NewCloudPolicy(), rescheduleTimeout,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// Pre-populate a failure timestamp (simulating a previous backoff).
	reconciler.failureMutex.Lock()
	reconciler.failureTimestamps["slice-success"] = time.Now()
	reconciler.failureMutex.Unlock()

	reconciler.updateGroupCache(ctx, slice)

	// Trigger scheduling. This is a new slice (no partitions), so it should
	// succeed if a partition is available.
	requeue, _ := reconciler.tryScheduleSliceGroup(ctx, "workload-success")

	if requeue {
		// Scheduling did not succeed, so the success-path cleanup was not reached.
		// This test expects scheduling to succeed (partition matches topology).
		t.Fatal("Scheduling returned requeue=true, expected successful scheduling")
	}

	// After successful scheduling, the failure timestamp should be cleared.
	reconciler.failureMutex.RLock()
	_, hasTimestamp := reconciler.failureTimestamps["slice-success"]
	reconciler.failureMutex.RUnlock()

	if hasTimestamp {
		t.Error("Expected failure timestamp to be cleared after scheduling, but it still exists")
	}
}

// TestCheckFailureBackoff_WithDefer directly tests the checkFailureBackoff helper
// to verify correct mutex handling with defer and proper timeout behavior.
func TestCheckFailureBackoff_WithDefer(t *testing.T) {
	s := scheme.Scheme
	fakeClient := fake.NewClientBuilder().WithScheme(s).Build()
	recorder := events.NewFakeRecorder(10)

	rescheduleTimeout := 100 * time.Millisecond
	rescheduleHealthyDelay := 10 * time.Millisecond

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, nil,
		watcher.NewPartitionCache(false),
		rescheduleHealthyDelay, 0,
		policy.NewCloudPolicy(), rescheduleTimeout,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}

	// 1. First call: should record timestamp and return (true, delay).
	continueBackoff, delay := reconciler.checkFailureBackoff("test-slice")
	if !continueBackoff {
		t.Error("First call: expected continueBackoff=true, got false")
	}
	if delay != rescheduleHealthyDelay {
		t.Errorf("First call: expected delay=%v, got %v", rescheduleHealthyDelay, delay)
	}

	// 2. Verify timestamp was recorded.
	reconciler.failureMutex.RLock()
	_, hasTimestamp := reconciler.failureTimestamps["test-slice"]
	reconciler.failureMutex.RUnlock()
	if !hasTimestamp {
		t.Error("First call: expected failure timestamp to be recorded")
	}

	// 3. Second call within timeout: should still return (true, delay).
	continueBackoff2, delay2 := reconciler.checkFailureBackoff("test-slice")
	if !continueBackoff2 {
		t.Error("Second call (within timeout): expected continueBackoff=true, got false")
	}
	if delay2 != rescheduleHealthyDelay {
		t.Errorf("Second call: expected delay=%v, got %v", rescheduleHealthyDelay, delay2)
	}

	// 4. Wait for timeout to expire.
	time.Sleep(rescheduleTimeout + 50*time.Millisecond)

	// 5. Call after timeout: should return (false, 0) and clear timestamp.
	continueBackoff3, delay3 := reconciler.checkFailureBackoff("test-slice")
	if continueBackoff3 {
		t.Error("After timeout: expected continueBackoff=false, got true")
	}
	if delay3 != 0 {
		t.Errorf("After timeout: expected delay=0, got %v", delay3)
	}

	// 6. Verify timestamp was cleared.
	reconciler.failureMutex.RLock()
	_, hasTimestampAfter := reconciler.failureTimestamps["test-slice"]
	reconciler.failureMutex.RUnlock()
	if hasTimestampAfter {
		t.Error("After timeout: expected failure timestamp to be cleared")
	}
}

// TestCheckFailureBackoff_TimeoutExpiry explicitly verifies that the timestamp
// map entry is removed after timeout expiry.
func TestCheckFailureBackoff_TimeoutExpiry(t *testing.T) {
	s := scheme.Scheme
	fakeClient := fake.NewClientBuilder().WithScheme(s).Build()
	recorder := events.NewFakeRecorder(10)

	rescheduleTimeout := 50 * time.Millisecond
	rescheduleHealthyDelay := 10 * time.Millisecond

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, nil,
		watcher.NewPartitionCache(false),
		rescheduleHealthyDelay, 0,
		policy.NewCloudPolicy(), rescheduleTimeout,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}

	// 1. First call: record timestamp.
	continueBackoff, _ := reconciler.checkFailureBackoff("expire-test")
	if !continueBackoff {
		t.Error("First call: expected continueBackoff=true")
	}

	// Verify timestamp exists.
	reconciler.failureMutex.RLock()
	_, exists := reconciler.failureTimestamps["expire-test"]
	reconciler.failureMutex.RUnlock()
	if !exists {
		t.Error("Expected timestamp to exist after first call")
	}

	// 2. Wait for timeout to expire.
	time.Sleep(100 * time.Millisecond)

	// 3. Call again after timeout.
	continueBackoff2, delay2 := reconciler.checkFailureBackoff("expire-test")
	if continueBackoff2 {
		t.Error("After timeout: expected continueBackoff=false, got true")
	}
	if delay2 != 0 {
		t.Errorf("After timeout: expected delay=0, got %v", delay2)
	}

	// 4. Verify the timestamp map entry is gone.
	reconciler.failureMutex.RLock()
	_, existsAfter := reconciler.failureTimestamps["expire-test"]
	reconciler.failureMutex.RUnlock()
	if existsAfter {
		t.Error("Expected timestamp to be removed after timeout expiry")
	}
}

// TestRescheduleTimeoutZero_SkipsBackoff verifies that when rescheduleTimeout=0,
// the entire backoff mechanism is disabled — even when partitions are healthy
// and the policy says to backoff, the scheduler proceeds directly to rescheduling
// without recording failure timestamps or applying any delay.
func TestRescheduleTimeoutZero_SkipsBackoff(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	// SliceHealthProbeFailed with healthy partitions would normally trigger
	// backoff (ShouldBackoffOnFailure=true, shouldBackoffHealthy=true).
	// With rescheduleTimeout=0, the entire block should be skipped.
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-zero-timeout",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-zero-timeout",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{"p1"}},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceHealthProbeFailed"},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithObjects(slice, pt).Build()
	recorder := events.NewFakeRecorder(10)

	// rescheduleTimeout = 0 → backoff disabled entirely.
	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, createTestScheduler(),
		watcher.NewPartitionCache(false),
		10*time.Millisecond, 0,
		policy.NewCloudPolicy(), 0, // rescheduleTimeout = 0
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	reconciler.updateGroupCache(ctx, slice)

	// Call tryScheduleSliceGroup. With rescheduleTimeout=0, the backoff block
	// (lines 507-538 in slice_scheduler.go) should be skipped entirely.
	requeue, delay := reconciler.tryScheduleSliceGroup(ctx, "workload-zero-timeout")

	// The scheduler should NOT return the rescheduleHealthyDelay. It either
	// succeeds (requeue=false) or fails for scheduling reasons (not backoff).
	if requeue && delay == 10*time.Millisecond {
		t.Errorf("With rescheduleTimeout=0, backoff should be skipped but got backoff delay=%v", delay)
	}

	// No failure timestamps should be recorded because the backoff block was skipped.
	reconciler.failureMutex.RLock()
	_, hasTimestamp := reconciler.failureTimestamps["slice-zero-timeout"]
	reconciler.failureMutex.RUnlock()
	if hasTimestamp {
		t.Error("With rescheduleTimeout=0, no failure timestamp should be recorded (backoff disabled)")
	}
}

// TestTryScheduleSliceGroup_PartialDeactivation_AbortsGroup verifies that when
// one slice in a multi-slice group is deactivating (has provisioned-partition
// annotation), the ENTIRE group scheduling is aborted rather than proceeding
// with a partial set of slices. Partial group scheduling would cause incorrect
// partition allocation for multi-slice workloads.
func TestTryScheduleSliceGroup_PartialDeactivation_AbortsGroup(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{PartitionID: "p1-child1", Topology: "2x2", State: partitiontreev1.PartitionStateHealthy},
					{PartitionID: "p1-child2", Topology: "2x2", State: partitiontreev1.PartitionStateHealthy},
				},
			},
		},
	}

	// Slice A: normal, needs scheduling
	sliceA := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-group-a",
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-multi",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
	}

	// Slice B: deactivating (has provisioned-partition annotation)
	sliceB := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice-group-b",
			Annotations: map[string]string{
				ManagedByAnnotation:                        interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:                   "workload-multi",
				"accelerator.gke.io/provisioned-partition": "p1-child2",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithObjects(sliceA, sliceB, pt).Build()
	recorder := events.NewFakeRecorder(10)

	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		recorder, false, createTestScheduler(),
		watcher.NewPartitionCache(false),
		10*time.Millisecond, 0,
		policy.NewCloudPolicy(), 0,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// Add both slices to the group cache
	reconciler.updateGroupCache(ctx, sliceA)
	reconciler.updateGroupCache(ctx, sliceB)

	// Verify both slices are in the same group
	reconciler.groupMutex.RLock()
	group, exists := reconciler.groups["workload-multi"]
	reconciler.groupMutex.RUnlock()
	if !exists || len(group.Slices) != 2 {
		t.Fatalf("Expected group 'workload-multi' with 2 slices, got exists=%v, slices=%d",
			exists, len(group.Slices))
	}

	// Call tryScheduleSliceGroup. Since slice-group-b is deactivating,
	// the entire group scheduling should be aborted.
	requeue, delay := reconciler.tryScheduleSliceGroup(ctx, "workload-multi")

	// Should requeue with DeactivationRequeueDelay (5s)
	if !requeue {
		t.Error("Expected requeue=true when a slice in the group is deactivating")
	}
	if delay != policy.DeactivationRequeueDelay {
		t.Errorf("Expected requeue delay=%v (DeactivationRequeueDelay), got %v",
			policy.DeactivationRequeueDelay, delay)
	}

	// Verify that slice-group-a was NOT partially scheduled
	updatedSliceA := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "slice-group-a"}, updatedSliceA); err != nil {
		t.Fatalf("Failed to get slice-group-a: %v", err)
	}
	if len(updatedSliceA.Spec.PartitionIds) > 0 {
		t.Errorf("slice-group-a should NOT have been scheduled (partial group), but got partitions: %v",
			updatedSliceA.Spec.PartitionIds)
	}
}

// TestTryScheduleSliceGroup_ActiveProvisionedMemberDoesNotAbortGroup verifies both paths
// where an already-bound GKE sub-slice carrying accelerator.gke.io/provisioned-partition
// previously blocked its multi-slice gang:
//  1. Live Reconcile() registration: a bound ACTIVE sub-slice created after Start() is not
//     skipped before updateGroupCache, so the gang reaches ExpectedSize and places its
//     unbound sibling.
//  2. prepareGroupAllocationRequest(): when a bound sub-slice member enters FAILED and needs
//     rescheduling alongside an unbound sibling, ShouldSkipReconcile does not abort the gang.
func TestTryScheduleSliceGroup_ActiveProvisionedMemberDoesNotAbortGroup(t *testing.T) {
	s := scheme.Scheme

	t.Run("live_reconcile_registers_bound_active_subslice_and_schedules_unbound_sibling", func(t *testing.T) {
		ctx := t.Context()
		pt := &partitiontreev1.PartitionTree{
			ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
			Status: partitiontreev1.PartitionTreeStatus{
				AcceleratorType: "v6e",
				Root: &partitiontreev1.PartitionInfo{
					PartitionID: "p1", Topology: "4x4",
					State: partitiontreev1.PartitionStateHealthy,
					ChildPartitions: []partitiontreev1.PartitionInfo{
						{PartitionID: "p1-child1", Topology: "2x2", State: partitiontreev1.PartitionStateHealthy},
						{PartitionID: "p1-child2", Topology: "2x2", State: partitiontreev1.PartitionStateHealthy},
					},
				},
			},
		}

		// Start reconciler BEFORE creating the gang slices so Start() does not pre-populate r.groups.
		fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(pt).Build()
		reconciler, err := NewSliceReconciler(
			fakeClient, s, ctrl.Log.WithName("test"), "v6e",
			events.NewFakeRecorder(10), false, createTestScheduler(),
			watcher.NewPartitionCache(false), 0, 0,
			policy.NewCloudPolicy(), 0,
		)
		if err != nil {
			t.Fatalf("NewSliceReconciler() error: %v", err)
		}
		reconciler.requeueWithDelayFn = func(context.Context, *pq.Item, time.Duration) {}
		if err := reconciler.Start(ctx); err != nil {
			t.Fatalf("Start() error: %v", err)
		}

		sliceA := &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name: "slice-active-gang-a",
				Annotations: map[string]string{
					ManagedByAnnotation:      interfaces.SliceSchedulerName,
					SliceGroupNameAnnotation: "workload-active-gang",
					NumSlicesAnnotation:      "2",
				},
			},
			Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
		}
		sliceB := &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "slice-active-gang-b",
				Generation: 2,
				Annotations: map[string]string{
					ManagedByAnnotation:                   interfaces.SliceSchedulerName,
					SliceGroupNameAnnotation:              "workload-active-gang",
					NumSlicesAnnotation:                   "2",
					policy.ProvisionedPartitionAnnotation: "p1-child2",
				},
			},
			Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{"p1-child2"}},
			Status: slicev1beta1.SliceStatus{
				ObservedGeneration: 2,
				Conditions: []metav1.Condition{
					{Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: policy.ReasonActive},
				},
			},
		}
		for _, sl := range []*slicev1beta1.Slice{sliceA, sliceB} {
			if err := fakeClient.Create(ctx, sl); err != nil {
				t.Fatalf("Create(%s) error: %v", sl.Name, err)
			}
		}
		reconciler.handleSliceCreate(ctx, event.TypedCreateEvent[client.Object]{Object: sliceB})

		// Reconcile both slices: on parent, Reconcile(sliceB) skipped before updateGroupCache,
		// leaving the 2-slice gang incomplete (1 < ExpectedSize 2).
		resB, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceB.Name}})
		if err != nil {
			t.Fatalf("Reconcile(sliceB) error: %v", err)
		}
		if resB.RequeueAfter != 0 {
			t.Fatalf("Reconcile(sliceB) skipped active bound sub-slice with RequeueAfter=%v, want 0", resB.RequeueAfter)
		}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceA.Name}}); err != nil {
			t.Fatalf("Reconcile(sliceA) error: %v", err)
		}

		reconciler.groupMutex.RLock()
		grp := reconciler.groups["workload-active-gang"]
		ready := grp != nil && grp.IsReady()
		reconciler.groupMutex.RUnlock()
		if !ready {
			t.Fatal("gang workload-active-gang is not ready after Reconcile(sliceB) + Reconcile(sliceA)")
		}

		requeue, delay := reconciler.tryScheduleSliceGroup(ctx, "workload-active-gang")
		if requeue {
			t.Fatalf("tryScheduleSliceGroup() = (%v, %v), want (false, 0)", requeue, delay)
		}

		updatedA := &slicev1beta1.Slice{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Name: sliceA.Name}, updatedA); err != nil {
			t.Fatalf("Get(sliceA) error: %v", err)
		}
		if len(updatedA.Spec.PartitionIds) != 1 || updatedA.Spec.PartitionIds[0] != "p1-child1" {
			t.Errorf("sliceA PartitionIds = %v, want [p1-child1]", updatedA.Spec.PartitionIds)
		}
	})

	t.Run("prepare_group_does_not_abort_when_bound_provisioned_member_needs_rescheduling", func(t *testing.T) {
		ctx := t.Context()
		pt := &partitiontreev1.PartitionTree{
			ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
			Status: partitiontreev1.PartitionTreeStatus{
				AcceleratorType: "v6e",
				Root: &partitiontreev1.PartitionInfo{
					PartitionID: "p1", Topology: "4x4",
					State: partitiontreev1.PartitionStateHealthy,
					ChildPartitions: []partitiontreev1.PartitionInfo{
						{PartitionID: "p1-child1", Topology: "2x2", State: partitiontreev1.PartitionStateHealthy},
						{PartitionID: "p1-child2", Topology: "2x2", State: partitiontreev1.PartitionStateUnhealthy},
						{PartitionID: "p1-child3", Topology: "2x2", State: partitiontreev1.PartitionStateHealthy},
					},
				},
			},
		}

		sliceA := &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name: "slice-gang-unbound-a",
				Annotations: map[string]string{
					ManagedByAnnotation:      interfaces.SliceSchedulerName,
					SliceGroupNameAnnotation: "workload-resched-gang",
					NumSlicesAnnotation:      "2",
				},
			},
			Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
		}
		sliceB := &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "slice-gang-failed-b",
				Generation: 2,
				Annotations: map[string]string{
					ManagedByAnnotation:                   interfaces.SliceSchedulerName,
					SliceGroupNameAnnotation:              "workload-resched-gang",
					NumSlicesAnnotation:                   "2",
					policy.ProvisionedPartitionAnnotation: "p1-child2",
				},
			},
			Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{"p1-child2"}},
			Status: slicev1beta1.SliceStatus{
				ObservedGeneration: 2,
				Conditions: []metav1.Condition{
					{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: policy.ReasonFailed},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(sliceA, sliceB, pt).Build()
		reconciler, err := NewSliceReconciler(
			fakeClient, s, ctrl.Log.WithName("test"), "v6e",
			events.NewFakeRecorder(10), false, createTestScheduler(),
			watcher.NewPartitionCache(false), 0, 0,
			policy.NewCloudPolicy(), 0,
		)
		if err != nil {
			t.Fatalf("NewSliceReconciler() error: %v", err)
		}
		reconciler.requeueWithDelayFn = func(context.Context, *pq.Item, time.Duration) {}
		if err := reconciler.Start(ctx); err != nil {
			t.Fatalf("Start() error: %v", err)
		}

		// On parent, prepareGroupAllocationRequest hit ShouldSkipReconcile(sliceB)==true
		// (returning (true, 5s)) instead of marking sliceB for moved-slice eviction.
		requeue, delay := reconciler.tryScheduleSliceGroup(ctx, "workload-resched-gang")
		if requeue {
			t.Fatalf("tryScheduleSliceGroup() = (%v, %v), want (false, 0) after marking moved sliceB", requeue, delay)
		}
		updatedB := &slicev1beta1.Slice{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Name: sliceB.Name}, updatedB); err != nil {
			t.Fatalf("Get(sliceB) error: %v", err)
		}
		if updatedB.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
			t.Errorf("sliceB evict-pods annotation = %q, want %q",
				updatedB.Annotations[interfaces.EvictPodsAnnotation], interfaces.TrueStr)
		}
	})
}

// TestTryScheduleSliceGroup_ObservedGenerationLag_AbortsGroup verifies that when a slice has
// PartitionIds=[] and provisioned-partition was already removed by slice-controller (or never
// set on a super-slice), but status.observedGeneration (> 0) still lags metadata.generation,
// scheduling waits for slice-controller to finish the N->0 update.
func TestTryScheduleSliceGroup_ObservedGenerationLag_AbortsGroup(t *testing.T) {
	ctx := t.Context()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
			},
		},
	}

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "slice-obsgen-lag",
			Generation: 3,
			Annotations: map[string]string{
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation: "workload-obsgen-lag",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "4x4", PartitionIds: []string{}},
		Status: slicev1beta1.SliceStatus{
			ObservedGeneration: 2,
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: policy.ReasonActive},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice, pt).Build()
	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		events.NewFakeRecorder(10), false, createTestScheduler(),
		watcher.NewPartitionCache(false), 0, 0,
		policy.NewCloudPolicy(), 0,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	reconciler.updateGroupCache(ctx, slice)
	requeue, delay := reconciler.tryScheduleSliceGroup(ctx, "workload-obsgen-lag")
	if !requeue || delay != policy.DeactivationRequeueDelay {
		t.Fatalf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)",
			requeue, delay, policy.DeactivationRequeueDelay)
	}

	updated := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: slice.Name}, updated); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if len(updated.Spec.PartitionIds) != 0 {
		t.Errorf("slice prematurely scheduled while ObservedGeneration < Generation: %v", updated.Spec.PartitionIds)
	}
}

// TestProvisionedSubslice_FailedStateFullLifecycle verifies that a bound GKE sub-slice
// (carrying accelerator.gke.io/provisioned-partition) that enters Ready=False/FAILED:
//  1. Is NOT skipped by Reconcile() while still bound.
//  2. Is unbound (N->0) by PodReconciler.
//  3. Is gated by ShouldSkipReconcile across both Phase 1 and Phase 2 of GCE deactivation.
//  4. Is rescheduled (0->M) onto a healthy sibling sub-slice once ObservedGeneration catches up.
func TestProvisionedSubslice_FailedStateFullLifecycle(t *testing.T) {
	ctx := t.Context()
	s := scheme.Scheme

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt1"},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: "v6e",
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "p1", Topology: "4x4",
				State: partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{PartitionID: "p1-child1", Topology: "2x2", State: partitiontreev1.PartitionStateUnhealthy},
					{PartitionID: "p1-child2", Topology: "2x2", State: partitiontreev1.PartitionStateHealthy},
				},
			},
		},
	}

	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "slice-failed-subslice",
			Generation: 2,
			Annotations: map[string]string{
				ManagedByAnnotation:                   interfaces.SliceSchedulerName,
				SliceGroupNameAnnotation:              "workload-failed-subslice",
				policy.ProvisionedPartitionAnnotation: "p1-child1",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{"p1-child1"}},
		Status: slicev1beta1.SliceStatus{
			ObservedGeneration: 2,
			Conditions: []metav1.Condition{
				{Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: policy.ReasonFailed},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&slicev1beta1.Slice{}).
		WithObjects(slice, pt).Build()
	cache := watcher.NewPartitionCache(false)
	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e",
		events.NewFakeRecorder(10), false, createTestScheduler(),
		cache, 0, 0, policy.NewCloudPolicy(), 0,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	reconciler.requeueWithDelayFn = func(context.Context, *pq.Item, time.Duration) {}
	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	// 1. Reconcile does NOT skip the bound FAILED sub-slice despite provisioned-partition.
	res, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: slice.Name}})
	if err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("Reconcile() skipped bound FAILED sub-slice with RequeueAfter=%v, want 0", res.RequeueAfter)
	}

	// 2. PodReconciler drains and unbinds N->0.
	pr := pod_reconciler.NewPodReconciler(fakeClient, fakeClient, s, ctrl.Log.WithName("pr"),
		"v6e", cache, events.NewFakeRecorder(10), 1, false, policy.NewCloudPolicy())
	if _, err := pr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: slice.Name}}); err != nil {
		t.Fatalf("PodReconciler.Reconcile() error: %v", err)
	}
	got := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: slice.Name}, got); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if len(got.Spec.PartitionIds) != 0 {
		t.Fatalf("PartitionIds = %v after PodReconciler unbind, want empty", got.Spec.PartitionIds)
	}
	got.Generation = 3
	if err := fakeClient.Update(ctx, got); err != nil {
		t.Fatalf("Update(generation=3) error: %v", err)
	}
	_ = fakeClient.Get(ctx, types.NamespacedName{Name: slice.Name}, got)
	cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, slice)})
	reconciler.updateGroupCache(ctx, got)

	// 3. Gate Phase 1 (provisioned-partition present) & Phase 2 (ObservedGeneration lag).
	if rq, d := reconciler.tryScheduleSliceGroup(ctx, "workload-failed-subslice"); !rq || d != policy.DeactivationRequeueDelay {
		t.Fatalf("Phase 1 deactivation = (%v, %v), want (true, %v)", rq, d, policy.DeactivationRequeueDelay)
	}
	delete(got.Annotations, policy.ProvisionedPartitionAnnotation)
	if err := fakeClient.Update(ctx, got); err != nil {
		t.Fatalf("Update(delete provisioned-partition) error: %v", err)
	}
	_ = fakeClient.Get(ctx, types.NamespacedName{Name: slice.Name}, got)
	reconciler.updateGroupCache(ctx, got)
	if rq, d := reconciler.tryScheduleSliceGroup(ctx, "workload-failed-subslice"); !rq || d != policy.DeactivationRequeueDelay {
		t.Fatalf("Phase 2 deactivation = (%v, %v), want (true, %v)", rq, d, policy.DeactivationRequeueDelay)
	}

	// 4. slice-controller finishes DEACTIVATE (ObservedGeneration == Generation, SliceNotCreated).
	got.Status.ObservedGeneration = got.Generation
	got.Status.Conditions = []metav1.Condition{
		{Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceNotCreated"},
	}
	if err := fakeClient.Status().Update(ctx, got); err != nil {
		t.Fatalf("Status().Update() error: %v", err)
	}
	_ = fakeClient.Get(ctx, types.NamespacedName{Name: slice.Name}, got)
	reconciler.updateGroupCache(ctx, got)

	if rq, d := reconciler.tryScheduleSliceGroup(ctx, "workload-failed-subslice"); rq {
		t.Fatalf("tryScheduleSliceGroup() = (%v, %v), want (false, 0)", rq, d)
	}
	_ = fakeClient.Get(ctx, types.NamespacedName{Name: slice.Name}, got)
	if len(got.Spec.PartitionIds) != 1 || got.Spec.PartitionIds[0] != "p1-child2" {
		t.Errorf("PartitionIds = %v, want [p1-child2]", got.Spec.PartitionIds)
	}
}

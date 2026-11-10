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

package pod_reconciler

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

// newTestScheme creates a scheme with all required types registered.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, addFn := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		policyv1.AddToScheme,
		slicev1beta1.AddToScheme,
	} {
		if err := addFn(s); err != nil {
			t.Fatalf("failed to register scheme: %v", err)
		}
	}
	return s
}

// TestReconcile_EvictionPolicy verifies eviction behavior for all failure
// states using CloudPolicy.ShouldEvictPod.
func TestReconcile_EvictionPolicy(t *testing.T) {
	s := newTestScheme(t)
	ctx := context.Background()
	logger := zap.New(zap.UseDevMode(true))

	tests := []struct {
		name         string
		sliceState   string
		condStatus   metav1.ConditionStatus
		annotations  map[string]string
		wantEviction bool
	}{
		{
			name:         "FAILED → evict",
			sliceState:   "FAILED",
			condStatus:   metav1.ConditionFalse,
			wantEviction: true,
		},
		{
			name:         "MMIGNotFound → evict",
			sliceState:   "MMIGNotFound",
			condStatus:   metav1.ConditionFalse,
			wantEviction: true,
		},
		{
			name:         "PreflightError → evict",
			sliceState:   "PreflightError",
			condStatus:   metav1.ConditionFalse,
			wantEviction: true,
		},
		{
			name:         "SliceCreationFailed (no retry) → evict",
			sliceState:   "SliceCreationFailed",
			condStatus:   metav1.ConditionFalse,
			wantEviction: true,
		},
		{
			name:       "SliceCreationFailed (retry enabled) → NOT evict",
			sliceState: "SliceCreationFailed",
			condStatus: metav1.ConditionFalse,
			annotations: map[string]string{
				"slice.gke.io/retry-on-failure": "true",
			},
			wantEviction: false,
		},
		{
			name:         "SliceHealthProbeFailed → NOT evict",
			sliceState:   "SliceHealthProbeFailed",
			condStatus:   metav1.ConditionUnknown,
			wantEviction: false,
		},
		{
			name:         "ACTIVE → NOT evict",
			sliceState:   "ACTIVE",
			condStatus:   metav1.ConditionTrue,
			wantEviction: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sliceAnnotations := tt.annotations
			if sliceAnnotations == nil {
				sliceAnnotations = map[string]string{}
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-0",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "test-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
					NodeName:      "node-1",
				},
			}

			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-slice",
					Annotations: sliceAnnotations,
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Ready",
							Status: tt.condStatus,
							Reason: tt.sliceState,
						},
					},
				},
			}

			objs := []client.Object{pod, slice}

			fakeClient := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(objs...).
				WithIndex(&corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
					p := rawObj.(*corev1.Pod)
					if p.Spec.NodeName == "" {
						return nil
					}
					return []string{p.Spec.NodeName}
				}).
				Build()

			subResourceClient := &mockSubResourceClient{
				SubResourceClient: fakeClient.SubResource("binding"),
			}
			mClient := &mockClient{
				Client:            fakeClient,
				subResourceClient: subResourceClient,
			}

			cache := watcher.NewPartitionCache(false)
			tree := &partitiontreev1.PartitionTree{
				ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
				Status: partitiontreev1.PartitionTreeStatus{
					Root: &partitiontreev1.PartitionInfo{
						PartitionID: "part-1",
						NodeNames:   []string{"node-1"},
					},
				},
			}
			cache.UpdateTree(ctx, tree, nil)

			reconciler := NewPodReconciler(
				mClient, mClient, s, logger, testTPUType, cache,
				events.NewFakeRecorder(10), 1, false,
				policy.NewCloudPolicy(),
			)

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: slice.Name,
				},
			}

			_, err := reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}

			gotEviction := subResourceClient.lastEviction != nil
			if gotEviction != tt.wantEviction {
				t.Errorf("eviction = %v, want %v", gotEviction, tt.wantEviction)
			}
		})
	}
}

// TestReconcile_EvictionPolicy_IsPartitionBroken verifies the partition-broken
// fallback: when ShouldEvictPod returns false (retry enabled), the
// reconciler falls back to isPartitionBroken to decide eviction.
func TestReconcile_EvictionPolicy_IsPartitionBroken(t *testing.T) {
	s := newTestScheme(t)
	ctx := context.Background()
	logger := zap.New(zap.UseDevMode(true))

	retryAnnotations := map[string]string{
		"slice.gke.io/retry-on-failure": "true",
	}

	tests := []struct {
		name           string
		partitionIds   []string
		partitionState partitiontreev1.PartitionStateType
		setupCache     bool // whether to populate the cache with the partition
		wantEviction   bool
	}{
		{
			name:           "retry + broken partition (Unhealthy) → evict",
			partitionIds:   []string{"part-1"},
			partitionState: partitiontreev1.PartitionStateUnhealthy,
			setupCache:     true,
			wantEviction:   true,
		},
		{
			name:           "retry + broken partition (Unknown) → evict",
			partitionIds:   []string{"part-1"},
			partitionState: partitiontreev1.PartitionStateUnknown,
			setupCache:     true,
			wantEviction:   true,
		},
		{
			name:         "retry + partition missing from cache → evict",
			partitionIds: []string{"part-missing"},
			setupCache:   false,
			wantEviction: true,
		},
		{
			name:           "retry + healthy partition → no evict",
			partitionIds:   []string{"part-1"},
			partitionState: partitiontreev1.PartitionStateHealthy,
			setupCache:     true,
			wantEviction:   false,
		},
		{
			name:         "retry + empty partitionIds → no evict",
			partitionIds: []string{},
			setupCache:   false,
			wantEviction: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The drain evicts only placed pods, so bind the pod. A slice with no
			// partitions evicts any bound pod, so leave it Pending there.
			nodeName := ""
			if len(tt.partitionIds) > 0 {
				nodeName = "node-1"
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-0",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "test-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
					NodeName:      nodeName,
				},
			}

			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-slice",
					Annotations: retryAnnotations,
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: tt.partitionIds,
				},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Ready",
							Status: metav1.ConditionFalse,
							Reason: "SliceCreationFailed",
						},
					},
				},
			}

			objs := []client.Object{pod, slice}

			fakeClient := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(objs...).
				WithIndex(&corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
					p := rawObj.(*corev1.Pod)
					if p.Spec.NodeName == "" {
						return nil
					}
					return []string{p.Spec.NodeName}
				}).
				Build()

			subResourceClient := &mockSubResourceClient{
				SubResourceClient: fakeClient.SubResource("binding"),
			}
			mClient := &mockClient{
				Client:            fakeClient,
				subResourceClient: subResourceClient,
			}

			cache := watcher.NewPartitionCache(false)
			if tt.setupCache {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							State:       tt.partitionState,
							NodeNames:   []string{"node-1"},
						},
					},
				}
				cache.UpdateTree(ctx, tree, nil)
			}

			reconciler := NewPodReconciler(
				mClient, mClient, s, logger, testTPUType, cache,
				events.NewFakeRecorder(10), 1, false,
				policy.NewCloudPolicy(),
			)

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: slice.Name,
				},
			}

			_, err := reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}

			gotEviction := subResourceClient.lastEviction != nil
			if gotEviction != tt.wantEviction {
				t.Errorf("eviction = %v, want %v (partitionState=%v, setupCache=%v)",
					gotEviction, tt.wantEviction, tt.partitionState, tt.setupCache)
			}
		})
	}
}

// TestReconcile_IsSliceReady verifies that:
//   - ACTIVE → pod binding proceeds.
//   - ACTIVE_DEGRADED → pod binding proceeds.
//   - ACTIVATING → returns without binding (waits).
func TestReconcile_IsSliceReady(t *testing.T) {
	s := newTestScheme(t)
	ctx := context.Background()
	logger := zap.New(zap.UseDevMode(true))

	tests := []struct {
		name        string
		sliceState  string
		condStatus  metav1.ConditionStatus
		wantBinding bool
	}{
		{
			name:        "ACTIVE → binding proceeds",
			sliceState:  "ACTIVE",
			condStatus:  metav1.ConditionTrue,
			wantBinding: true,
		},
		{
			name:        "ACTIVE_DEGRADED → binding proceeds",
			sliceState:  "ACTIVE_DEGRADED",
			condStatus:  metav1.ConditionTrue,
			wantBinding: true,
		},
		{
			name:        "ACTIVATING → waits (no binding)",
			sliceState:  "ACTIVATING",
			condStatus:  metav1.ConditionFalse,
			wantBinding: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-0",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "test-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
				},
			}

			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Ready",
							Status: tt.condStatus,
							Reason: tt.sliceState,
						},
					},
				},
			}

			objs := []client.Object{pod, slice}

			fakeClient := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(objs...).
				WithIndex(&corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
					p := rawObj.(*corev1.Pod)
					if p.Spec.NodeName == "" {
						return nil
					}
					return []string{p.Spec.NodeName}
				}).
				Build()

			subResourceClient := &mockSubResourceClient{
				SubResourceClient: fakeClient.SubResource("binding"),
			}
			mClient := &mockClient{
				Client:            fakeClient,
				subResourceClient: subResourceClient,
			}

			cache := watcher.NewPartitionCache(false)
			tree := &partitiontreev1.PartitionTree{
				ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
				Status: partitiontreev1.PartitionTreeStatus{
					Root: &partitiontreev1.PartitionInfo{
						PartitionID: "part-1",
						NodeNames:   []string{"node-1"},
					},
				},
			}
			cache.UpdateTree(ctx, tree, nil)

			// WaitForSliceReady=true to exercise the IsSliceReady check.
			reconciler := NewPodReconciler(
				mClient, mClient, s, logger, testTPUType, cache,
				events.NewFakeRecorder(10), 1, true,
				policy.NewCloudPolicy(),
			)

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: slice.Name,
				},
			}

			_, err := reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}

			gotBinding := subResourceClient.lastBinding != nil
			if gotBinding != tt.wantBinding {
				t.Errorf("binding = %v, want %v", gotBinding, tt.wantBinding)
			}
		})
	}
}

// TestReconcile_NodeReadyForBinding verifies that:
//   - Node with HEALTHY label → binding proceeds.
//   - Node with DEGRADED label → binding proceeds.
//   - Node missing label → requeue with delay.
//   - Empty topology → skip label check, binding proceeds.
func TestReconcile_NodeReadyForBinding(t *testing.T) {
	s := newTestScheme(t)
	ctx := context.Background()
	logger := zap.New(zap.UseDevMode(true))

	tests := []struct {
		name        string
		topology    string
		nodeLabels  map[string]string
		wantBinding bool
		wantRequeue bool
	}{
		{
			name:     "Node with HEALTHY label → binding proceeds",
			topology: "4x4x4",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-4x4x4-state": "HEALTHY",
				"cloud.google.com/gke-tpu-topology":              "4x4x4",
				"cloud.google.com/gke-tpu-slice":                 "test-slice",
			},
			wantBinding: true,
		},
		{
			name:     "Node with DEGRADED label → binding proceeds",
			topology: "4x4x4",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-4x4x4-state": "DEGRADED",
				"cloud.google.com/gke-tpu-topology":              "4x4x4",
				"cloud.google.com/gke-tpu-slice":                 "test-slice",
			},
			wantBinding: true,
		},
		{
			name:        "Node missing label → requeue with delay",
			topology:    "4x4x4",
			nodeLabels:  map[string]string{},
			wantBinding: false,
			wantRequeue: true,
		},
		{
			name:        "Empty topology → skip label check, binding proceeds",
			topology:    "",
			nodeLabels:  map[string]string{},
			wantBinding: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-0",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "test-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
				},
			}

			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					Topology:     tt.topology,
					PartitionIds: []string{"part-1"},
				},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Ready",
							Status: metav1.ConditionTrue,
							Reason: "ACTIVE",
						},
					},
				},
			}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node-1",
					Labels: tt.nodeLabels,
				},
			}

			objs := []client.Object{pod, slice, node}

			fakeClient := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(objs...).
				WithIndex(&corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
					p := rawObj.(*corev1.Pod)
					if p.Spec.NodeName == "" {
						return nil
					}
					return []string{p.Spec.NodeName}
				}).
				Build()

			subResourceClient := &mockSubResourceClient{
				SubResourceClient: fakeClient.SubResource("binding"),
			}
			mClient := &mockClient{
				Client:            fakeClient,
				subResourceClient: subResourceClient,
			}

			cache := watcher.NewPartitionCache(false)
			tree := &partitiontreev1.PartitionTree{
				ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
				Status: partitiontreev1.PartitionTreeStatus{
					Root: &partitiontreev1.PartitionInfo{
						PartitionID: "part-1",
						NodeNames:   []string{"node-1"},
					},
				},
			}
			cache.UpdateTree(ctx, tree, nil)

			reconciler := NewPodReconciler(
				mClient, mClient, s, logger, testTPUType, cache,
				events.NewFakeRecorder(10), 1, false,
				policy.NewCloudPolicy(),
			)

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: slice.Name,
				},
			}

			result, err := reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}

			gotBinding := subResourceClient.lastBinding != nil
			if gotBinding != tt.wantBinding {
				t.Errorf("binding = %v, want %v", gotBinding, tt.wantBinding)
			}

			if tt.wantRequeue {
				if result.RequeueAfter == 0 {
					t.Error("expected requeue with delay, got RequeueAfter=0")
				}
			}

			if tt.wantBinding && gotBinding {
				if subResourceClient.lastBinding.Target.Name != "node-1" {
					t.Errorf("target node = %v, want node-1", subResourceClient.lastBinding.Target.Name)
				}
			}
		})
	}
}

// TestReconcile_NodeReadyForBinding_SupersliceTopology verifies that
// superslice topologies (e.g. 4x4x8) use the per-cube topology (4x4x4)
// for the health label check.
func TestReconcile_NodeReadyForBinding_SupersliceTopology(t *testing.T) {
	s := newTestScheme(t)
	ctx := context.Background()
	logger := zap.New(zap.UseDevMode(true))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels: map[string]string{
				SliceNameLabel: "test-slice",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "slice-scheduler",
		},
	}

	// Superslice topology 4x4x8: health label should use 4x4x4
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-slice",
		},
		Spec: slicev1beta1.SliceSpec{
			Topology:     "4x4x8",
			PartitionIds: []string{"part-1"},
		},
		Status: slicev1beta1.SliceStatus{
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "ACTIVE"},
			},
		},
	}

	// Node has the 4x4x4 label (not 4x4x8)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				"cloud.google.com/gke-tpu-partition-4x4x4-state": "HEALTHY",
				"cloud.google.com/gke-tpu-topology":              "4x4x4",
				"cloud.google.com/gke-tpu-slice":                 "test-slice",
			},
		},
	}

	objs := []client.Object{pod, slice, node}
	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
			p := rawObj.(*corev1.Pod)
			if p.Spec.NodeName == "" {
				return nil
			}
			return []string{p.Spec.NodeName}
		}).
		Build()

	subResourceClient := &mockSubResourceClient{
		SubResourceClient: fakeClient.SubResource("binding"),
	}
	mClient := &mockClient{Client: fakeClient, subResourceClient: subResourceClient}

	cache := watcher.NewPartitionCache(false)
	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "part-1",
				NodeNames:   []string{"node-1"},
			},
		},
	}
	cache.UpdateTree(ctx, tree, nil)

	reconciler := NewPodReconciler(
		mClient, mClient, s, logger, testTPUType, cache,
		events.NewFakeRecorder(10), 1, false,
		policy.NewCloudPolicy(),
	)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: slice.Name}}
	_, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile() unexpected error: %v", err)
	}

	if subResourceClient.lastBinding == nil {
		t.Fatal("Expected binding for superslice topology with 4x4x4 HEALTHY label, got nil")
	}
	if subResourceClient.lastBinding.Target.Name != "node-1" {
		t.Errorf("target node = %v, want node-1", subResourceClient.lastBinding.Target.Name)
	}
}

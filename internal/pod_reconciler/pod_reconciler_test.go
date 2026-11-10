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
	"fmt"
	"testing"
	"time"

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
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

// testTPUType is the TPU type the reconciler under test is started for.
const testTPUType = string(slicev1beta1.TypeV6e)

// mockSubResourceClient implements client.SubResourceClient to handle "binding" and "eviction" subresources in tests.
type mockSubResourceClient struct {
	client.SubResourceClient
	lastBinding  *corev1.Binding
	lastEviction *policyv1.Eviction
	err          error
	// failFor fails writes for the named pods only.
	failFor map[string]error
	// bound lists every pod a Binding was created for, in order.
	bound []string
}

func (m *mockSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	if m.err != nil {
		return m.err
	}
	if err, ok := m.failFor[obj.GetName()]; ok {
		return err
	}
	if b, ok := subResource.(*corev1.Binding); ok {
		m.lastBinding = b
		m.bound = append(m.bound, obj.GetName())
		return nil
	}
	if e, ok := subResource.(*policyv1.Eviction); ok {
		m.lastEviction = e
		return nil
	}
	return m.SubResourceClient.Create(ctx, obj, subResource, opts...)
}

// mockClient wraps fake client to return mockSubResourceClient for "binding" and "eviction".
type mockClient struct {
	client.Client
	subResourceClient *mockSubResourceClient
}

func (m *mockClient) SubResource(subResource string) client.SubResourceClient {
	if subResource == "binding" || subResource == "eviction" {
		return m.subResourceClient
	}
	return m.Client.SubResource(subResource)
}

type reconcileTestCase struct {
	name              string
	pod               *corev1.Pod
	slice             *slicev1beta1.Slice
	existingPod       *corev1.Pod
	nodes             []*corev1.Node
	cacheSetup        func(*watcher.PartitionCache)
	bindingErr        error
	wantBinding       bool
	wantEviction      bool
	wantRequeue       bool
	targetNode        string
	expectError       bool
	waitForSliceReady bool
	verify            func(*testing.T, client.Client)
}

func runReconcileTest(t *testing.T, tt reconcileTestCase) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	if err := policyv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add policyv1 to scheme: %v", err)
	}
	if err := slicev1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add slicev1beta1 to scheme: %v", err)
	}

	ctx := context.Background()
	logger := zap.New(zap.UseDevMode(true))

	var objs []client.Object
	if tt.pod != nil {
		objs = append(objs, tt.pod)
	}
	if tt.slice != nil {
		objs = append(objs, tt.slice)
	}
	if tt.existingPod != nil {
		objs = append(objs, tt.existingPod)
	}
	for _, node := range tt.nodes {
		objs = append(objs, node)
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
			pod := rawObj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		Build()

	subResourceClient := &mockSubResourceClient{SubResourceClient: fakeClient.SubResource("binding"), err: tt.bindingErr}
	mClient := &mockClient{Client: fakeClient, subResourceClient: subResourceClient}

	cache := watcher.NewPartitionCache(false)
	if tt.cacheSetup != nil {
		tt.cacheSetup(cache)
	}

	reconciler := NewPodReconciler(mClient, fakeClient, scheme, logger, testTPUType, cache, events.NewFakeRecorder(10), 1, tt.waitForSliceReady, policy.NewCloudPolicy())

	// Reconcile request uses SLICE name (not Pod name)
	reqName := "non-existent-slice"
	if tt.slice != nil {
		reqName = tt.slice.Name
	}
	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name: reqName,
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	if (err != nil) != tt.expectError {
		t.Errorf("Reconcile() error = %v, expectError %v", err, tt.expectError)
	}

	if tt.wantRequeue {
		if result.RequeueAfter == 0 {
			t.Errorf("Reconcile() should have requeued, but got no requeue")
		}
		if tt.verify == nil {
			return
		}
	}

	if tt.wantEviction {
		if subResourceClient.lastEviction == nil {
			t.Fatal("Eviction should have been created but was nil")
		}
		if tt.pod != nil && subResourceClient.lastEviction.Name != tt.pod.Name {
			t.Errorf("eviction name = %v, want %v", subResourceClient.lastEviction.Name, tt.pod.Name)
		}
		if tt.verify == nil {
			return
		}
	}

	if tt.wantBinding {
		if subResourceClient.lastBinding == nil {
			t.Fatal("Binding should have been created but was nil")
		}
		if subResourceClient.lastBinding.Target.Name != tt.targetNode {
			t.Errorf("target node = %v, want %v", subResourceClient.lastBinding.Target.Name, tt.targetNode)
		}
		if tt.pod != nil && subResourceClient.lastBinding.Name != tt.pod.Name {
			t.Errorf("binding name = %v, want %v", subResourceClient.lastBinding.Name, tt.pod.Name)
		}
	} else if tt.pod != nil && tt.pod.Spec.NodeName == "" && !tt.expectError {
		if subResourceClient.lastBinding != nil {
			t.Errorf("Binding should not have been created, but got: %v", subResourceClient.lastBinding)
		}
	}

	if tt.verify != nil {
		tt.verify(t, fakeClient)
	}
}

func TestReconcile_Scheduling(t *testing.T) {
	ctx := context.Background()
	tests := []reconcileTestCase{
		{
			name: "Success - Bind pod-0 to first node",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"node-1", "node-2"},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			wantBinding: true,
			targetNode:  "node-1",
		},
		{
			name: "Success - Multiple partitions",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-2",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "test-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
				},
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1", "part-2"},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree1 := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"node-1", "node-2"},
						},
					},
				}
				tree2 := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-2"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-2",
							NodeNames:   []string{"node-3", "node-4"},
						},
					},
				}
				pc.UpdateTree(ctx, tree1, nil)
				pc.UpdateTree(ctx, tree2, nil)
			},
			wantBinding: true,
			targetNode:  "node-3",
		},
		{
			name: "Partition Not Found",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"missing-part"},
				},
			},
			expectError: true,
		},
		{
			name: "Invalid Pod Name (No Ordinal)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalid-name",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "test-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
				},
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "part-1", NodeNames: []string{"n1"}},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			expectError: true,
		},
		{
			name: "No Hostnames for Partition",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			expectError: true,
		},
		{
			name: "Pod already scheduled on correct node",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-0",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "test-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
					NodeName:      "correct-node",
				},
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"correct-node"},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			wantBinding: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runReconcileTest(t, tt)
		})
	}
}

func TestReconcile_Eviction(t *testing.T) {
	ctx := context.Background()
	tests := []reconcileTestCase{
		{
			name: "Evict scheduled pod for UNHEALTHY slice",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Ready",
							Status: metav1.ConditionFalse,
							Reason: "FAILED",
						},
					},
				},
			},
			wantEviction: true,
		},
		{
			// A Pending pod holds no hardware; evicting it can't advance the drain.
			name: "Pending pod is not evicted for UNHEALTHY slice",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Status: slicev1beta1.SliceStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Ready",
							Status: metav1.ConditionFalse,
							Reason: "FAILED",
						},
					},
				},
			},
			wantEviction: false,
		},
		{
			name: "Evict scheduled pod on wrong node",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-0",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "test-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
					NodeName:      "wrong-node",
				},
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"correct-node"},
						},
					},
				}
				pc.UpdateTree(context.Background(), tree, nil)
			},
			wantEviction: true,
		},
		{
			name: "Evict scheduled pod because slice has no partitions",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-scheduled",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "empty-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
					NodeName:      "node-1",
				},
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "empty-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{},
				},
			},
			wantEviction: true,
		},
		{
			name: "No-op for pending pod when slice has no partitions",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-pending",
					Namespace: "default",
					Labels: map[string]string{
						SliceNameLabel: "empty-slice",
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "slice-scheduler",
				},
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "empty-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{},
				},
			},
			wantEviction: false,
			wantBinding:  false,
		},
		{
			name: "Eviction annotation triggers pod eviction",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
					Annotations: map[string]string{
						interfaces.EvictPodsAnnotation: "true",
					},
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			wantEviction: true,
			wantRequeue:  true,
		},
		{
			name: "Eviction completion clears PartitionIds and annotation",
			pod:  nil,
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
					Annotations: map[string]string{
						interfaces.EvictPodsAnnotation: "true",
						watcher.ManagedByAnnotation:    interfaces.SliceSchedulerName,
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         slicev1beta1.TypeV6e,
					PartitionIds: []string{"part-1"},
				},
			},
			wantBinding:  false,
			wantEviction: false,
			verify: func(t *testing.T, fakeClient client.Client) {
				updatedSlice := &slicev1beta1.Slice{}
				err := fakeClient.Get(ctx, types.NamespacedName{Name: "test-slice"}, updatedSlice)
				if err != nil {
					t.Fatalf("failed to get updated slice: %v", err)
				}
				if len(updatedSlice.Spec.PartitionIds) != 0 {
					t.Errorf("expected PartitionIds to be empty, got %v", updatedSlice.Spec.PartitionIds)
				}
				if updatedSlice.Annotations != nil {
					if _, ok := updatedSlice.Annotations[interfaces.EvictPodsAnnotation]; ok {
						t.Errorf("expected annotation %s to be removed", interfaces.EvictPodsAnnotation)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runReconcileTest(t, tt)
		})
	}
}

// TestReconcile_EvictionUnbindsOnlyOwnedSlices checks that a drained failed slice is
// unbound only by the scheduler instance that manages it.
func TestReconcile_EvictionUnbindsOnlyOwnedSlices(t *testing.T) {
	ctx := context.Background()
	failedSlice := func(annotations map[string]string, sliceType slicev1beta1.Type) *slicev1beta1.Slice {
		return &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{Name: "failed-slice", Annotations: annotations},
			Spec: slicev1beta1.SliceSpec{
				Type:         sliceType,
				PartitionIds: []string{"part-1"},
			},
			Status: slicev1beta1.SliceStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionFalse, Reason: "FAILED"},
				},
			},
		}
	}
	wantPartitionIds := func(want []string) func(*testing.T, client.Client) {
		return func(t *testing.T, c client.Client) {
			got := &slicev1beta1.Slice{}
			if err := c.Get(ctx, types.NamespacedName{Name: "failed-slice"}, got); err != nil {
				t.Fatalf("failed to get slice: %v", err)
			}
			if len(got.Spec.PartitionIds) != len(want) || (len(want) > 0 && got.Spec.PartitionIds[0] != want[0]) {
				t.Errorf("PartitionIds = %v, want %v", got.Spec.PartitionIds, want)
			}
		}
	}
	managed := map[string]string{watcher.ManagedByAnnotation: interfaces.SliceSchedulerName}

	tests := []reconcileTestCase{
		{
			name:   "owned failed slice is unbound",
			slice:  failedSlice(managed, slicev1beta1.TypeV6e),
			verify: wantPartitionIds(nil),
		},
		{
			name:   "slice without managed-by is left bound",
			slice:  failedSlice(nil, slicev1beta1.TypeV6e),
			verify: wantPartitionIds([]string{"part-1"}),
		},
		{
			name:   "slice managed by another scheduler is left bound",
			slice:  failedSlice(map[string]string{watcher.ManagedByAnnotation: "other"}, slicev1beta1.TypeV6e),
			verify: wantPartitionIds([]string{"part-1"}),
		},
		{
			name:   "slice of another TPU type is left bound",
			slice:  failedSlice(managed, slicev1beta1.TypeTpu7x),
			verify: wantPartitionIds([]string{"part-1"}),
		},
		{
			name: "evict annotation on a slice of another TPU type does not unbind it",
			slice: failedSlice(map[string]string{
				watcher.ManagedByAnnotation:    interfaces.SliceSchedulerName,
				interfaces.EvictPodsAnnotation: interfaces.TrueStr,
			}, slicev1beta1.TypeTpu7x),
			verify: wantPartitionIds([]string{"part-1"}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runReconcileTest(t, tt)
		})
	}
}

func TestReconcile_Concurrency(t *testing.T) {
	ctx := context.Background()
	tests := []reconcileTestCase{
		{
			name: "Wait for termination - node occupied",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			existingPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "old-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName:      "node-1",
					SchedulerName: "slice-scheduler",
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"node-1"},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			wantRequeue: true,
		},
		{
			name: "Wait for termination - node occupied (deleting pod)",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			existingPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "old-pod",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().Add(-10 * time.Second)},
					Finalizers:        []string{"keep-around"},
				},
				Spec: corev1.PodSpec{
					NodeName:      "node-1",
					SchedulerName: "slice-scheduler",
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"node-1"},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			wantRequeue: true,
		},
		{
			name: "Bind - node only has a Succeeded pod",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			existingPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "old-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName:      "node-1",
					SchedulerName: "slice-scheduler",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"node-1"},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			wantBinding: true,
			targetNode:  "node-1",
		},
		{
			name: "Bind - node only has a Failed pod",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			existingPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "old-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName:      "node-1",
					SchedulerName: "slice-scheduler",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"node-1"},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			wantBinding: true,
			targetNode:  "node-1",
		},
		{
			name: "Wait for termination - node occupied (Running pod)",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			existingPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "old-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName:      "node-1",
					SchedulerName: "slice-scheduler",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"node-1"},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			wantRequeue: true,
		},
		{
			name: "Binding Error",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					PartitionIds: []string{"part-1"},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{
							PartitionID: "part-1",
							NodeNames:   []string{"node-1"},
						},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			bindingErr:  fmt.Errorf("api error"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runReconcileTest(t, tt)
		})
	}
}

func TestReconcile_WaitForReady(t *testing.T) {
	ctx := context.Background()
	tests := []reconcileTestCase{
		{
			name: "Don't requeue pending pod when WaitForSliceReady=true and Slice is NOT Ready",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
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
							Status: metav1.ConditionFalse,
							Reason: "PENDING",
						},
					},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "part-1", NodeNames: []string{"n1"}},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			waitForSliceReady: true,
			wantRequeue:       false,
		},
		{
			name: "Bind pending pod when WaitForSliceReady=true and Slice is Ready",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
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
							Status: metav1.ConditionTrue,
							Reason: "ACTIVE",
						},
					},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "part-1", NodeNames: []string{"n1"}},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			waitForSliceReady: true,
			wantBinding:       true,
			targetNode:        "n1",
		},
		{
			name: "Bind pending pod when WaitForSliceReady=false and Slice is NOT Ready",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
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
							Status: metav1.ConditionFalse,
							Reason: "PENDING",
						},
					},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "part-1", NodeNames: []string{"n1"}},
					},
				}
				pc.UpdateTree(ctx, tree, nil)
			},
			waitForSliceReady: false,
			wantBinding:       true,
			targetNode:        "n1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runReconcileTest(t, tt)
		})
	}
}

func TestReconcile_NodeReadiness(t *testing.T) {
	tests := []reconcileTestCase{
		{
			name: "BUG2 - Requeue when target node missing partition-state label",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					Topology:     "4x4x4",
					PartitionIds: []string{"part-1"},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node-1",
						Labels: map[string]string{}, // No partition-state label
					},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "part-1", NodeNames: []string{"node-1"}},
					},
				}
				pc.UpdateTree(context.Background(), tree, nil)
			},
			wantRequeue: true,
		},
		{
			name: "BUG2 - Bind when target node has partition-state HEALTHY",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					Topology:     "4x4x4",
					PartitionIds: []string{"part-1"},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
						Labels: map[string]string{
							"cloud.google.com/gke-tpu-partition-4x4x4-state": "HEALTHY",
							"cloud.google.com/gke-tpu-topology":              "4x4x4",
							"cloud.google.com/gke-tpu-slice":                 "test-slice",
						},
					},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "part-1", NodeNames: []string{"node-1"}},
					},
				}
				pc.UpdateTree(context.Background(), tree, nil)
			},
			wantBinding: true,
			targetNode:  "node-1",
		},
		{
			name: "BUG2 - Bind when target node has partition-state DEGRADED",
			pod: &corev1.Pod{
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
			},
			slice: &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice",
				},
				Spec: slicev1beta1.SliceSpec{
					Topology:     "4x4x4",
					PartitionIds: []string{"part-1"},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
						Labels: map[string]string{
							"cloud.google.com/gke-tpu-partition-4x4x4-state": "DEGRADED",
							"cloud.google.com/gke-tpu-topology":              "4x4x4",
							"cloud.google.com/gke-tpu-slice":                 "test-slice",
						},
					},
				},
			},
			cacheSetup: func(pc *watcher.PartitionCache) {
				tree := &partitiontreev1.PartitionTree{
					Status: partitiontreev1.PartitionTreeStatus{
						Root: &partitiontreev1.PartitionInfo{PartitionID: "part-1", NodeNames: []string{"node-1"}},
					},
				}
				pc.UpdateTree(context.Background(), tree, nil)
			},
			wantBinding: true,
			targetNode:  "node-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runReconcileTest(t, tt)
		})
	}
}

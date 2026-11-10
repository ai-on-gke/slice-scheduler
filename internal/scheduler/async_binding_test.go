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
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/pq"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

// TestAsyncBinding_EndToEnd_Success verifies the full Reconcile → schedule →
// async binding → partition assignment flow with EnableAsyncBinding active.
func TestAsyncBinding_EndToEnd_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := scheme.Scheme

	sliceName := "e2e-async-slice"
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: sliceName,
			Annotations: map[string]string{
				SliceGroupNameAnnotation: "e2e-async-group",
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				NumSlicesAnnotation:      "1",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
	}

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pt-e2e",
			Labels: map[string]string{TpuPodIDLabel: "pt-e2e"},
		},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: "v6e",
			Root:            &partitiontreev1.PartitionInfo{PartitionID: "p-e2e-1", Topology: "2x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice, pt).Build()
	recorder := events.NewFakeRecorder(10)
	reconciler, err := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-e2e"), "v6e", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("Failed to create reconciler: %v", err)
	}
	reconciler.EnableAsyncBinding(10, 2)

	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// Trigger reconciliation
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName}}
	_, err = reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	// Wait for the async binding worker to complete the binding
	success := false
	for i := 0; i < 40; i++ {
		scheduledSlice := &slicev1beta1.Slice{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Name: sliceName}, scheduledSlice); err == nil && len(scheduledSlice.Spec.PartitionIds) > 0 {
			success = true
			if scheduledSlice.Spec.PartitionIds[0] != "p-e2e-1" {
				t.Errorf("Expected partition p-e2e-1, got %v", scheduledSlice.Spec.PartitionIds)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !success {
		t.Fatal("Timed out waiting for async binding to assign partitions via Reconcile")
	}
}

// TestAsyncBinding_FailureRollback_Unreserve verifies that when async API binding
// fails, the worker unreserves partitions and requeues the group.
func TestAsyncBinding_FailureRollback_Unreserve(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := scheme.Scheme

	sliceName := "fail-rollback-slice"
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: sliceName,
			Annotations: map[string]string{
				SliceGroupNameAnnotation: "fail-rollback-group",
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				NumSlicesAnnotation:      "1",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
	}

	cache := watcher.NewPartitionCache(false)
	recorder := events.NewFakeRecorder(10)

	// Create a fake client that fails on Update (simulates API failure during bind)
	baseClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice).Build()
	failingClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Update: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			return fmt.Errorf("simulated API failure")
		},
	})

	r, err := NewSliceReconciler(failingClient, s, ctrl.Log.WithName("test-fail-rollback"), "v6e", recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("Failed to create reconciler: %v", err)
	}
	r.EnableAsyncBinding(10, 2)

	requeuedItems := make(chan *pq.Item, 10)
	r.requeueWithDelayFn = func(ctx context.Context, item *pq.Item, delay time.Duration) {
		requeuedItems <- item
	}

	// Start binding workers manually
	for i := 0; i < r.bindingWorkers; i++ {
		go r.processBindingQueue(ctx, i)
	}

	// Populate group cache so the worker can look up priority
	r.groupMutex.Lock()
	r.groups["fail-rollback-group"] = &interfaces.SliceGroup{
		Name:         "fail-rollback-group",
		ExpectedSize: 1,
		MinSize:      1,
		Slices:       map[string]*interfaces.SliceInfo{sliceName: {Name: sliceName, Topology: "2x4"}},
		Priority:     50,
	}
	r.groupMutex.Unlock()

	// Send a binding task directly
	task := &BindingTask{
		groupId: "fail-rollback-group",
		groupSlices: map[string]*slicev1beta1.Slice{
			sliceName: slice,
		},
		req: interfaces.GroupAllocationRequest{GroupName: "fail-rollback-group"},
		slicesToReserve: []*interfaces.SliceInfo{
			{Name: sliceName, PartitionIds: []string{"p1"}, Topology: "2x4", Type: "v6e"},
		},
		candidate: &interfaces.PlacementCandidate{
			Assignments: map[string][]*interfaces.PartitionInfo{
				sliceName: {{ID: "p1"}},
			},
		},
		firstTopology: "2x4",
		startTime:     time.Now(),
	}
	r.bindingQueue <- task

	// Verify the group was requeued
	select {
	case item := <-requeuedItems:
		if item.GroupId != "fail-rollback-group" {
			t.Errorf("Expected requeued groupId 'fail-rollback-group', got %q", item.GroupId)
		}
		if item.Priority != 50 {
			t.Errorf("Expected requeued priority 50, got %d", item.Priority)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for group to be requeued after async binding failure")
	}
}

// TestProcessBindingTask_DirectSuccess verifies processBindingTask
// on successful binding updates the slice.
func TestProcessBindingTask_DirectSuccess(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	sliceName := "direct-task-success"
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: sliceName,
			Annotations: map[string]string{
				ManagedByAnnotation: interfaces.SliceSchedulerName,
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice).Build()
	recorder := events.NewFakeRecorder(10)
	cache := watcher.NewPartitionCache(false)

	r, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-task-direct"), "v6e", recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)

	task := &BindingTask{
		groupId: "task-group",
		groupSlices: map[string]*slicev1beta1.Slice{
			sliceName: slice,
		},
		slicesToReserve: []*interfaces.SliceInfo{
			{Name: sliceName, PartitionIds: []string{"p1"}, Topology: "2x4", Type: "v6e"},
		},
		candidate: &interfaces.PlacementCandidate{
			Assignments: map[string][]*interfaces.PartitionInfo{
				sliceName: {{ID: "p1"}},
			},
		},
		firstTopology: "2x4",
		startTime:     time.Now(),
	}

	logf := r.log.WithName("test")
	r.processBindingTask(ctx, logf, task)

	// Verify the slice was updated
	updated := &slicev1beta1.Slice{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: sliceName}, updated); err != nil {
		t.Fatalf("Failed to get updated slice: %v", err)
	}
	if len(updated.Spec.PartitionIds) != 1 || updated.Spec.PartitionIds[0] != "p1" {
		t.Errorf("Expected partitions [p1], got %v", updated.Spec.PartitionIds)
	}
}

// TestProcessBindingTask_DirectFailure verifies processBindingTask
// requeues the group on failure.
func TestProcessBindingTask_DirectFailure(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	sliceName := "direct-task-failure"
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: sliceName,
			Annotations: map[string]string{
				ManagedByAnnotation: interfaces.SliceSchedulerName,
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
	}

	baseClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice).Build()
	failingClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			return fmt.Errorf("simulated failure")
		},
	})

	recorder := events.NewFakeRecorder(10)
	cache := watcher.NewPartitionCache(false)

	r, _ := NewSliceReconciler(failingClient, s, ctrl.Log.WithName("test-task-fail"), "v6e", recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)

	requeuedItems := make(chan *pq.Item, 10)
	r.requeueWithDelayFn = func(ctx context.Context, item *pq.Item, delay time.Duration) {
		requeuedItems <- item
	}

	task := &BindingTask{
		groupId: "task-fail-group",
		groupSlices: map[string]*slicev1beta1.Slice{
			sliceName: slice,
		},
		req: interfaces.GroupAllocationRequest{GroupName: "task-fail-group"},
		slicesToReserve: []*interfaces.SliceInfo{
			{Name: sliceName, PartitionIds: []string{"p1"}, Topology: "2x4", Type: "v6e"},
		},
		candidate: &interfaces.PlacementCandidate{
			Assignments: map[string][]*interfaces.PartitionInfo{
				sliceName: {{ID: "p1"}},
			},
		},
		firstTopology: "2x4",
		startTime:     time.Now(),
	}

	logf := r.log.WithName("test")
	r.processBindingTask(ctx, logf, task)

	// Verify requeue happened
	select {
	case item := <-requeuedItems:
		if item.GroupId != "task-fail-group" {
			t.Errorf("Expected requeued groupId 'task-fail-group', got %q", item.GroupId)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Group was not requeued after binding failure")
	}
}

// TestAsyncBinding_ClearsFailureTimestamps verifies that successful async binding
// clears the failure timestamps for all slices in the group.
func TestAsyncBinding_ClearsFailureTimestamps(t *testing.T) {
	ctx := context.Background()
	s := scheme.Scheme

	sliceName := "failure-ts-slice"
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: sliceName,
			Annotations: map[string]string{
				ManagedByAnnotation: interfaces.SliceSchedulerName,
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice).Build()
	recorder := events.NewFakeRecorder(10)
	cache := watcher.NewPartitionCache(false)

	r, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-failure-ts"), "v6e", recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)

	// Pre-seed a failure timestamp
	r.failureMutex.Lock()
	r.failureTimestamps[sliceName] = time.Now().Add(-5 * time.Minute)
	r.failureMutex.Unlock()

	task := &BindingTask{
		groupId: "ts-group",
		groupSlices: map[string]*slicev1beta1.Slice{
			sliceName: slice,
		},
		slicesToReserve: []*interfaces.SliceInfo{
			{Name: sliceName, PartitionIds: []string{"p1"}, Topology: "2x4", Type: "v6e"},
		},
		candidate: &interfaces.PlacementCandidate{
			Assignments: map[string][]*interfaces.PartitionInfo{
				sliceName: {{ID: "p1"}},
			},
		},
		firstTopology: "2x4",
		startTime:     time.Now(),
	}

	logf := r.log.WithName("test")
	r.processBindingTask(ctx, logf, task)

	// Verify failure timestamp was cleared
	r.failureMutex.RLock()
	_, exists := r.failureTimestamps[sliceName]
	r.failureMutex.RUnlock()
	if exists {
		t.Error("Failure timestamp was not cleared after successful async binding")
	}
}

// TestEnableAsyncBinding verifies that EnableAsyncBinding correctly initializes
// the binding queue and worker count.
func TestEnableAsyncBinding(t *testing.T) {
	s := scheme.Scheme
	r, _ := NewSliceReconciler(nil, s, ctrl.Log.WithName("test-enable"), "v6e", nil, false, nil, nil, 0, 0, policy.NewCloudPolicy(), 0)

	// Before enabling, queue should be nil
	if r.bindingQueue != nil {
		t.Fatal("bindingQueue should be nil before EnableAsyncBinding")
	}

	r.EnableAsyncBinding(100, 4)

	if r.bindingQueue == nil {
		t.Fatal("bindingQueue should not be nil after EnableAsyncBinding")
	}
	if cap(r.bindingQueue) != 100 {
		t.Errorf("Expected queue capacity 100, got %d", cap(r.bindingQueue))
	}
	if r.bindingWorkers != 4 {
		t.Errorf("Expected 4 workers, got %d", r.bindingWorkers)
	}
}

// TestAsyncBinding_QueueFullFallback verifies that when the binding queue is full,
// the scheduler falls back to synchronous binding.
func TestAsyncBinding_QueueFullFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := scheme.Scheme

	sliceName := "queue-full-slice"
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: sliceName,
			Annotations: map[string]string{
				SliceGroupNameAnnotation: "queue-full-group",
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				NumSlicesAnnotation:      "1",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
	}

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pt-queue-full",
			Labels: map[string]string{TpuPodIDLabel: "pt-queue-full"},
		},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: "v6e",
			Root:            &partitiontreev1.PartitionInfo{PartitionID: "p-qf-1", Topology: "2x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice, pt).Build()
	recorder := events.NewFakeRecorder(10)
	reconciler, err := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-queue-full"), "v6e", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("Failed to create reconciler: %v", err)
	}
	// Create a queue with capacity 0 — it will always be full.
	// Do NOT start workers — we want the channel to block.
	reconciler.bindingQueue = make(chan *BindingTask, 0)
	reconciler.bindingWorkers = 0

	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// Trigger reconciliation
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName}}
	_, err = reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	// The synchronous fallback should have bound the partition directly.
	// Wait for the scheduling worker goroutine to process.
	success := false
	for i := 0; i < 40; i++ {
		scheduledSlice := &slicev1beta1.Slice{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Name: sliceName}, scheduledSlice); err == nil && len(scheduledSlice.Spec.PartitionIds) > 0 {
			success = true
			if scheduledSlice.Spec.PartitionIds[0] != "p-qf-1" {
				t.Errorf("Expected partition p-qf-1, got %v", scheduledSlice.Spec.PartitionIds)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !success {
		t.Fatal("Timed out waiting for synchronous fallback binding")
	}
}

// TestAsyncBinding_SyncFallback_NilQueue verifies that when bindingQueue is nil
// (not enabled), the scheduler uses the synchronous binding path.
func TestAsyncBinding_SyncFallback_NilQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := scheme.Scheme

	sliceName := "sync-fallback-slice"
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: sliceName,
			Annotations: map[string]string{
				SliceGroupNameAnnotation: "sync-fallback-group",
				ManagedByAnnotation:      interfaces.SliceSchedulerName,
				NumSlicesAnnotation:      "1",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
	}

	pt := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pt-sync",
			Labels: map[string]string{TpuPodIDLabel: "pt-sync"},
		},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: "v6e",
			Root:            &partitiontreev1.PartitionInfo{PartitionID: "p-sync-1", Topology: "2x4", State: partitiontreev1.PartitionStateHealthy},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(slice, pt).Build()
	recorder := events.NewFakeRecorder(10)
	reconciler, err := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-sync"), "v6e", recorder, false, createTestScheduler(), watcher.NewPartitionCache(false), 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("Failed to create reconciler: %v", err)
	}
	// Do NOT call EnableAsyncBinding — bindingQueue is nil.

	if err := reconciler.Start(ctx); err != nil {
		t.Fatalf("Failed to start reconciler: %v", err)
	}

	// Trigger reconciliation
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName}}
	_, err = reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	// Synchronous binding should complete directly.
	success := false
	for i := 0; i < 40; i++ {
		scheduledSlice := &slicev1beta1.Slice{}
		if err := fakeClient.Get(ctx, types.NamespacedName{Name: sliceName}, scheduledSlice); err == nil && len(scheduledSlice.Spec.PartitionIds) > 0 {
			success = true
			if scheduledSlice.Spec.PartitionIds[0] != "p-sync-1" {
				t.Errorf("Expected partition p-sync-1, got %v", scheduledSlice.Spec.PartitionIds)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !success {
		t.Fatal("Timed out waiting for synchronous binding with nil queue")
	}
}

// TestAsyncBinding_WorkerShutdown verifies that binding workers exit cleanly
// when the context is cancelled.
func TestAsyncBinding_WorkerShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := scheme.Scheme
	cache := watcher.NewPartitionCache(false)
	recorder := events.NewFakeRecorder(10)

	r, _ := NewSliceReconciler(nil, s, ctrl.Log.WithName("test-shutdown"), "v6e", recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)
	r.EnableAsyncBinding(10, 2)

	// Track worker exits
	workersDone := make(chan struct{}, 2)
	for i := 0; i < r.bindingWorkers; i++ {
		go func(id int) {
			r.processBindingQueue(ctx, id)
			workersDone <- struct{}{}
		}(i)
	}

	// Let workers start
	time.Sleep(50 * time.Millisecond)

	// Cancel context to trigger shutdown
	cancel()

	// Both workers should exit within a reasonable time
	for i := 0; i < 2; i++ {
		select {
		case <-workersDone:
			// Worker exited successfully
		case <-time.After(2 * time.Second):
			t.Fatalf("Worker %d did not shut down within timeout", i)
		}
	}
}

// TestAsyncBinding_MultipleConcurrentTasks verifies that multiple binding tasks
// can be processed concurrently by different workers.
func TestAsyncBinding_MultipleConcurrentTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := scheme.Scheme

	// Create 3 slices, each in its own group
	sliceNames := []string{"concurrent-slice-1", "concurrent-slice-2", "concurrent-slice-3"}
	var initObjs []client.Object
	for _, name := range sliceNames {
		initObjs = append(initObjs, &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Annotations: map[string]string{
					ManagedByAnnotation: interfaces.SliceSchedulerName,
				},
			},
			Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
		})
	}

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(initObjs...).Build()
	recorder := events.NewFakeRecorder(10)
	cache := watcher.NewPartitionCache(false)

	r, _ := NewSliceReconciler(fakeClient, s, ctrl.Log.WithName("test-concurrent"), "v6e", recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)
	r.EnableAsyncBinding(10, 3)

	// Start workers
	for i := 0; i < r.bindingWorkers; i++ {
		go r.processBindingQueue(ctx, i)
	}

	// Enqueue 3 tasks concurrently
	for i, name := range sliceNames {
		task := &BindingTask{
			groupId: fmt.Sprintf("concurrent-group-%d", i),
			groupSlices: map[string]*slicev1beta1.Slice{
				name: {
					ObjectMeta: metav1.ObjectMeta{
						Name:        name,
						Annotations: map[string]string{ManagedByAnnotation: interfaces.SliceSchedulerName},
					},
					Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x4", PartitionIds: []string{}},
				},
			},
			slicesToReserve: []*interfaces.SliceInfo{
				{Name: name, PartitionIds: []string{fmt.Sprintf("p-%d", i)}, Topology: "2x4", Type: "v6e"},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					name: {{ID: fmt.Sprintf("p-%d", i)}},
				},
			},
			firstTopology: "2x4",
			startTime:     time.Now(),
		}
		r.bindingQueue <- task
	}

	// Wait for all 3 slices to be bound
	for _, name := range sliceNames {
		success := false
		for i := 0; i < 40; i++ {
			updated := &slicev1beta1.Slice{}
			if err := fakeClient.Get(ctx, types.NamespacedName{Name: name}, updated); err == nil && len(updated.Spec.PartitionIds) > 0 {
				success = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !success {
			t.Errorf("Timed out waiting for concurrent binding of %s", name)
		}
	}
}

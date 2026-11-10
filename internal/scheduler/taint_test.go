/*
Copyright 2026 Google LLC.

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
	"maps"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/generators"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins/taint"
	"github.com/ai-on-gke/slice-scheduler/internal/pod_reconciler"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/pq"
	"github.com/ai-on-gke/slice-scheduler/internal/schedulerlib"
	"github.com/ai-on-gke/slice-scheduler/internal/testutil"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestShouldBackoffHealthy_BlockedTaint checks that a blocked NoExecute taint, which leaves
// IsBroken false, counts as unhealthy for backoff, and that a NoSchedule taint does not.
func TestShouldBackoffHealthy_BlockedTaint(t *testing.T) {
	taint := func(key string, effect partitiontreev1.TaintEffect) []*partitiontreev1.PartitionTaint {
		return []*partitiontreev1.PartitionTaint{{Key: key, Effect: effect}}
	}

	tests := []struct {
		name           string
		isRescheduling bool
		partition      *interfaces.PartitionInfo
		wantBackoff    bool
	}{
		{
			name:           "not rescheduling never backs off",
			isRescheduling: false,
			partition:      &interfaces.PartitionInfo{ID: "p1"},
			wantBackoff:    false,
		},
		{
			name:           "healthy and untainted backs off",
			isRescheduling: true,
			partition:      &interfaces.PartitionInfo{ID: "p1"},
			wantBackoff:    true,
		},
		{
			name:           "broken does not back off",
			isRescheduling: true,
			partition:      &interfaces.PartitionInfo{ID: "p1", IsBroken: true},
			wantBackoff:    false,
		},
		{
			name:           "blocked NoExecute taint does not back off even though IsBroken is false",
			isRescheduling: true,
			partition: &interfaces.PartitionInfo{
				ID:     "p1",
				Taints: taint(interfaces.TaintKeyNodesNotFound, partitiontreev1.TaintEffectNoExecute),
			},
			wantBackoff: false,
		},
		{
			name:           "blocked NoSchedule taint still backs off",
			isRescheduling: true,
			partition: &interfaces.PartitionInfo{
				ID:     "p1",
				Taints: taint(corev1.TaintNodeUnschedulable, partitiontreev1.TaintEffectNoSchedule),
			},
			wantBackoff: true,
		},
		{
			name:           "PreferNoSchedule is not a blocked effect, so it still backs off",
			isRescheduling: true,
			partition: &interfaces.PartitionInfo{
				ID:     "p1",
				Taints: taint(interfaces.TaintKeyPartitionState, partitiontreev1.TaintEffectPreferNoSchedule),
			},
			wantBackoff: true,
		},
		{
			name:           "unrecognised taint key is not blocked, so it still backs off",
			isRescheduling: true,
			partition: &interfaces.PartitionInfo{
				ID:     "p1",
				Taints: taint("custom.taint.io/x", partitiontreev1.TaintEffectNoSchedule),
			},
			wantBackoff: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &SliceReconciler{watcher: &stateWatcher{state: &testutil.MockClusterState{
				PartitionTrees: []*interfaces.PartitionInfo{tt.partition},
			}}}

			got := r.shouldBackoffHealthy(logr.Discard(), tt.isRescheduling, []string{"p1"})
			if got != tt.wantBackoff {
				t.Errorf("shouldBackoffHealthy() = %v, want %v", got, tt.wantBackoff)
			}
		})
	}
}

// TestShouldBackoffHealthy_TaintedLeafUnderHeldCube uses the real cache. A slice holds the
// cube, but the controller records the node taint on a leaf beneath it; backoff only sees
// it because the cache unions descendants' taints onto the cube.
func TestShouldBackoffHealthy_TaintedLeafUnderHeldCube(t *testing.T) {
	cache := watcher.NewPartitionCache(false, watcher.WithTaintFiltering(true))
	cache.UpdateTree(context.Background(), &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-a"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "cube",
				Topology:    "4x4x4",
				State:       partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: "half-0",
						Topology:    "2x4x4",
						State:       partitiontreev1.PartitionStateHealthy,
						Taints:      []partitiontreev1.PartitionTaint{{Key: corev1.TaintNodeUnreachable, Effect: partitiontreev1.TaintEffectNoExecute}},
					},
					{PartitionID: "half-1", Topology: "2x4x4", State: partitiontreev1.PartitionStateHealthy},
				},
			},
		},
	}, nil)

	r := &SliceReconciler{watcher: watcher.NewPartitionWatcher(cache)}
	if r.shouldBackoffHealthy(logr.Discard(), true, []string{"cube"}) {
		t.Error("shouldBackoffHealthy() = true for a cube with an unreachable leaf, want false")
	}
}

var nodeUnreachable = partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnreachable, Effect: partitiontreev1.TaintEffectNoExecute}

// noExecuteTree is a v6e tree with two cubes, each split into two halves. leafTaints go
// on cube-a's first half, which is where the controller records a node's taints.
func noExecuteTree(leafTaints ...partitiontreev1.PartitionTaint) *partitiontreev1.PartitionTree {
	half := func(id string, taints []partitiontreev1.PartitionTaint) partitiontreev1.PartitionInfo {
		return partitiontreev1.PartitionInfo{PartitionID: id, Topology: "2x4", State: partitiontreev1.PartitionStateHealthy, Taints: taints}
	}
	return &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt-taint", Labels: map[string]string{TpuPodIDLabel: "pt-taint"}},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: TPU6e,
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "4x8",
				State:       partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{PartitionID: "cube-a", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy, ChildPartitions: []partitiontreev1.PartitionInfo{
						half("cube-a-0", leafTaints), half("cube-a-1", nil),
					}},
					{PartitionID: "cube-b", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy, ChildPartitions: []partitiontreev1.PartitionInfo{
						half("cube-b-0", nil), half("cube-b-1", nil),
					}},
				},
			},
		},
	}
}

func taintTestSlice(name, sliceType, managedBy string, partitionIDs ...string) *slicev1beta1.Slice {
	return &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				ManagedByAnnotation:      managedBy,
				SliceGroupNameAnnotation: name + "-group",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: slicev1beta1.Type(sliceType), Topology: "4x4", PartitionIds: partitionIDs},
	}
}

func newTaintReconciler(t *testing.T, filtering bool, tree *partitiontreev1.PartitionTree, slices ...*slicev1beta1.Slice) *SliceReconciler {
	t.Helper()
	return newTaintReconcilerWithOpts(t, tree, []watcher.CacheOption{watcher.WithTaintFiltering(filtering)}, slices...)
}

func newTaintReconcilerWithOpts(t *testing.T, tree *partitiontreev1.PartitionTree, opts []watcher.CacheOption, sliceObjs ...*slicev1beta1.Slice) *SliceReconciler {
	t.Helper()
	ctx := context.Background()
	objs := []client.Object{tree.DeepCopy()}
	for _, s := range sliceObjs {
		objs = append(objs, s.DeepCopy())
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithStatusSubresource(&slicev1beta1.Slice{}).WithObjects(objs...).Build()
	cache := watcher.NewPartitionCache(false, opts...)
	r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, events.NewFakeRecorder(10),
		false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	cache.UpdateTree(ctx, tree, nil)
	for _, s := range sliceObjs {
		cache.ReservePartition(ctx, sliceInfo(ctx, s))
	}
	return r
}

func newTestQueue() workqueue.TypedRateLimitingInterface[reconcile.Request] {
	return workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
}

func drainNames(q workqueue.TypedRateLimitingInterface[reconcile.Request]) []string {
	var names []string
	for q.Len() > 0 {
		req, _ := q.Get()
		names = append(names, req.Name)
		q.Done(req)
	}
	sort.Strings(names)
	return names
}

func TestHandlePartitionTreeUpdate_NoExecuteSweep(t *testing.T) {
	owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	elsewhere := taintTestSlice("elsewhere", TPU6e, interfaces.SliceSchedulerName, "cube-b")
	foreign := taintTestSlice("foreign", TPU6e, "someone-else", "cube-a-1")
	cordon := partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnschedulable, Effect: partitiontreev1.TaintEffectNoSchedule}

	tests := []struct {
		name      string
		filtering bool
		old, new  *partitiontreev1.PartitionTree
		want      []string
	}{
		{
			name:      "leaf gaining NoExecute requeues our slice on the cube above it",
			filtering: true,
			old:       noExecuteTree(),
			new:       noExecuteTree(nodeUnreachable),
			want:      []string{"owned"},
		},
		{
			name:      "taint already present is not swept again",
			filtering: true,
			old:       noExecuteTree(nodeUnreachable),
			new:       noExecuteTree(nodeUnreachable),
		},
		{
			name:      "NoSchedule does not evict",
			filtering: true,
			old:       noExecuteTree(),
			new:       noExecuteTree(cordon),
		},
		{
			name:      "filtering disabled",
			filtering: false,
			old:       noExecuteTree(),
			new:       noExecuteTree(nodeUnreachable),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTaintReconciler(t, tt.filtering, tt.old, owned, elsewhere, foreign)
			q := newTestQueue()
			defer q.ShutDown()

			r.handlePartitionTreeUpdate(context.Background(), event.TypedUpdateEvent[client.Object]{ObjectOld: tt.old, ObjectNew: tt.new}, q)

			if got := drainNames(q); !slices.Equal(got, tt.want) {
				t.Errorf("requeued %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandlePartitionTreeCreate_NoExecuteSweep(t *testing.T) {
	owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a-0")
	otherType := taintTestSlice("other-type", TPU7x, interfaces.SliceSchedulerName, "cube-a-1")
	// Seed the cache with an untainted copy so the slices are reserved, then deliver the
	// tainted tree as a create, as happens when a tree is recreated.
	r := newTaintReconciler(t, true, noExecuteTree(), owned, otherType)
	q := newTestQueue()
	defer q.ShutDown()

	r.handlePartitionTreeCreate(context.Background(), event.TypedCreateEvent[client.Object]{Object: noExecuteTree(nodeUnreachable)}, q)

	if got, want := drainNames(q), []string{"owned"}; !slices.Equal(got, want) {
		t.Errorf("requeued %v, want %v", got, want)
	}
}

func TestIsSliceOnNoExecutePartition(t *testing.T) {
	tests := []struct {
		name      string
		filtering bool
		taints    []partitiontreev1.PartitionTaint
		partition string
		marked    bool
		want      bool
	}{
		{name: "slice on the tainted leaf", filtering: true, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a-0", want: true},
		{name: "slice on the cube above it", filtering: true, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a", want: true},
		{name: "slice on an untainted sibling", filtering: true, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a-1", want: false},
		{name: "slice already marked for eviction", filtering: true, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a", marked: true, want: false},
		{name: "filtering disabled", filtering: false, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a", want: false},
		{
			name:      "nodes-not-found from the controller",
			filtering: true,
			taints:    []partitiontreev1.PartitionTaint{{Key: interfaces.TaintKeyNodesNotFound, Effect: partitiontreev1.TaintEffectNoExecute}},
			partition: "cube-a",
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := taintTestSlice("s", TPU6e, interfaces.SliceSchedulerName, tt.partition)
			if tt.marked {
				slice.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
			}
			r := newTaintReconciler(t, tt.filtering, noExecuteTree(tt.taints...))
			if got := r.isSliceOnNoExecutePartition(slice); got != tt.want {
				t.Errorf("isSliceOnNoExecutePartition() = %v, want %v", got, tt.want)
			}
			if got := r.needsScheduling(slice); got != tt.want {
				t.Errorf("needsScheduling() = %v, want %v", got, tt.want)
			}
		})
	}
}

func isMarkedForEviction(t *testing.T, r *SliceReconciler, name string) bool {
	t.Helper()
	slice := &slicev1beta1.Slice{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name}, slice); err != nil {
		t.Fatalf("Get(%s) error: %v", name, err)
	}
	return slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr
}

type skipBoundPolicy struct {
	policy.SlicePolicy
	targetName string
}

func (p *skipBoundPolicy) ShouldSkipReconcile(slice *slicev1beta1.Slice) (bool, string, time.Duration) {
	if len(slice.Spec.PartitionIds) > 0 && (p.targetName == "" || slice.Name == p.targetName) {
		return true, "custom policy freeze", time.Second
	}
	return p.SlicePolicy.ShouldSkipReconcile(slice)
}

func TestMarkSliceForEviction(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		slice        *slicev1beta1.Slice
		clearTaint   bool
		customPolicy policy.SlicePolicy
		sliceName    string
		wantMarked   bool
	}{
		{
			name:       "still tainted is marked",
			slice:      taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
			sliceName:  "owned",
			wantMarked: true,
		},
		{
			name:       "provisioned active sub-slice on tainted partition is marked",
			slice:      provisionedActiveSlice("owned", "cube-a"),
			sliceName:  "owned",
			wantMarked: true,
		},
		{
			name:       "taint cleared during the delay is left alone",
			slice:      taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
			clearTaint: true,
			sliceName:  "owned",
		},
		{
			name:      "slice managed by someone else is left alone",
			slice:     taintTestSlice("foreign", TPU6e, "someone-else", "cube-a"),
			sliceName: "foreign",
		},
		{
			name:         "bound slice skipped by custom policy is left alone",
			slice:        taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
			customPolicy: &skipBoundPolicy{SlicePolicy: policy.NewCloudPolicy()},
			sliceName:    "owned",
		},
		{
			name:      "deleted slice is not an error",
			sliceName: "gone",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objs []*slicev1beta1.Slice
			if tt.slice != nil {
				objs = append(objs, tt.slice)
			}
			r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), objs...)
			if tt.customPolicy != nil {
				r.policy = tt.customPolicy
			}
			if tt.clearTaint {
				r.cache.UpdateTree(ctx, noExecuteTree(), nil)
			}
			marked, err := r.markSliceForEviction(ctx, logr.Discard(), tt.sliceName,
				r.isSliceOnNoExecutePartition, r.recordNoExecuteEvictionEvent)
			if err != nil {
				t.Fatalf("markSliceForEviction() error: %v", err)
			}
			if marked != tt.wantMarked {
				t.Errorf("markSliceForEviction() = %v, want %v", marked, tt.wantMarked)
			}
			if tt.slice != nil && isMarkedForEviction(t, r, tt.sliceName) != tt.wantMarked {
				t.Errorf("evict-pods annotation set = %v, want %v", !tt.wantMarked, tt.wantMarked)
			}
		})
	}
}

// taintAwareScheduler is the test scheduler with TaintFilter added, so placement avoids
// tainted partitions the way cmd/main.go configures it.
func taintAwareScheduler() interfaces.SchedulerLibrary {
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 10}),
	}
	filters := []interfaces.FilterPlugin{
		&plugins.HealthyFilter{},
		&taint.TaintFilter{},
		&plugins.ManagedFilter{},
		&plugins.PriorityFilter{},
	}
	scorers := []interfaces.ScorePlugin{
		&plugins.PreemptionScorer{},
		&plugins.TightestFitScorer{},
	}
	return schedulerlib.NewDefaultScheduler(gens, filters, scorers)
}

func getSlice(t *testing.T, r *SliceReconciler, name string) *slicev1beta1.Slice {
	t.Helper()
	slice := &slicev1beta1.Slice{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name}, slice); err != nil {
		t.Fatalf("Get(%s) error: %v", name, err)
	}
	return slice
}

// delayRecorder captures the delay requeueWithDelayFn is given for each group; Reconcile
// and Start call it from a goroutine.
type delayRecorder struct {
	mu    sync.Mutex
	calls map[string]time.Duration
	done  chan struct{}
}

func newDelayRecorder() *delayRecorder {
	return &delayRecorder{calls: make(map[string]time.Duration), done: make(chan struct{}, 16)}
}

func (d *delayRecorder) record(_ context.Context, item *pq.Item, delay time.Duration) {
	d.mu.Lock()
	d.calls[item.GroupId] = delay
	d.mu.Unlock()
	d.done <- struct{}{}
}

func (d *delayRecorder) wait(t *testing.T, n int) map[string]time.Duration {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-d.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for requeue %d of %d", i+1, n)
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.calls)
}

// TestReconcile_NoExecuteUsesUnhealthyDelay checks that a slice on a NoExecute partition
// is queued after rescheduleUnhealthyDelay, like any other failed slice.
func TestReconcile_NoExecuteUsesUnhealthyDelay(t *testing.T) {
	slice := provisionedActiveSlice("owned", "cube-a")
	r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
	r.rescheduleUnhealthyDelay = 42 * time.Second
	rec := newDelayRecorder()
	r.requeueWithDelayFn = rec.record

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}}); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if got := rec.wait(t, 1)["owned-group"]; got != r.rescheduleUnhealthyDelay {
		t.Errorf("requeue delay = %v, want %v", got, r.rescheduleUnhealthyDelay)
	}
}

// TestTryScheduleSliceGroup_NoExecuteEvictsThenReschedules walks a slice through the whole
// move: the scheduling loop marks it, the pod reconciler unbinds it (N→0), and the next
// pass places it on a clean partition (0→M). Placing it straight there would be rejected.
func TestTryScheduleSliceGroup_NoExecuteEvictsThenReschedules(t *testing.T) {
	ctx := context.Background()
	slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
	r.schedulerLib = taintAwareScheduler()
	r.updateGroupCache(ctx, slice)

	if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
		t.Error("tryScheduleSliceGroup() requeued after marking the slice; the unbind requeues it")
	}
	got := getSlice(t, r, "owned")
	if got.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
		t.Fatal("slice not marked for eviction")
	}
	if !slices.Equal(got.Spec.PartitionIds, []string{"cube-a"}) {
		t.Fatalf("PartitionIds = %v, want unchanged [cube-a] until the pod reconciler unbinds", got.Spec.PartitionIds)
	}

	podReconciler := pod_reconciler.NewPodReconciler(r.Client, r.Client, scheme.Scheme, logr.Discard(), TPU6e, r.cache,
		events.NewFakeRecorder(10), 1, false, policy.NewCloudPolicy())
	if _, err := podReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}}); err != nil {
		t.Fatalf("PodReconciler.Reconcile() error: %v", err)
	}
	got = getSlice(t, r, "owned")
	if len(got.Spec.PartitionIds) != 0 {
		t.Fatalf("PartitionIds = %v after eviction, want empty", got.Spec.PartitionIds)
	}
	// What handleSliceUpdate does when it sees the unbind.
	r.cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, slice)})
	r.updateGroupCache(ctx, got)

	if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
		t.Fatal("tryScheduleSliceGroup() requeued instead of placing the unbound slice")
	}
	if got := getSlice(t, r, "owned").Spec.PartitionIds; !slices.Equal(got, []string{"cube-b"}) {
		t.Errorf("PartitionIds = %v, want [cube-b]", got)
	}
}

func TestTryScheduleSliceGroup_NoExecuteEviction(t *testing.T) {
	ctx := context.Background()

	t.Run("failed mark is retried", func(t *testing.T) {
		slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, slice)
		r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
			Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				return apierrors.NewInternalError(fmt.Errorf("injected"))
			},
		})

		requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
		if !requeue || delay != retryDelayBind {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)", requeue, delay, retryDelayBind)
		}
	})

	t.Run("taint cleared before the pass is left alone", func(t *testing.T) {
		slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, slice)
		r.cache.UpdateTree(ctx, noExecuteTree(), nil)

		if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
			t.Error("tryScheduleSliceGroup() requeued a healthy bound slice")
		}
		if got := getSlice(t, r, "owned"); got.Annotations[interfaces.EvictPodsAnnotation] != "" ||
			!slices.Equal(got.Spec.PartitionIds, []string{"cube-a"}) {
			t.Errorf("slice changed after its taint cleared: annotations %v, partitions %v", got.Annotations, got.Spec.PartitionIds)
		}
	})

	t.Run("only the tainted member of a gang is marked", func(t *testing.T) {
		tainted := provisionedActiveSlice("tainted", "cube-a")
		healthy := provisionedActiveSlice("healthy", "cube-b")
		for _, s := range []*slicev1beta1.Slice{tainted, healthy} {
			s.Annotations[SliceGroupNameAnnotation] = "gang"
			s.Annotations[NumSlicesAnnotation] = "2"
		}
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), tainted, healthy)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, tainted)
		r.updateGroupCache(ctx, healthy)

		r.tryScheduleSliceGroup(ctx, "gang")
		if !isMarkedForEviction(t, r, "tainted") {
			t.Error("tainted member not marked for eviction")
		}
		if isMarkedForEviction(t, r, "healthy") {
			t.Error("healthy member marked for eviction")
		}
	})

	gang := func(t *testing.T, markTainted, skipTaintedByPolicy bool) *SliceReconciler {
		tainted := provisionedActiveSlice("tainted", "cube-a")
		binding := provisionedActiveSlice("binding", "cube-b")
		for _, s := range []*slicev1beta1.Slice{tainted, binding} {
			s.Annotations[SliceGroupNameAnnotation] = "gang"
			s.Annotations[NumSlicesAnnotation] = "2"
		}
		if markTainted {
			tainted.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
		}
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), tainted, binding)
		if skipTaintedByPolicy {
			r.policy = &skipBoundPolicy{SlicePolicy: policy.NewCloudPolicy(), targetName: "tainted"}
		}
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, tainted)
		r.updateGroupCache(ctx, binding)
		r.sliceTasks["binding"] = SliceStateBinding
		return r
	}

	t.Run("gang blocked by a bind keeps retrying a tainted member", func(t *testing.T) {
		r := gang(t, false, false)
		requeue, delay := r.tryScheduleSliceGroup(ctx, "gang")
		if !requeue || delay != retryDelayBind {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)", requeue, delay, retryDelayBind)
		}
		if isMarkedForEviction(t, r, "tainted") {
			t.Error("tainted member marked while the gang is blocked")
		}

		r.sliceTasksMutex.Lock()
		delete(r.sliceTasks, "binding")
		r.sliceTasksMutex.Unlock()
		r.tryScheduleSliceGroup(ctx, "gang")
		if !isMarkedForEviction(t, r, "tainted") {
			t.Error("tainted member not marked once the bind finished")
		}
	})

	t.Run("gang blocked by a bind with the tainted member already marked is not retried", func(t *testing.T) {
		r := gang(t, true, false)
		if requeue, _ := r.tryScheduleSliceGroup(ctx, "gang"); requeue {
			t.Error("tryScheduleSliceGroup() requeued; the unbind requeues the group")
		}
	})

	t.Run("gang blocked by a bind with the tainted member skipped by policy is not retried", func(t *testing.T) {
		r := gang(t, false, true)
		if requeue, _ := r.tryScheduleSliceGroup(ctx, "gang"); requeue {
			t.Error("tryScheduleSliceGroup() requeued a member the evict pass would skip")
		}
	})

	t.Run("bound slice skipped by custom policy is not marked", func(t *testing.T) {
		slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
		r.policy = &skipBoundPolicy{SlicePolicy: policy.NewCloudPolicy(), targetName: "owned"}
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, slice)

		requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
		if !requeue || delay != time.Second {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, 1s)", requeue, delay)
		}
		if isMarkedForEviction(t, r, "owned") {
			t.Error("bound slice skipped by policy marked for eviction")
		}
	})

	t.Run("unbound slice being deactivated is not marked and requeues for deactivation", func(t *testing.T) {
		slice := deactivatingSlice("owned", "cube-a")
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, slice)

		requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
		if !requeue || delay != policy.DeactivationRequeueDelay {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)", requeue, delay, policy.DeactivationRequeueDelay)
		}
		if isMarkedForEviction(t, r, "owned") {
			t.Error("unbound slice being deactivated marked for eviction")
		}
	})
}

func deactivatingSlice(name string, provisionedPartition string) *slicev1beta1.Slice {
	slice := taintTestSlice(name, TPU6e, interfaces.SliceSchedulerName)
	slice.Annotations[policy.ProvisionedPartitionAnnotation] = provisionedPartition
	return slice
}

func provisionedActiveSlice(name string, partitionIDs ...string) *slicev1beta1.Slice {
	slice := taintTestSlice(name, TPU6e, interfaces.SliceSchedulerName, partitionIDs...)
	slice.Annotations[policy.ProvisionedPartitionAnnotation] = partitionIDs[0]
	slice.Generation = 2
	slice.Status.ObservedGeneration = 2
	return slice
}

func markedSlice(name string, partitionIDs ...string) *slicev1beta1.Slice {
	slice := taintTestSlice(name, TPU6e, interfaces.SliceSchedulerName, partitionIDs...)
	slice.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
	return slice
}

func TestStart_RequeuesSlicesOnNoExecutePartitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	secondTree := noExecuteTree(nodeUnreachable)
	secondTree.Name = "pt-taint-2"
	secondTree.Labels[TpuPodIDLabel] = "pt-taint-2"
	secondTree.Status.Root.PartitionID = "root-2"
	secondTree.Status.Root.ChildPartitions[0].PartitionID = "cube-c"
	secondTree.Status.Root.ChildPartitions[0].ChildPartitions[0].PartitionID = "cube-c-0"
	secondTree.Status.Root.ChildPartitions[0].ChildPartitions[1].PartitionID = "cube-c-1"
	secondTree.Status.Root.ChildPartitions[0].ChildPartitions[1].Taints = []partitiontreev1.PartitionTaint{nodeUnreachable}
	secondTree.Status.Root.ChildPartitions[1].PartitionID = "cube-d"
	secondTree.Status.Root.ChildPartitions[1].ChildPartitions[0].PartitionID = "cube-d-0"
	secondTree.Status.Root.ChildPartitions[1].ChildPartitions[0].Taints = []partitiontreev1.PartitionTaint{nodeUnreachable}
	secondTree.Status.Root.ChildPartitions[1].ChildPartitions[1].PartitionID = "cube-d-1"

	objs := []client.Object{
		noExecuteTree(nodeUnreachable),
		secondTree,
		provisionedActiveSlice("tainted", "cube-a"),
		taintTestSlice("healthy", TPU6e, interfaces.SliceSchedulerName, "cube-b"),
		taintTestSlice("foreign", TPU6e, "someone-else", "cube-d-0"),
		taintTestSlice("policy-skipped", TPU6e, interfaces.SliceSchedulerName, "cube-c-0"),
		deactivatingSlice("deactivating", "cube-a-0"),
		markedSlice("marked", "cube-c-1"),
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithStatusSubresource(&slicev1beta1.Slice{}).WithObjects(objs...).Build()
	customPolicy := &skipBoundPolicy{SlicePolicy: policy.NewCloudPolicy(), targetName: "policy-skipped"}
	r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, events.NewFakeRecorder(10),
		false, nil, watcher.NewPartitionCache(false, watcher.WithTaintFiltering(true)), 0, 7*time.Second, customPolicy, 0)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	rec := newDelayRecorder()
	r.requeueWithDelayFn = rec.record

	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	rec.wait(t, 1)
	// Give a stray second requeue a chance to land before checking there was only one.
	time.Sleep(50 * time.Millisecond)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.calls) != 1 || rec.calls["tainted-group"] != 7*time.Second {
		t.Errorf("startup requeues = %v, want only tainted-group after 7s", rec.calls)
	}
}

var nodeCordoned = partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnschedulable, Effect: partitiontreev1.TaintEffectNoSchedule}

// probeFailedSlice is a bound slice whose health probe failed: it needs rescheduling, but
// CloudPolicy neither evicts its pods nor skips the backoff for it.
func probeFailedSlice(name string, partitionIDs ...string) *slicev1beta1.Slice {
	slice := taintTestSlice(name, TPU6e, interfaces.SliceSchedulerName, partitionIDs...)
	slice.Status.Conditions = []metav1.Condition{{
		Type:   policy.ConditionTypeReady,
		Status: metav1.ConditionUnknown,
		Reason: policy.ReasonSliceHealthProbeFailed,
	}}
	return slice
}

// TestTryScheduleSliceGroup_UnbindsMovedSlice checks that a bound slice the solver places
// on other partitions is marked for eviction, with no victims picked, instead of being
// bound there, which assignPartitionsWithRetry would refuse.
func TestTryScheduleSliceGroup_UnbindsMovedSlice(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T, tree *partitiontreev1.PartitionTree, timeout time.Duration, slices ...*slicev1beta1.Slice) *SliceReconciler {
		r := newTaintReconciler(t, true, tree, slices...)
		r.schedulerLib = taintAwareScheduler()
		r.rescheduleTimeout = timeout
		r.rescheduleHealthyDelay = time.Minute
		for _, s := range slices {
			r.updateGroupCache(ctx, s)
		}
		return r
	}
	expireBackoff := func(r *SliceReconciler, name string) {
		r.failureMutex.Lock()
		r.failureTimestamps[name] = time.Now().Add(-time.Hour)
		r.failureMutex.Unlock()
	}
	wantUnbinding := func(t *testing.T, r *SliceReconciler, requeue bool) {
		t.Helper()
		if requeue {
			t.Error("tryScheduleSliceGroup() requeued after marking the slice; the unbind requeues it")
		}
		got := getSlice(t, r, "owned")
		if got.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
			t.Error("slice not marked for eviction")
		}
		if !slices.Equal(got.Spec.PartitionIds, []string{"cube-a"}) {
			t.Errorf("PartitionIds = %v, want unchanged [cube-a]", got.Spec.PartitionIds)
		}
		// The pods are still running there until the pod reconciler clears the partitions.
		if node, ok := r.cache.GetNode("cube-a"); !ok || node.InUseBy == nil || node.InUseBy.Name != "owned" {
			t.Error("cube-a not reserved for the slice being unbound")
		}
	}

	t.Run("cordoned: backs off while the backoff runs", func(t *testing.T) {
		r := setup(t, noExecuteTree(nodeCordoned), 10*time.Minute, probeFailedSlice("owned", "cube-a"))
		requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
		if !requeue || delay != r.rescheduleHealthyDelay {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)", requeue, delay, r.rescheduleHealthyDelay)
		}
		if isMarkedForEviction(t, r, "owned") {
			t.Error("slice marked for eviction during the backoff")
		}
	})

	t.Run("cordoned: marked once the backoff times out", func(t *testing.T) {
		r := setup(t, noExecuteTree(nodeCordoned), 10*time.Minute, probeFailedSlice("owned", "cube-a"))
		expireBackoff(r, "owned")
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
	})

	t.Run("cordoned: marked at once with the backoff disabled", func(t *testing.T) {
		r := setup(t, noExecuteTree(nodeCordoned), 0, probeFailedSlice("owned", "cube-a"))
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
	})

	t.Run("broken partition: marked without a backoff", func(t *testing.T) {
		tree := noExecuteTree()
		tree.Status.Root.ChildPartitions[0].State = partitiontreev1.PartitionStateUnhealthy
		r := setup(t, tree, 10*time.Minute, probeFailedSlice("owned", "cube-a"))
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
	})

	t.Run("no victim is preempted for the move", func(t *testing.T) {
		owned := probeFailedSlice("owned", "cube-a")
		owned.Annotations[SlicePriorityAnnotation] = "100"
		victim := taintTestSlice("victim", TPU6e, interfaces.SliceSchedulerName, "cube-b")
		r := setup(t, noExecuteTree(nodeCordoned), 0, owned, victim)
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
		if isMarkedForEviction(t, r, "victim") {
			t.Error("victim preempted for a placement that can't be bound")
		}
	})

	t.Run("untainted partition is rescheduled in place", func(t *testing.T) {
		r := setup(t, noExecuteTree(), 10*time.Minute, probeFailedSlice("owned", "cube-a"))
		expireBackoff(r, "owned")
		if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
			t.Error("tryScheduleSliceGroup() requeued instead of rescheduling in place")
		}
		if isMarkedForEviction(t, r, "owned") {
			t.Error("slice on an untainted partition marked for eviction")
		}
	})

	t.Run("healthy slice on a cordoned partition is left alone", func(t *testing.T) {
		r := setup(t, noExecuteTree(nodeCordoned), 0, taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a"))
		r.tryScheduleSliceGroup(ctx, "owned-group")
		if isMarkedForEviction(t, r, "owned") {
			t.Error("healthy slice marked for eviction by a cordon")
		}
	})

	t.Run("provisioned active sub-slice: marked once backoff times out", func(t *testing.T) {
		owned := probeFailedSlice("owned", "cube-a")
		owned.Annotations[policy.ProvisionedPartitionAnnotation] = "cube-a"
		r := setup(t, noExecuteTree(nodeCordoned), 10*time.Minute, owned)
		expireBackoff(r, "owned")
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
	})
}

// TestTryScheduleSliceGroup_ProvisionedSubsliceFullLifecycle walks a bound GKE sub-slice
// (carrying accelerator.gke.io/provisioned-partition) through NoExecute eviction, N->0
// unbind, both phases of slice-controller deactivation gating (provisioned-partition present
// and ObservedGeneration lag after provisioned-partition deletion), and 0->M placement.
func TestTryScheduleSliceGroup_ProvisionedSubsliceFullLifecycle(t *testing.T) {
	ctx := context.Background()
	slice := provisionedActiveSlice("owned", "cube-a")
	r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
	r.schedulerLib = taintAwareScheduler()
	r.updateGroupCache(ctx, slice)

	// 1. Live NoExecute taint marks the bound sub-slice for eviction even though
	// provisioned-partition="cube-a" is present.
	if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
		t.Fatal("tryScheduleSliceGroup() requeued after marking the slice")
	}
	got := getSlice(t, r, "owned")
	if got.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
		t.Fatal("provisioned active sub-slice not marked for eviction on NoExecute taint")
	}

	// 2. PodReconciler drains and clears PartitionIds=[] (leaving provisioned-partition="cube-a").
	podReconciler := pod_reconciler.NewPodReconciler(r.Client, r.Client, scheme.Scheme, logr.Discard(), TPU6e, r.cache,
		events.NewFakeRecorder(10), 1, false, policy.NewCloudPolicy())
	if _, err := podReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}}); err != nil {
		t.Fatalf("PodReconciler.Reconcile() error: %v", err)
	}
	got = getSlice(t, r, "owned")
	if len(got.Spec.PartitionIds) != 0 {
		t.Fatalf("PartitionIds = %v after eviction, want empty", got.Spec.PartitionIds)
	}
	// Simulate API server bumping metadata.generation on spec.partitionIds=[] write while
	// status.observedGeneration stays at 2 until slice-controller finishes N->0.
	got.Generation = 3
	if err := r.Update(ctx, got); err != nil {
		t.Fatalf("Update(generation=3) error: %v", err)
	}
	got = getSlice(t, r, "owned")
	r.cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, slice)})
	r.updateGroupCache(ctx, got)

	// 3. Deactivation Phase 1: PartitionIds=[] and provisioned-partition="cube-a" present -> must skip.
	requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
	if !requeue || delay != policy.DeactivationRequeueDelay {
		t.Fatalf("Phase 1 deactivation: tryScheduleSliceGroup() = (%v, %v), want (true, %v)",
			requeue, delay, policy.DeactivationRequeueDelay)
	}
	if ids := getSlice(t, r, "owned").Spec.PartitionIds; len(ids) != 0 {
		t.Fatalf("Phase 1 deactivation: slice prematurely scheduled onto %v", ids)
	}

	// 4. Deactivation Phase 2 (slice-controller Race Window A): provisioned-partition deleted
	// before status.observedGeneration catches up to metadata.generation -> must still skip.
	got = getSlice(t, r, "owned")
	delete(got.Annotations, policy.ProvisionedPartitionAnnotation)
	if err := r.Update(ctx, got); err != nil {
		t.Fatalf("Update(delete provisioned-partition) error: %v", err)
	}
	got = getSlice(t, r, "owned")
	r.updateGroupCache(ctx, got)

	requeue, delay = r.tryScheduleSliceGroup(ctx, "owned-group")
	if !requeue || delay != policy.DeactivationRequeueDelay {
		t.Fatalf("Phase 2 race window: tryScheduleSliceGroup() = (%v, %v), want (true, %v)",
			requeue, delay, policy.DeactivationRequeueDelay)
	}
	if ids := getSlice(t, r, "owned").Spec.PartitionIds; len(ids) != 0 {
		t.Fatalf("Phase 2 race window: slice prematurely scheduled onto %v", ids)
	}

	// 5. Deactivation complete: slice-controller updates status.observedGeneration == generation.
	got = getSlice(t, r, "owned")
	got.Status.ObservedGeneration = got.Generation
	if err := r.Status().Update(ctx, got); err != nil {
		t.Fatalf("Status().Update(observedGeneration) error: %v", err)
	}
	got = getSlice(t, r, "owned")
	r.updateGroupCache(ctx, got)

	if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
		t.Fatal("tryScheduleSliceGroup() requeued instead of placing slice after deactivation completed")
	}
	if ids := getSlice(t, r, "owned").Spec.PartitionIds; !slices.Equal(ids, []string{"cube-b"}) {
		t.Errorf("PartitionIds = %v, want [cube-b]", ids)
	}
}

func TestMovedSlices(t *testing.T) {
	groupSlices := map[string]*slicev1beta1.Slice{
		"new":    taintTestSlice("new", TPU6e, interfaces.SliceSchedulerName),
		"stays":  taintTestSlice("stays", TPU6e, interfaces.SliceSchedulerName, "cube-a-0", "cube-a-1"),
		"moves":  taintTestSlice("moves", TPU6e, interfaces.SliceSchedulerName, "cube-b"),
		"absent": taintTestSlice("absent", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
	}
	toReserve := []*interfaces.SliceInfo{
		{Name: "new", PartitionIds: []string{"cube-b-0"}},
		{Name: "stays", PartitionIds: []string{"cube-a-1", "cube-a-0"}},
		{Name: "moves", PartitionIds: []string{"cube-a"}},
	}
	moved := movedSlices(groupSlices, toReserve)
	if len(moved) != 1 || moved[0].Name != "moves" {
		t.Errorf("movedSlices() = %v, want [moves]", moved)
	}
}

// TestTryScheduleSliceGroup_AnyBlockingEffect checks that a NoSchedule or NoExecute taint
// with a key outside BlockedTaints keeps a new slice off its partition, and that a slice
// already running there is not evicted for it.
func TestTryScheduleSliceGroup_AnyBlockingEffect(t *testing.T) {
	ctx := context.Background()
	suspect := func(effect partitiontreev1.TaintEffect) partitiontreev1.PartitionTaint {
		return partitiontreev1.PartitionTaint{Key: "example.com/hw-suspect", Value: "repairing", Effect: effect}
	}

	for _, tt := range []struct {
		effect partitiontreev1.TaintEffect
		want   []string
	}{
		{effect: partitiontreev1.TaintEffectNoSchedule, want: []string{"cube-b"}},
		{effect: partitiontreev1.TaintEffectNoExecute, want: []string{"cube-b"}},
		{effect: partitiontreev1.TaintEffectPreferNoSchedule, want: []string{"cube-a"}},
	} {
		t.Run(string(tt.effect)+": new slice", func(t *testing.T) {
			slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName)
			r := newTaintReconciler(t, true, noExecuteTree(suspect(tt.effect)), slice)
			r.schedulerLib = taintAwareScheduler()
			r.updateGroupCache(ctx, slice)
			r.tryScheduleSliceGroup(ctx, "owned-group")
			if got := getSlice(t, r, "owned").Spec.PartitionIds; !slices.Equal(got, tt.want) {
				t.Errorf("PartitionIds = %v, want %v", got, tt.want)
			}
		})

		t.Run(string(tt.effect)+": running slice is left alone", func(t *testing.T) {
			slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
			r := newTaintReconciler(t, true, noExecuteTree(suspect(tt.effect)), slice)
			r.schedulerLib = taintAwareScheduler()
			r.updateGroupCache(ctx, slice)
			r.tryScheduleSliceGroup(ctx, "owned-group")
			got := getSlice(t, r, "owned")
			if got.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr {
				t.Error("running slice marked for eviction for a taint outside BlockedTaints")
			}
			if !slices.Equal(got.Spec.PartitionIds, []string{"cube-a"}) {
				t.Errorf("PartitionIds = %v, want unchanged [cube-a]", got.Spec.PartitionIds)
			}
		})
	}
}

// TestTryScheduleSliceGroup_ProvisionedSubsliceNoExecuteExhaustedThenRecovers verifies that
// when an active sub-slice on a NoExecute partition has NO healthy alternative partition:
//  1. It is still marked for eviction, drained, and unbound (N->0).
//  2. Both deactivation phases (provisioned-partition present and ObservedGeneration lag) gate it.
//  3. Once deactivated, it waits cleanly with retryDelayNotSchedulable while capacity is exhausted.
//  4. As soon as capacity becomes Healthy, it schedules 0->M onto the freed partition.
func TestTryScheduleSliceGroup_ProvisionedSubsliceNoExecuteExhaustedThenRecovers(t *testing.T) {
	ctx := context.Background()
	tree := noExecuteTree(nodeUnreachable)
	tree.Status.Root.ChildPartitions[1].State = partitiontreev1.PartitionStateUnhealthy

	slice := provisionedActiveSlice("owned", "cube-a")
	r := newTaintReconciler(t, true, tree, slice)
	r.schedulerLib = taintAwareScheduler()
	r.updateGroupCache(ctx, slice)

	// 1. Mark for NoExecute eviction even though cube-b is Unhealthy.
	if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
		t.Fatal("tryScheduleSliceGroup() requeued after marking slice on NoExecute partition")
	}
	if !isMarkedForEviction(t, r, "owned") {
		t.Fatal("active sub-slice not marked for eviction when capacity is exhausted")
	}

	// 2. PodReconciler drains and unbinds N->0.
	pr := pod_reconciler.NewPodReconciler(r.Client, r.Client, scheme.Scheme, logr.Discard(), TPU6e, r.cache,
		events.NewFakeRecorder(10), 1, false, policy.NewCloudPolicy())
	if _, err := pr.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}}); err != nil {
		t.Fatalf("PodReconciler.Reconcile() error: %v", err)
	}
	got := getSlice(t, r, "owned")
	got.Generation = 3
	if err := r.Update(ctx, got); err != nil {
		t.Fatalf("Update(generation=3) error: %v", err)
	}
	got = getSlice(t, r, "owned")
	r.cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, slice)})
	r.updateGroupCache(ctx, got)

	// 3. Deactivation Phase 1 & Phase 2 gate scheduling BEFORE invoking Schedule().
	// Drain prior eviction events so we can prove Schedule/handleUnschedulableResult do not run
	// (emitting 0 FailedToSchedule events) even though DeactivationRequeueDelay == retryDelayNotSchedulable.
	recorder := r.Recorder.(*events.FakeRecorder)
	for len(recorder.Events) > 0 {
		<-recorder.Events
	}
	if requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group"); !requeue || delay != policy.DeactivationRequeueDelay {
		t.Fatalf("Phase 1 deactivation = (%v, %v), want (true, %v)", requeue, delay, policy.DeactivationRequeueDelay)
	}
	delete(got.Annotations, policy.ProvisionedPartitionAnnotation)
	if err := r.Update(ctx, got); err != nil {
		t.Fatalf("Update(delete provisioned-partition) error: %v", err)
	}
	got = getSlice(t, r, "owned")
	r.updateGroupCache(ctx, got)
	if requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group"); !requeue || delay != policy.DeactivationRequeueDelay {
		t.Fatalf("Phase 2 deactivation = (%v, %v), want (true, %v)", requeue, delay, policy.DeactivationRequeueDelay)
	}
	if n := len(recorder.Events); n != 0 {
		t.Fatalf("deactivation gate emitted %d events during Phase 1 & Phase 2, want 0 (Schedule must not run)", n)
	}

	// 4. Deactivation finishes (ObservedGeneration == Generation); capacity still exhausted -> FailedToSchedule emitted.
	got.Status.ObservedGeneration = got.Generation
	if err := r.Status().Update(ctx, got); err != nil {
		t.Fatalf("Status().Update() error: %v", err)
	}
	got = getSlice(t, r, "owned")
	r.updateGroupCache(ctx, got)

	requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
	if !requeue || delay != retryDelayNotSchedulable {
		t.Fatalf("exhausted capacity = (%v, %v), want (true, %v)", requeue, delay, retryDelayNotSchedulable)
	}
	if n := len(recorder.Events); n != 1 {
		t.Fatalf("exhausted capacity emitted %d events, want 1 FailedToSchedule", n)
	}
	if ids := getSlice(t, r, "owned").Spec.PartitionIds; len(ids) != 0 {
		t.Fatalf("PartitionIds = %v while capacity exhausted, want empty", ids)
	}

	// 5. cube-b recovers to Healthy -> slice binds 0->M onto cube-b.
	healthyBTree := noExecuteTree(nodeUnreachable)
	r.cache.UpdateTree(ctx, healthyBTree, nil)
	if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
		t.Fatal("tryScheduleSliceGroup() requeued after cube-b became Healthy")
	}
	if ids := getSlice(t, r, "owned").Spec.PartitionIds; !slices.Equal(ids, []string{"cube-b"}) {
		t.Errorf("PartitionIds = %v, want [cube-b]", ids)
	}
}

// TestTryScheduleSliceGroup_PreemptProvisionedSubsliceFullLifecycle verifies that when a
// higher-priority slice preempts an active sub-slice (carrying provisioned-partition):
//  1. Preemptor binds the partition while victim is marked with cloud.google.com/evict-pods=true.
//  2. PodReconciler drains and unbinds victim (N->0).
//  3. Victim is gated through both Phase 1 (provisioned-partition) and Phase 2 (ObservedGeneration lag).
//  4. Once slice-controller finishes DEACTIVATE and another partition untaints, victim schedules 0->M.
func TestTryScheduleSliceGroup_PreemptProvisionedSubsliceFullLifecycle(t *testing.T) {
	ctx := context.Background()

	// Victim holds cube-a (priority 0); cube-b is cordoned (NoSchedule) so preemptor must take cube-a.
	tree := noExecuteTree()
	tree.Status.Root.ChildPartitions[1].Taints = []partitiontreev1.PartitionTaint{nodeCordoned}

	victim := provisionedActiveSlice("victim", "cube-a")
	preemptor := taintTestSlice("preemptor", TPU6e, interfaces.SliceSchedulerName)
	preemptor.Annotations[SlicePriorityAnnotation] = "100"

	r := newTaintReconciler(t, true, tree, victim, preemptor)
	r.schedulerLib = taintAwareScheduler()
	r.updateGroupCache(ctx, victim)
	r.updateGroupCache(ctx, preemptor)

	// 1. Preemptor preempts victim on cube-a.
	if requeue, _ := r.tryScheduleSliceGroup(ctx, "preemptor-group"); requeue {
		t.Fatal("preemptor-group unexpectedly requeued")
	}
	if ids := getSlice(t, r, "preemptor").Spec.PartitionIds; !slices.Equal(ids, []string{"cube-a"}) {
		t.Fatalf("preemptor PartitionIds = %v, want [cube-a]", ids)
	}
	if !isMarkedForEviction(t, r, "victim") {
		t.Fatal("provisioned sub-slice victim not marked for eviction on preemption")
	}

	// 2. PodReconciler drains and unbinds victim (N->0) without corrupting preemptor's cache hold.
	pr := pod_reconciler.NewPodReconciler(r.Client, r.Client, scheme.Scheme, logr.Discard(), TPU6e, r.cache,
		events.NewFakeRecorder(10), 1, false, policy.NewCloudPolicy())
	if _, err := pr.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "victim"}}); err != nil {
		t.Fatalf("PodReconciler.Reconcile(victim) error: %v", err)
	}
	gotVictim := getSlice(t, r, "victim")
	gotVictim.Generation = 3
	if err := r.Update(ctx, gotVictim); err != nil {
		t.Fatalf("Update(victim generation=3) error: %v", err)
	}
	gotVictim = getSlice(t, r, "victim")
	r.handleSliceUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: victim, ObjectNew: gotVictim})
	r.updateGroupCache(ctx, gotVictim)

	if node, ok := r.cache.GetNode("cube-a"); !ok || node.InUseBy == nil || node.InUseBy.Name != "preemptor" {
		t.Fatalf("cube-a holder after victim unbind = %+v, want preemptor", node.InUseBy)
	}

	// 3. Uncordon cube-b BEFORE Phase 1 & Phase 2 so cube-b is free and schedulable: without the
	// deactivation gate, victim would immediately bind cube-b and return (false, 0).
	r.cache.UpdateTree(ctx, noExecuteTree(), nil)
	if requeue, delay := r.tryScheduleSliceGroup(ctx, "victim-group"); !requeue || delay != policy.DeactivationRequeueDelay {
		t.Fatalf("victim Phase 1 deactivation = (%v, %v), want (true, %v)", requeue, delay, policy.DeactivationRequeueDelay)
	}
	if ids := getSlice(t, r, "victim").Spec.PartitionIds; len(ids) != 0 {
		t.Fatalf("victim Phase 1 prematurely scheduled onto %v", ids)
	}
	delete(gotVictim.Annotations, policy.ProvisionedPartitionAnnotation)
	if err := r.Update(ctx, gotVictim); err != nil {
		t.Fatalf("Update(delete victim provisioned-partition) error: %v", err)
	}
	gotVictim = getSlice(t, r, "victim")
	r.updateGroupCache(ctx, gotVictim)
	if requeue, delay := r.tryScheduleSliceGroup(ctx, "victim-group"); !requeue || delay != policy.DeactivationRequeueDelay {
		t.Fatalf("victim Phase 2 deactivation = (%v, %v), want (true, %v)", requeue, delay, policy.DeactivationRequeueDelay)
	}
	if ids := getSlice(t, r, "victim").Spec.PartitionIds; len(ids) != 0 {
		t.Fatalf("victim Phase 2 prematurely scheduled onto %v", ids)
	}

	// 4. Victim finishes DEACTIVATE (ObservedGeneration == Generation) -> victim binds cube-b.
	gotVictim.Status.ObservedGeneration = gotVictim.Generation
	if err := r.Status().Update(ctx, gotVictim); err != nil {
		t.Fatalf("Status().Update(victim) error: %v", err)
	}
	gotVictim = getSlice(t, r, "victim")
	r.updateGroupCache(ctx, gotVictim)

	if requeue, _ := r.tryScheduleSliceGroup(ctx, "victim-group"); requeue {
		t.Fatal("victim-group requeued after deactivation completed and cube-b uncordoned")
	}
	if ids := getSlice(t, r, "victim").Spec.PartitionIds; !slices.Equal(ids, []string{"cube-b"}) {
		t.Errorf("victim PartitionIds = %v, want [cube-b]", ids)
	}
}

// productionPluginScheduler configures DefaultScheduler with the exact generator, filter,
// and scorer plugin pipeline wired by cmd/main.go.
func productionPluginScheduler() interfaces.SchedulerLibrary {
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 3}),
	}
	filters := []interfaces.FilterPlugin{
		&plugins.ExcludeDegradedFilter{},
		&taint.TaintFilter{},
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

// TestTryScheduleSliceGroup_RescheduleKeepsHealthySiblingReservation verifies that when a
// gang is being rescheduled, only members in req.Slices (those needing scheduling) have
// their PartitionCache reservations temporarily released. Releasing all bound groupSlices
// drops healthy siblings' holds once reserveInCache only re-reserves req.Slices, allowing
// another slice (or the rescheduled sibling itself after unbind) to double-book the
// healthy sibling's partition.
func TestTryScheduleSliceGroup_RescheduleKeepsHealthySiblingReservation(t *testing.T) {
	ctx := context.Background()

	makeGang := func(members ...*slicev1beta1.Slice) {
		n := fmt.Sprintf("%d", len(members))
		for _, s := range members {
			s.Annotations[SliceGroupNameAnnotation] = "gang"
			s.Annotations[NumSlicesAnnotation] = n
		}
	}

	t.Run("failed_member_rescheduled_in_place_keeps_healthy_sibling_hold", func(t *testing.T) {
		healthy := provisionedActiveSlice("healthy", "cube-a")
		healthy.Status.Conditions = []metav1.Condition{{
			Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: policy.ReasonActive,
		}}
		failed := provisionedActiveSlice("failed", "cube-b")
		failed.Status.Conditions = []metav1.Condition{{
			Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: policy.ReasonSliceCreationFailed,
		}}
		makeGang(healthy, failed)

		r := newTaintReconciler(t, true, noExecuteTree(), healthy, failed)
		r.schedulerLib = productionPluginScheduler()
		r.rescheduleTimeout = 10 * time.Minute
		r.updateGroupCache(ctx, healthy)
		r.updateGroupCache(ctx, failed)

		if requeue, _ := r.tryScheduleSliceGroup(ctx, "gang"); requeue {
			t.Fatal("tryScheduleSliceGroup() requeued an in-place reschedule")
		}
		if nodeA, ok := r.cache.GetNode("cube-a"); !ok || nodeA.InUseBy == nil || nodeA.InUseBy.Name != "healthy" {
			t.Fatalf("cube-a cache holder = %+v, want healthy", nodeA.InUseBy)
		}
		if nodeB, ok := r.cache.GetNode("cube-b"); !ok || nodeB.InUseBy == nil || nodeB.InUseBy.Name != "failed" {
			t.Fatalf("cube-b cache holder = %+v, want failed", nodeB.InUseBy)
		}
	})

	t.Run("double_booking_prevented_after_failed_member_unbinds", func(t *testing.T) {
		healthy := provisionedActiveSlice("healthy", "cube-a")
		healthy.Status.Conditions = []metav1.Condition{{
			Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: policy.ReasonActive,
		}}
		failed := provisionedActiveSlice("failed", "cube-b")
		failed.Status.Conditions = []metav1.Condition{{
			Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: policy.ReasonSliceCreationFailed,
		}}
		makeGang(healthy, failed)

		r := newTaintReconciler(t, true, noExecuteTree(), healthy, failed)
		r.schedulerLib = productionPluginScheduler()
		r.rescheduleTimeout = 10 * time.Minute
		r.updateGroupCache(ctx, healthy)
		r.updateGroupCache(ctx, failed)

		// Pass 1: failed member reschedules in place onto cube-b.
		if requeue, _ := r.tryScheduleSliceGroup(ctx, "gang"); requeue {
			t.Fatal("Pass 1 tryScheduleSliceGroup() unexpectedly requeued")
		}

		// PodReconciler drains and unbinds failed (N->0), deactivation finishes, and cube-b goes Unhealthy.
		pr := pod_reconciler.NewPodReconciler(r.Client, r.Client, scheme.Scheme, logr.Discard(), TPU6e, r.cache,
			events.NewFakeRecorder(10), 1, false, policy.NewCloudPolicy())
		if _, err := pr.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "failed"}}); err != nil {
			t.Fatalf("PodReconciler.Reconcile(failed) error: %v", err)
		}
		gotFailed := getSlice(t, r, "failed")
		delete(gotFailed.Annotations, policy.ProvisionedPartitionAnnotation)
		gotFailed.Generation = 3
		if err := r.Update(ctx, gotFailed); err != nil {
			t.Fatalf("Update(failed) error: %v", err)
		}
		gotFailed = getSlice(t, r, "failed")
		gotFailed.Status.ObservedGeneration = 3
		gotFailed.Status.Conditions = []metav1.Condition{{
			Type: policy.ConditionTypeReady, Status: metav1.ConditionUnknown, Reason: "SliceNotCreated",
		}}
		if err := r.Status().Update(ctx, gotFailed); err != nil {
			t.Fatalf("Status().Update(failed) error: %v", err)
		}
		gotFailed = getSlice(t, r, "failed")
		r.cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, failed)})
		r.updateGroupCache(ctx, gotFailed)

		unhealthyB := noExecuteTree()
		unhealthyB.Status.Root.ChildPartitions[1].State = partitiontreev1.PartitionStateUnhealthy
		r.cache.UpdateTree(ctx, unhealthyB, nil)

		// Pass 2: cube-b is Unhealthy and cube-a is occupied by healthy, so failed must NOT bind cube-a.
		r.tryScheduleSliceGroup(ctx, "gang")
		gotHealthy := getSlice(t, r, "healthy")
		gotFailed = getSlice(t, r, "failed")
		if slices.Equal(gotFailed.Spec.PartitionIds, []string{"cube-a"}) {
			t.Fatalf("cube-a double-booked: healthy=%v failed=%v", gotHealthy.Spec.PartitionIds, gotFailed.Spec.PartitionIds)
		}
	})

	for _, tc := range []struct {
		name        string
		provisioned bool
	}{
		{name: "probe_failed_with_provisioned_partition_after_backoff", provisioned: true},
		{name: "probe_failed_without_provisioned_partition_after_backoff", provisioned: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			healthy := taintTestSlice("healthy", TPU6e, interfaces.SliceSchedulerName, "cube-a")
			healthy.Status.Conditions = []metav1.Condition{{
				Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: policy.ReasonActive,
			}}
			failed := probeFailedSlice("failed", "cube-b")
			if tc.provisioned {
				healthy.Annotations[policy.ProvisionedPartitionAnnotation] = "cube-a"
				healthy.Generation = 2
				healthy.Status.ObservedGeneration = 2
				failed.Annotations[policy.ProvisionedPartitionAnnotation] = "cube-b"
				failed.Generation = 2
				failed.Status.ObservedGeneration = 2
			}
			makeGang(healthy, failed)

			r := newTaintReconciler(t, true, noExecuteTree(), healthy, failed)
			r.schedulerLib = productionPluginScheduler()
			r.rescheduleTimeout = 10 * time.Minute
			r.rescheduleHealthyDelay = time.Minute
			r.failureMutex.Lock()
			r.failureTimestamps["failed"] = time.Now().Add(-time.Hour)
			r.failureMutex.Unlock()
			r.updateGroupCache(ctx, healthy)
			r.updateGroupCache(ctx, failed)

			if requeue, _ := r.tryScheduleSliceGroup(ctx, "gang"); requeue {
				t.Fatal("tryScheduleSliceGroup() requeued after backoff expired")
			}
			if nodeA, ok := r.cache.GetNode("cube-a"); !ok || nodeA.InUseBy == nil || nodeA.InUseBy.Name != "healthy" {
				t.Fatalf("cube-a cache holder = %+v, want healthy", nodeA.InUseBy)
			}
		})
	}

	t.Run("moved_member_eviction_keeps_both_members_reserved", func(t *testing.T) {
		tree := noExecuteTree(nodeCordoned)
		tree.Status.Root.ChildPartitions = append(tree.Status.Root.ChildPartitions, partitiontreev1.PartitionInfo{
			PartitionID: "cube-c", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy,
		})
		failed := probeFailedSlice("failed", "cube-a")
		failed.Annotations[policy.ProvisionedPartitionAnnotation] = "cube-a"
		failed.Generation = 2
		failed.Status.ObservedGeneration = 2
		healthy := provisionedActiveSlice("healthy", "cube-b")
		healthy.Status.Conditions = []metav1.Condition{{
			Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: policy.ReasonActive,
		}}
		makeGang(healthy, failed)

		r := newTaintReconciler(t, true, tree, healthy, failed)
		r.schedulerLib = productionPluginScheduler()
		r.rescheduleTimeout = 10 * time.Minute
		r.rescheduleHealthyDelay = time.Minute
		r.failureMutex.Lock()
		r.failureTimestamps["failed"] = time.Now().Add(-time.Hour)
		r.failureMutex.Unlock()
		r.updateGroupCache(ctx, healthy)
		r.updateGroupCache(ctx, failed)

		if requeue, _ := r.tryScheduleSliceGroup(ctx, "gang"); requeue {
			t.Fatal("tryScheduleSliceGroup() requeued after marking moved member")
		}
		if !isMarkedForEviction(t, r, "failed") {
			t.Error("moved failed member not marked for eviction")
		}
		if isMarkedForEviction(t, r, "healthy") {
			t.Error("healthy sibling marked for eviction")
		}
		if nodeA, ok := r.cache.GetNode("cube-a"); !ok || nodeA.InUseBy == nil || nodeA.InUseBy.Name != "failed" {
			t.Fatalf("cube-a cache holder = %+v, want failed until unbind", nodeA.InUseBy)
		}
		if nodeB, ok := r.cache.GetNode("cube-b"); !ok || nodeB.InUseBy == nil || nodeB.InUseBy.Name != "healthy" {
			t.Fatalf("cube-b cache holder = %+v, want healthy", nodeB.InUseBy)
		}
	})
}

// TestTryScheduleSliceGroup_UnschedulableRescheduleKeepsReservations verifies that when a
// rescheduling gang cannot be placed (!result.IsSchedulable) and async binding marks its
// bound members with evict-pods=true, tryScheduleSliceGroup restores the released members'
// PartitionCache holds before unlocking schedulingMutex so another group cannot double-book
// their hardware while pods are still draining.
func TestTryScheduleSliceGroup_UnschedulableRescheduleKeepsReservations(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name              string
		provisioned       bool
		gangTopology      string
		failedPartition   string
		contenderTopology string
		unhealthyHalfB1   bool
	}{
		{
			name:              "provisioned_2x4_gang_prevents_2x4_double_booking",
			provisioned:       true,
			gangTopology:      "2x4",
			failedPartition:   "cube-b-0",
			contenderTopology: "2x4",
			unhealthyHalfB1:   true,
		},
		{
			name:              "unannotated_2x4_gang_prevents_2x4_double_booking",
			provisioned:       false,
			gangTopology:      "2x4",
			failedPartition:   "cube-b-0",
			contenderTopology: "2x4",
			unhealthyHalfB1:   true,
		},
		{
			name:              "provisioned_4x4_gang_prevents_4x4_double_booking",
			provisioned:       true,
			gangTopology:      "4x4",
			failedPartition:   "cube-b",
			contenderTopology: "4x4",
		},
		{
			name:              "unannotated_4x4_gang_prevents_4x4_double_booking",
			provisioned:       false,
			gangTopology:      "4x4",
			failedPartition:   "cube-b",
			contenderTopology: "4x4",
		},
		{
			name:              "provisioned_4x4_gang_prevents_child_2x4_double_booking",
			provisioned:       true,
			gangTopology:      "4x4",
			failedPartition:   "cube-b",
			contenderTopology: "2x4",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tree := noExecuteTree()
			if tc.unhealthyHalfB1 {
				tree.Status.Root.ChildPartitions[1].ChildPartitions[1].State = partitiontreev1.PartitionStateUnhealthy
			}

			healthy := taintTestSlice("healthy", TPU6e, interfaces.SliceSchedulerName, "cube-a")
			healthy.Status.Conditions = []metav1.Condition{{
				Type: policy.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: policy.ReasonActive,
			}}
			failed := taintTestSlice("failed", TPU6e, interfaces.SliceSchedulerName, tc.failedPartition)
			failed.Spec.Topology = tc.gangTopology
			failed.Status.Conditions = []metav1.Condition{{
				Type: policy.ConditionTypeReady, Status: metav1.ConditionFalse, Reason: policy.ReasonSliceCreationFailed,
			}}
			unbound := taintTestSlice("unbound", TPU6e, interfaces.SliceSchedulerName)
			unbound.Spec.Topology = tc.gangTopology

			if tc.provisioned {
				healthy.Annotations[policy.ProvisionedPartitionAnnotation] = "cube-a"
				healthy.Generation = 2
				healthy.Status.ObservedGeneration = 2
				failed.Annotations[policy.ProvisionedPartitionAnnotation] = tc.failedPartition
				failed.Generation = 2
				failed.Status.ObservedGeneration = 2
			}
			for _, s := range []*slicev1beta1.Slice{healthy, failed, unbound} {
				s.Annotations[SliceGroupNameAnnotation] = "gang"
				s.Annotations[NumSlicesAnnotation] = "3"
			}

			contender := taintTestSlice("contender", TPU6e, interfaces.SliceSchedulerName)
			contender.Spec.Topology = tc.contenderTopology

			r := newTaintReconciler(t, true, tree, healthy, failed, unbound, contender)
			r.schedulerLib = productionPluginScheduler()
			r.rescheduleTimeout = 10 * time.Minute
			r.EnableAsyncBinding(10, 1)
			r.requeueWithDelayFn = func(context.Context, *pq.Item, time.Duration) {}

			for _, s := range []*slicev1beta1.Slice{healthy, failed, unbound, contender} {
				r.updateGroupCache(ctx, s)
			}

			drainAsyncTasks := func() {
				for len(r.bindingQueue) > 0 {
					r.processBindingTask(ctx, logr.Discard(), <-r.bindingQueue)
				}
			}

			// 1. Gang reschedule is unschedulable (healthy holds cube-a; remaining hardware cannot fit failed + unbound).
			// Async eviction stamps evict-pods=true without clearing PartitionIds until pods drain.
			r.tryScheduleSliceGroup(ctx, "gang")
			drainAsyncTasks()

			if got := getSlice(t, r, "failed"); !slices.Equal(got.Spec.PartitionIds, []string{tc.failedPartition}) {
				t.Fatalf("failed PartitionIds = %v, want [%s] while pods are draining", got.Spec.PartitionIds, tc.failedPartition)
			}
			if !isMarkedForEviction(t, r, "failed") || !isMarkedForEviction(t, r, "healthy") {
				t.Fatal("bound gang members were not marked with evict-pods=true on async unschedulable eviction")
			}
			if nodeA, ok := r.cache.GetNode("cube-a"); !ok || nodeA.InUseBy == nil || nodeA.InUseBy.Name != "healthy" {
				t.Fatalf("cube-a cache holder = %+v, want healthy", nodeA.InUseBy)
			}
			if nodeF, ok := r.cache.GetNode(tc.failedPartition); !ok || nodeF.InUseBy == nil || nodeF.InUseBy.Name != "failed" {
				t.Fatalf("%s cache holder = %+v, want failed", tc.failedPartition, nodeF.InUseBy)
			}

			// 2. Second group must NOT double-book failed's partition (or an overlapping child) while failed still holds it.
			requeue, delay := r.tryScheduleSliceGroup(ctx, "contender-group")
			drainAsyncTasks()
			if !requeue || delay != retryDelayNotSchedulable {
				t.Fatalf("contender tryScheduleSliceGroup() = (%v, %v), want (true, %v)",
					requeue, delay, retryDelayNotSchedulable)
			}
			if ids := getSlice(t, r, "contender").Spec.PartitionIds; len(ids) != 0 {
				t.Fatalf("contender double-booked %v while failed still holds [%s]", ids, tc.failedPartition)
			}
		})
	}
}

func TestNoExecuteToleration_WithinWindowDefersEvictionAndBlocksPlacement(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

	for _, taintKey := range []string{corev1.TaintNodeNotReady, corev1.TaintNodeUnreachable} {
		t.Run(taintKey, func(t *testing.T) {
			now := t0.Add(11 * time.Second)
			added := metav1.NewTime(t0)
			leafTaint := partitiontreev1.PartitionTaint{
				Key:       taintKey,
				Effect:    partitiontreev1.TaintEffectNoExecute,
				TimeAdded: &added,
			}

			owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a-0")
			owned.Spec.Topology = "2x4"
			newcomer := taintTestSlice("newcomer", TPU6e, interfaces.SliceSchedulerName)
			newcomer.Spec.Topology = "2x4"

			r := newTaintReconcilerWithOpts(t, noExecuteTree(leafTaint), []watcher.CacheOption{
				watcher.WithTaintFiltering(true),
				watcher.WithDefaultNotReadyTolerationSeconds(300),
				watcher.WithDefaultUnreachableTolerationSeconds(300),
				watcher.WithClock(func() time.Time { return now }),
			}, owned, newcomer)
			r.schedulerLib = taintAwareScheduler()
			r.rescheduleUnhealthyDelay = 5 * time.Second
			rec := newDelayRecorder()
			r.requeueWithDelayFn = rec.record

			// 1. At t0+11s (< 300s), Reconcile defers eviction via RequeueAfter=289s and never queues scheduling.
			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}})
			if err != nil {
				t.Fatalf("Reconcile(owned) error: %v", err)
			}
			if res.RequeueAfter != 289*time.Second {
				t.Errorf("Reconcile(owned) RequeueAfter = %v, want 289s", res.RequeueAfter)
			}
			if isMarkedForEviction(t, r, "owned") {
				t.Error("owned marked for eviction while still within toleration window")
			}

			// 2. Placement (R5): a new 2x4 slice must NOT be placed on tainted cube-a-0 (nor if cube-a-1 is tainted).
			// Taint cube-a-1 as well so all of cube-a is NoExecute-tainted while unoccupied on cube-a-1;
			// newcomer must place in cube-b.
			treeBothHalves := noExecuteTree(leafTaint)
			treeBothHalves.Status.Root.ChildPartitions[0].ChildPartitions[1].Taints = []partitiontreev1.PartitionTaint{leafTaint}
			r.cache.UpdateTree(ctx, treeBothHalves, nil)
			r.updateGroupCache(ctx, newcomer)
			if requeue, _ := r.tryScheduleSliceGroup(ctx, "newcomer-group"); requeue {
				t.Fatal("tryScheduleSliceGroup(newcomer) unexpectedly requeued")
			}
			gotNewcomer := getSlice(t, r, "newcomer")
			if len(gotNewcomer.Spec.PartitionIds) != 1 || (gotNewcomer.Spec.PartitionIds[0] != "cube-b-0" && gotNewcomer.Spec.PartitionIds[0] != "cube-b-1") {
				t.Errorf("newcomer PartitionIds = %v, want placement in cube-b (R5)", gotNewcomer.Spec.PartitionIds)
			}

			// 3. Recovery before deadline (t0+20s): taint clears, zero evictions occur.
			now = t0.Add(20 * time.Second)
			r.cache.UpdateTree(ctx, noExecuteTree(), nil)
			resAfterClear, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}})
			if err != nil {
				t.Fatalf("Reconcile(owned after clear) error: %v", err)
			}
			if resAfterClear.RequeueAfter != 0 {
				t.Errorf("Reconcile(owned after clear) RequeueAfter = %v, want 0", resAfterClear.RequeueAfter)
			}
			if isMarkedForEviction(t, r, "owned") {
				t.Error("owned marked for eviction after taint cleared within toleration window")
			}
		})
	}
}

func TestNoExecuteToleration_DeadlineExpiryAndUntoleratedShortening(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	added := metav1.NewTime(t0)
	notReady := partitiontreev1.PartitionTaint{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitiontreev1.TaintEffectNoExecute,
		TimeAdded: &added,
	}

	t.Run("evicts once toleration window expires", func(t *testing.T) {
		now := t0.Add(300 * time.Second)
		owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		r := newTaintReconcilerWithOpts(t, noExecuteTree(notReady), []watcher.CacheOption{
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(300),
			watcher.WithClock(func() time.Time { return now }),
		}, owned)
		r.schedulerLib = taintAwareScheduler()
		r.rescheduleUnhealthyDelay = 5 * time.Second
		rec := newDelayRecorder()
		r.requeueWithDelayFn = rec.record
		r.updateGroupCache(ctx, owned)

		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}
		if res.RequeueAfter != 0 {
			t.Errorf("Reconcile() RequeueAfter = %v, want 0 once deadline expired", res.RequeueAfter)
		}
		if got := rec.wait(t, 1)["owned-group"]; got != 5*time.Second {
			t.Errorf("requeueWithDelay = %v, want 5s", got)
		}
		if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
			t.Error("tryScheduleSliceGroup() unexpectedly requeued after marking slice")
		}
		if !isMarkedForEviction(t, r, "owned") {
			t.Error("owned not marked for eviction after toleration window expired")
		}
	})

	t.Run("untolerated partition-state NoExecute added mid-toleration triggers immediate sweep and eviction", func(t *testing.T) {
		now := t0.Add(10 * time.Second)
		owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		r := newTaintReconcilerWithOpts(t, noExecuteTree(notReady), []watcher.CacheOption{
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(300),
			watcher.WithClock(func() time.Time { return now }),
		}, owned)
		r.schedulerLib = taintAwareScheduler()
		r.rescheduleUnhealthyDelay = 5 * time.Second
		rec := newDelayRecorder()
		r.requeueWithDelayFn = rec.record

		// Initial Reconcile parks owned for the remaining 290s toleration window.
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}})
		if err != nil || res.RequeueAfter != 290*time.Second {
			t.Fatalf("initial Reconcile() = (%+v, %v), want RequeueAfter=290s", res, err)
		}

		// Now GCE reports partition-state=Unhealthy:NoExecute on cube-a while not-ready is still present!
		unhealthyTree := noExecuteTree(notReady)
		unhealthyTree.Status.Root.ChildPartitions[0].Taints = []partitiontreev1.PartitionTaint{{
			Key:       interfaces.TaintKeyPartitionState,
			Value:     string(partitiontreev1.PartitionStateUnhealthy),
			Effect:    partitiontreev1.TaintEffectNoExecute,
			TimeAdded: &added,
		}}
		q := newTestQueue()
		defer q.ShutDown()
		r.handlePartitionTreeUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectNew: unhealthyTree}, q)

		// handlePartitionTreeUpdate MUST re-enqueue owned because its eviction deadline shortened from t0+300s to immediate!
		if got := drainNames(q); !slices.Equal(got, []string{"owned"}) {
			t.Fatalf("handlePartitionTreeUpdate requeued %v, want [owned] when deadline shortened", got)
		}

		// Subsequent Reconcile + scheduling pass evicts immediately without waiting 290s.
		res2, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}})
		if err != nil || res2.RequeueAfter != 0 {
			t.Fatalf("Reconcile() after untolerated taint = (%+v, %v), want RequeueAfter=0", res2, err)
		}
		if got := rec.wait(t, 1)["owned-group"]; got != 5*time.Second {
			t.Errorf("requeueWithDelay = %v, want 5s", got)
		}
		r.tryScheduleSliceGroup(ctx, "owned-group")
		if !isMarkedForEviction(t, r, "owned") {
			t.Error("owned not marked for eviction for untolerated partition-state NoExecute")
		}
	})

	t.Run("zero toleration seconds evicts immediately and negative tolerates forever", func(t *testing.T) {
		now := t0.Add(10 * time.Second)
		ownedZero := taintTestSlice("owned-zero", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		rZero := newTaintReconcilerWithOpts(t, noExecuteTree(notReady), []watcher.CacheOption{
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(0),
			watcher.WithClock(func() time.Time { return now }),
		}, ownedZero)
		rZero.schedulerLib = taintAwareScheduler()
		rZero.updateGroupCache(ctx, ownedZero)
		rZero.tryScheduleSliceGroup(ctx, "owned-zero-group")
		if !isMarkedForEviction(t, rZero, "owned-zero") {
			t.Error("owned-zero not marked immediately when toleration is 0")
		}

		// Negative toleration = forever, even hours later.
		later := t0.Add(24 * time.Hour)
		ownedInf := taintTestSlice("owned-inf", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		rInf := newTaintReconcilerWithOpts(t, noExecuteTree(notReady), []watcher.CacheOption{
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(-1),
			watcher.WithClock(func() time.Time { return later }),
		}, ownedInf)
		res, err := rInf.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned-inf"}})
		if err != nil || res.RequeueAfter != 0 {
			t.Errorf("Reconcile(negative toleration) = (%+v, %v), want RequeueAfter=0", res, err)
		}
		if rInf.isSliceOnNoExecutePartition(ownedInf) {
			t.Error("isSliceOnNoExecutePartition() = true with negative (infinite) toleration")
		}
	})
}

func TestNoExecuteToleration_RestartPreservesRemainingDeadlineAndGangMinWait(t *testing.T) {
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

	t.Run("R3 scheduler restart mid-window queues remaining deadline from PartitionTaint.TimeAdded", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Partition was tainted 250s ago; 50s of the 300s toleration remain.
		now := t0.Add(250 * time.Second)
		added250sAgo := metav1.NewTime(t0)
		added180sAgo := metav1.NewTime(t0.Add(70 * time.Second)) // 120s remaining

		tree := noExecuteTree(partitiontreev1.PartitionTaint{
			Key:       corev1.TaintNodeNotReady,
			Effect:    partitiontreev1.TaintEffectNoExecute,
			TimeAdded: &added250sAgo,
		})
		// Second cube in the tree has 120s remaining; a 2-slice gang spans cube-a and cube-b.
		tree.Status.Root.ChildPartitions[1].ChildPartitions[0].Taints = []partitiontreev1.PartitionTaint{{
			Key:       corev1.TaintNodeUnreachable,
			Effect:    partitiontreev1.TaintEffectNoExecute,
			TimeAdded: &added180sAgo,
		}}

		gang0 := taintTestSlice("gang-0", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		gang1 := taintTestSlice("gang-1", TPU6e, interfaces.SliceSchedulerName, "cube-b")
		for _, s := range []*slicev1beta1.Slice{gang0, gang1} {
			s.Annotations[SliceGroupNameAnnotation] = "gang"
			s.Annotations[NumSlicesAnnotation] = "2"
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(tree, gang0, gang1).Build()
		cache := watcher.NewPartitionCache(false,
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(300),
			watcher.WithDefaultUnreachableTolerationSeconds(300),
			watcher.WithClock(func() time.Time { return now }),
		)
		r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, events.NewFakeRecorder(10),
			false, nil, cache, 0, 7*time.Second, policy.NewCloudPolicy(), 0)
		if err != nil {
			t.Fatalf("NewSliceReconciler() error: %v", err)
		}
		rec := newDelayRecorder()
		r.requeueWithDelayFn = rec.record

		if err := r.Start(ctx); err != nil {
			t.Fatalf("Start() error: %v", err)
		}

		calls := rec.wait(t, 1)
		time.Sleep(50 * time.Millisecond)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if len(rec.calls) != 1 || calls["gang"] != 50*time.Second {
			t.Errorf("startup requeues = %v, want single gang requeue after 50s (min of 50s and 120s)", rec.calls)
		}
	})

	t.Run("tryScheduleSliceGroup requeues gang for earliest remaining member toleration", func(t *testing.T) {
		ctx := context.Background()
		now := t0.Add(260 * time.Second) // 40s remaining on cube-a
		added := metav1.NewTime(t0)
		tree := noExecuteTree(partitiontreev1.PartitionTaint{
			Key:       corev1.TaintNodeNotReady,
			Effect:    partitiontreev1.TaintEffectNoExecute,
			TimeAdded: &added,
		})
		gang0 := taintTestSlice("gang-0", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		gang1 := taintTestSlice("gang-1", TPU6e, interfaces.SliceSchedulerName, "cube-b")
		for _, s := range []*slicev1beta1.Slice{gang0, gang1} {
			s.Annotations[SliceGroupNameAnnotation] = "gang"
			s.Annotations[NumSlicesAnnotation] = "2"
		}
		r := newTaintReconcilerWithOpts(t, tree, []watcher.CacheOption{
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(300),
			watcher.WithClock(func() time.Time { return now }),
		}, gang0, gang1)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, gang0)
		r.updateGroupCache(ctx, gang1)

		requeue, delay := r.tryScheduleSliceGroup(ctx, "gang")
		if !requeue || delay != 40*time.Second {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, 40s)", requeue, delay)
		}
		if isMarkedForEviction(t, r, "gang-0") {
			t.Error("gang-0 marked before toleration expired")
		}
	})
}

func TestNoExecuteToleration_FallbackWhenTimeAddedNil(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	now := t0

	// Older PartitionTree without TimeAdded populated.
	notReadyNilTime := partitiontreev1.PartitionTaint{
		Key:    corev1.TaintNodeNotReady,
		Effect: partitiontreev1.TaintEffectNoExecute,
	}
	unreachableNilTime := partitiontreev1.PartitionTaint{
		Key:    corev1.TaintNodeUnreachable,
		Effect: partitiontreev1.TaintEffectNoExecute,
	}

	owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	r := newTaintReconcilerWithOpts(t, noExecuteTree(notReadyNilTime), []watcher.CacheOption{
		watcher.WithTaintFiltering(true),
		watcher.WithDefaultNotReadyTolerationSeconds(300),
		watcher.WithDefaultUnreachableTolerationSeconds(300),
		watcher.WithClock(func() time.Time { return now }),
	}, owned)
	r.schedulerLib = taintAwareScheduler()
	r.updateGroupCache(ctx, owned)

	// 1. At t0+50s, swap not-ready -> unreachable (still nil TimeAdded): fallback must preserve t0!
	now = t0.Add(50 * time.Second)
	r.cache.UpdateTree(ctx, noExecuteTree(unreachableNilTime), nil)
	res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}})
	if err != nil {
		t.Fatalf("Reconcile at t0+50s error: %v", err)
	}
	if res.RequeueAfter != 250*time.Second {
		t.Fatalf("Reconcile at t0+50s RequeueAfter = %v, want 250s (preserved from t0 across swap)", res.RequeueAfter)
	}

	// 2. At t0+300s, repeated UpdateTree calls must NOT push the deadline forward; slice evicts!
	now = t0.Add(300 * time.Second)
	r.cache.UpdateTree(ctx, noExecuteTree(unreachableNilTime), nil)
	r.tryScheduleSliceGroup(ctx, "owned-group")
	if !isMarkedForEviction(t, r, "owned") {
		t.Fatal("owned not marked for eviction at t0+300s in nil-TimeAdded fallback mode")
	}

	// 3. Clearing the taint and re-tainting later starts a fresh 300s window.
	now = t0.Add(400 * time.Second)
	r.cache.UpdateTree(ctx, noExecuteTree(), nil)
	now = t0.Add(500 * time.Second)
	r.cache.UpdateTree(ctx, noExecuteTree(notReadyNilTime), nil)

	owned2 := taintTestSlice("owned-2", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	if err := r.Create(ctx, owned2); err != nil {
		t.Fatalf("Create(owned-2): %v", err)
	}
	r.cache.ReservePartition(ctx, sliceInfo(ctx, owned2))
	now = t0.Add(510 * time.Second)
	res2, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned-2"}})
	if err != nil || res2.RequeueAfter != 290*time.Second {
		t.Errorf("Reconcile(owned-2) after clear+re-taint = (%+v, %v), want RequeueAfter=290s", res2, err)
	}
}

func TestSlicePredicate_NoExecuteTolerationWindowAndLateBind(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	added := metav1.NewTime(t0)
	notReady := partitiontreev1.PartitionTaint{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitiontreev1.TaintEffectNoExecute,
		TimeAdded: &added,
	}

	t.Run("predicate matrix inside NoExecute toleration window", func(t *testing.T) {
		now := t0.Add(15 * time.Second) // 285s remaining on cube-a
		r := newTaintReconcilerWithOpts(t, noExecuteTree(notReady), []watcher.CacheOption{
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(300),
			watcher.WithClock(func() time.Time { return now }),
		})
		// Freeze only the slice named "policy-skipped" so the remaining cases run under
		// CloudPolicy, which no longer skips bound (active) sub-slices.
		r.policy = &skipBoundPolicy{SlicePolicy: policy.NewCloudPolicy(), targetName: "policy-skipped"}
		pred := r.slicePredicate()

		deleting := taintTestSlice("deleting", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		delTS := metav1.NewTime(now)
		deleting.DeletionTimestamp = &delTS

		cases := []struct {
			name  string
			slice *slicev1beta1.Slice
			want  bool
		}{
			{
				name:  "unbound owned slice needs scheduling",
				slice: taintTestSlice("unbound", TPU6e, interfaces.SliceSchedulerName),
				want:  true,
			},
			{
				name:  "owned slice bound inside toleration window admitted to arm RequeueAfter",
				slice: taintTestSlice("windowed", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
				want:  true,
			},
			{
				name:  "active provisioned sub-slice inside toleration window admitted to arm RequeueAfter",
				slice: provisionedActiveSlice("active-sub", "cube-a"),
				want:  true,
			},
			{
				name:  "owned slice bound on healthy partition skipped",
				slice: taintTestSlice("healthy", TPU6e, interfaces.SliceSchedulerName, "cube-b"),
				want:  false,
			},
			{
				name:  "foreign slice on windowed partition skipped",
				slice: taintTestSlice("foreign", TPU6e, "other-scheduler", "cube-a"),
				want:  false,
			},
			{
				name:  "wrong TPU type on windowed partition skipped",
				slice: taintTestSlice("wrong-type", TPU7x, interfaces.SliceSchedulerName, "cube-a"),
				want:  false,
			},
			{
				name:  "deleting slice on windowed partition skipped",
				slice: deleting,
				want:  false,
			},
			{
				name:  "already-marked slice on windowed partition skipped",
				slice: markedSlice("marked", "cube-a"),
				want:  false,
			},
			{
				name:  "policy-skipped bound slice on windowed partition skipped",
				slice: taintTestSlice("policy-skipped", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
				want:  false,
			},
			{
				name:  "unbound deactivating slice admitted via needsScheduling for the Reconcile deactivation gate",
				slice: deactivatingSlice("deactivating", "cube-a"),
				want:  true,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if got := pred.Create(event.CreateEvent{Object: tc.slice}); got != tc.want {
					t.Errorf("pred.Create(%s) = %v, want %v", tc.name, got, tc.want)
				}
				if got := pred.Update(event.UpdateEvent{ObjectOld: tc.slice, ObjectNew: tc.slice}); got != tc.want {
					t.Errorf("pred.Update(%s) = %v, want %v", tc.name, got, tc.want)
				}
			})
		}

		if pred.Create(event.CreateEvent{Object: &corev1.Pod{}}) {
			t.Error("pred.Create(Pod) = true, want false")
		}
		if pred.Update(event.UpdateEvent{ObjectNew: &corev1.Pod{}}) {
			t.Error("pred.Update(Pod) = true, want false")
		}
		if pred.Delete(event.DeleteEvent{Object: taintTestSlice("s", TPU6e, interfaces.SliceSchedulerName, "cube-a")}) {
			t.Error("pred.Delete() = true, want false")
		}

		// Negative toleration (tolerate forever) must NOT admit bound slices on that partition.
		rInf := newTaintReconcilerWithOpts(t, noExecuteTree(notReady), []watcher.CacheOption{
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(-1),
			watcher.WithClock(func() time.Time { return now }),
		})
		infSlice := taintTestSlice("inf", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		if rInf.slicePredicate().Update(event.UpdateEvent{ObjectOld: infSlice, ObjectNew: infSlice}) {
			t.Error("pred.Update(negative toleration) = true, want false")
		}
	})

	t.Run("late bind finishing after partition toleration window started sets eviction timer and evicts at expiry", func(t *testing.T) {
		now := t0
		unbound := taintTestSlice("late-bound", TPU6e, interfaces.SliceSchedulerName)
		r := newTaintReconcilerWithOpts(t, noExecuteTree(), []watcher.CacheOption{
			watcher.WithTaintFiltering(true),
			watcher.WithDefaultNotReadyTolerationSeconds(300),
			watcher.WithClock(func() time.Time { return now }),
		})
		if err := r.Create(ctx, unbound); err != nil {
			t.Fatalf("Create(unbound): %v", err)
		}
		r.schedulerLib = taintAwareScheduler()
		r.rescheduleUnhealthyDelay = 5 * time.Second
		rec := newDelayRecorder()
		r.requeueWithDelayFn = rec.record
		r.updateGroupCache(ctx, unbound)

		// 1. Scheduler reserves cube-a in memory while async API bind is still in flight (Spec.PartitionIds still empty in API).
		reserved := taintTestSlice("late-bound", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		r.cache.ReservePartition(ctx, sliceInfo(ctx, reserved))

		// 2. PartitionTree gains not-ready:NoExecute at t0 while API object is still unbound.
		q := newTestQueue()
		defer q.ShutDown()
		r.handlePartitionTreeUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectNew: noExecuteTree(notReady)}, q)
		_ = drainNames(q) // Earlier sweep ran before Spec.PartitionIds was committed!

		// 3. At t0+5s, async API bind commits Spec.PartitionIds=["cube-a"].
		now = t0.Add(5 * time.Second)
		bound := getSlice(t, r, "late-bound")
		oldCopy := bound.DeepCopy()
		bound.Spec.PartitionIds = []string{"cube-a"}
		if err := r.Update(ctx, bound); err != nil {
			t.Fatalf("Update(bound): %v", err)
		}
		r.handleSliceUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: oldCopy, ObjectNew: bound})

		// For() Update predicate MUST admit the newly-bound slice so Reconcile arms RequeueAfter=295s!
		if !r.slicePredicate().Update(event.UpdateEvent{ObjectOld: oldCopy, ObjectNew: bound}) {
			t.Fatal("slicePredicate().Update() dropped slice whose bind finished inside NoExecute toleration window")
		}
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "late-bound"}})
		if err != nil {
			t.Fatalf("Reconcile(late-bound at t0+5s) error: %v", err)
		}
		if res.RequeueAfter != 295*time.Second {
			t.Fatalf("Reconcile(late-bound at t0+5s) RequeueAfter = %v, want 295s", res.RequeueAfter)
		}

		// 4. At t0+300s (when RequeueAfter fires), Reconcile queues the group and marks late-bound for eviction!
		now = t0.Add(300 * time.Second)
		resExpired, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "late-bound"}})
		if err != nil || resExpired.RequeueAfter != 0 {
			t.Fatalf("Reconcile(at expiry) = (%+v, %v), want RequeueAfter=0", resExpired, err)
		}
		if got := rec.wait(t, 1)["late-bound-group"]; got != 5*time.Second {
			t.Errorf("requeueWithDelay = %v, want 5s", got)
		}
		r.tryScheduleSliceGroup(ctx, "late-bound-group")
		if !isMarkedForEviction(t, r, "late-bound") {
			t.Error("late-bound slice was not marked for eviction once toleration window expired")
		}
	})
}

func TestTryScheduleSliceGroup_BlockedGangRequeuesAtTolerationDeadline(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	now := t0.Add(60 * time.Second)
	added := metav1.NewTime(t0)
	notReady := partitiontreev1.PartitionTaint{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitiontreev1.TaintEffectNoExecute,
		TimeAdded: &added,
	}
	tainted := taintTestSlice("tainted", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	binding := taintTestSlice("binding", TPU6e, interfaces.SliceSchedulerName, "cube-b")
	for _, s := range []*slicev1beta1.Slice{tainted, binding} {
		s.Annotations[SliceGroupNameAnnotation] = "gang"
		s.Annotations[NumSlicesAnnotation] = "2"
	}
	r := newTaintReconcilerWithOpts(t, noExecuteTree(notReady), []watcher.CacheOption{
		watcher.WithTaintFiltering(true),
		watcher.WithDefaultNotReadyTolerationSeconds(300),
		watcher.WithClock(func() time.Time { return now }),
	}, tainted, binding)
	r.schedulerLib = taintAwareScheduler()
	r.updateGroupCache(ctx, tainted)
	r.updateGroupCache(ctx, binding)
	r.sliceTasks["binding"] = SliceStateBinding

	if requeue, delay := r.tryScheduleSliceGroup(ctx, "gang"); !requeue || delay != 240*time.Second {
		t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, 240s)", requeue, delay)
	}
	if isMarkedForEviction(t, r, "tainted") {
		t.Error("tainted member marked inside its toleration window")
	}
}

// TestNoExecuteToleration_ProvisionedActiveSubsliceDefersThenEvicts verifies that an active
// GKE sub-slice (bound, carrying accelerator.gke.io/provisioned-partition for its whole
// lifetime, which CloudPolicy no longer skips) gets the same not-ready NoExecute toleration
// window as any other bound slice: deferred inside the window, marked once it expires.
func TestNoExecuteToleration_ProvisionedActiveSubsliceDefersThenEvicts(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	now := t0.Add(11 * time.Second)
	added := metav1.NewTime(t0)
	notReady := partitiontreev1.PartitionTaint{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitiontreev1.TaintEffectNoExecute,
		TimeAdded: &added,
	}

	sub := provisionedActiveSlice("active-sub", "cube-a-0")
	sub.Spec.Topology = "2x4"
	r := newTaintReconcilerWithOpts(t, noExecuteTree(notReady), []watcher.CacheOption{
		watcher.WithTaintFiltering(true),
		watcher.WithDefaultNotReadyTolerationSeconds(300),
		watcher.WithClock(func() time.Time { return now }),
	}, sub)
	r.schedulerLib = taintAwareScheduler()
	r.rescheduleUnhealthyDelay = 5 * time.Second
	rec := newDelayRecorder()
	r.requeueWithDelayFn = rec.record
	r.updateGroupCache(ctx, sub)

	if !r.slicePredicate().Update(event.UpdateEvent{ObjectOld: sub, ObjectNew: sub}) {
		t.Fatal("slicePredicate().Update() dropped an active sub-slice inside its toleration window")
	}

	// Inside the window (t0+11s): deferred via RequeueAfter=289s and not marked.
	res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "active-sub"}})
	if err != nil || res.RequeueAfter != 289*time.Second {
		t.Fatalf("Reconcile(t0+11s) = (%+v, %v), want RequeueAfter=289s", res, err)
	}
	if requeue, delay := r.tryScheduleSliceGroup(ctx, "active-sub-group"); !requeue || delay != 289*time.Second {
		t.Errorf("tryScheduleSliceGroup(t0+11s) = (%v, %v), want (true, 289s)", requeue, delay)
	}
	if isMarkedForEviction(t, r, "active-sub") {
		t.Fatal("active sub-slice marked for eviction inside its toleration window")
	}

	// Window expired (t0+300s): Reconcile queues the group, and the sweep marks the sub-slice.
	now = t0.Add(300 * time.Second)
	res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "active-sub"}})
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("Reconcile(t0+300s) = (%+v, %v), want RequeueAfter=0", res, err)
	}
	if got := rec.wait(t, 1)["active-sub-group"]; got != 5*time.Second {
		t.Errorf("requeueWithDelay = %v, want 5s", got)
	}
	r.tryScheduleSliceGroup(ctx, "active-sub-group")
	if !isMarkedForEviction(t, r, "active-sub") {
		t.Error("active sub-slice not marked for eviction after its toleration window expired")
	}
}

// TestNoExecuteToleration_UpgradeTimeAddedAppearsThenDisappears covers a mixed-version
// rollout: the scheduler starts while an older partition-tree controller publishes the
// not-ready taint without timeAdded (fallback first-seen), the upgraded controller later
// publishes the node's earlier original timeAdded (deadline must move earlier and the slice
// be re-swept), and a rollback that drops timeAdded again must not reset the clock.
func TestNoExecuteToleration_UpgradeTimeAddedAppearsThenDisappears(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC) // node went NotReady (node taint timeAdded)
	now := t0.Add(60 * time.Second)                     // scheduler first observes the taint
	nilTime := partitiontreev1.PartitionTaint{Key: corev1.TaintNodeNotReady, Effect: partitiontreev1.TaintEffectNoExecute}
	added := metav1.NewTime(t0)
	withTime := partitiontreev1.PartitionTaint{Key: corev1.TaintNodeNotReady, Effect: partitiontreev1.TaintEffectNoExecute, TimeAdded: &added}

	owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	r := newTaintReconcilerWithOpts(t, noExecuteTree(nilTime), []watcher.CacheOption{
		watcher.WithTaintFiltering(true),
		watcher.WithDefaultNotReadyTolerationSeconds(300),
		watcher.WithClock(func() time.Time { return now }),
	}, owned)
	r.schedulerLib = taintAwareScheduler()
	r.rescheduleUnhealthyDelay = 5 * time.Second
	rec := newDelayRecorder()
	r.requeueWithDelayFn = rec.record
	r.updateGroupCache(ctx, owned)
	reconcileOwned := func(when string) time.Duration {
		t.Helper()
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}})
		if err != nil {
			t.Fatalf("Reconcile(%s) error: %v", when, err)
		}
		return res.RequeueAfter
	}

	// 1. Old partition-tree (no timeAdded): no immediate eviction; the fallback clock starts at t0+60s.
	if got := reconcileOwned("t0+60s, old partition-tree"); got != 300*time.Second {
		t.Fatalf("RequeueAfter = %v, want 300s from the fallback first-seen time", got)
	}

	// 2. Upgraded partition-tree publishes timeAdded=t0: the deadline moves from t0+360s to t0+300s.
	now = t0.Add(120 * time.Second)
	q := newTestQueue()
	defer q.ShutDown()
	r.handlePartitionTreeUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectNew: noExecuteTree(withTime)}, q)
	if got := drainNames(q); !slices.Equal(got, []string{"owned"}) {
		t.Fatalf("handlePartitionTreeUpdate requeued %v, want [owned] when timeAdded moved the deadline earlier", got)
	}
	if got := reconcileOwned("t0+120s, upgraded partition-tree"); got != 180*time.Second {
		t.Fatalf("RequeueAfter = %v, want 180s (anchored at the node's original t0)", got)
	}

	// 3. Rollback drops timeAdded again: the deadline stays anchored at t0 and nothing is re-swept.
	now = t0.Add(150 * time.Second)
	r.handlePartitionTreeUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectNew: noExecuteTree(nilTime)}, q)
	if got := drainNames(q); len(got) != 0 {
		t.Errorf("handlePartitionTreeUpdate requeued %v after rollback, want none (deadline unchanged)", got)
	}
	if got := reconcileOwned("t0+150s, rolled-back partition-tree"); got != 150*time.Second {
		t.Fatalf("RequeueAfter = %v, want 150s (rollback must not reset the clock)", got)
	}

	// 4. The window expires at t0+300s and the slice is marked.
	now = t0.Add(300 * time.Second)
	if got := reconcileOwned("t0+300s"); got != 0 {
		t.Fatalf("RequeueAfter = %v, want 0 once the deadline expired", got)
	}
	r.tryScheduleSliceGroup(ctx, "owned-group")
	if !isMarkedForEviction(t, r, "owned") {
		t.Error("owned not marked for eviction at t0+300s")
	}
}

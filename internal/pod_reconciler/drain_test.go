/*
Copyright 2025.

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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

// drainTestEnv is a PodReconciler over a fake cluster, with the subresource
// writes it makes recorded.
type drainTestEnv struct {
	r      *PodReconciler
	client client.WithWatch
	sub    *mockSubResourceClient
}

// newDrainTestEnv builds a reconciler whose cache holds objs. apiReader stands
// in for the API server; nil reads the same objects as the cache.
func newDrainTestEnv(t *testing.T, apiReader client.Reader, objs ...client.Object) *drainTestEnv {
	t.Helper()
	s := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
			pod := rawObj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		Build()
	sub := &mockSubResourceClient{SubResourceClient: fakeClient.SubResource("binding")}
	if apiReader == nil {
		apiReader = fakeClient
	}
	cache := watcher.NewPartitionCache(false)
	cache.UpdateTree(context.Background(), &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{PartitionID: "part-1", NodeNames: []string{"node-1", "node-2"}},
		},
	}, nil)
	r := NewPodReconciler(&mockClient{Client: fakeClient, subResourceClient: sub}, apiReader, s,
		zap.New(zap.UseDevMode(true)), testTPUType, cache, events.NewFakeRecorder(100), 1, false,
		policy.NewCloudPolicy())
	return &drainTestEnv{r: r, client: fakeClient, sub: sub}
}

func (e *drainTestEnv) reconcile(t *testing.T, sliceName string) (ctrl.Result, error) {
	t.Helper()
	return e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName}})
}

func (e *drainTestEnv) partitionIds(t *testing.T, sliceName string) []string {
	t.Helper()
	slice := &slicev1beta1.Slice{}
	if err := e.client.Get(context.Background(), types.NamespacedName{Name: sliceName}, slice); err != nil {
		t.Fatalf("failed to get slice %s: %v", sliceName, err)
	}
	return slice.Spec.PartitionIds
}

// drainSlice is a slice this scheduler owns, bound to part-1 and marked for eviction.
func drainSlice(name string) *slicev1beta1.Slice {
	return &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				watcher.ManagedByAnnotation:    interfaces.SliceSchedulerName,
				interfaces.EvictPodsAnnotation: interfaces.TrueStr,
			},
		},
		Spec: slicev1beta1.SliceSpec{
			Type:         slicev1beta1.TypeV6e,
			Topology:     "2x2",
			PartitionIds: []string{"part-1"},
		},
	}
}

// drainPod is a pod of the slice, bound to nodeName unless it is empty.
func drainPod(name, sliceName, nodeName string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{SliceNameLabel: sliceName},
		},
		Spec: corev1.PodSpec{
			SchedulerName: interfaces.SliceSchedulerName,
			NodeName:      nodeName,
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func TestHandleEviction_WaitsOnlyForPlacedPods(t *testing.T) {
	tests := []struct {
		name         string
		pods         []client.Object
		wantEviction bool
		wantUnbound  bool
		wantRequeued bool
	}{
		{
			name:         "running pod is evicted and the slice stays bound",
			pods:         []client.Object{drainPod("w-0", "s", "node-1", corev1.PodRunning)},
			wantEviction: true,
			wantRequeued: true,
		},
		{
			// The workload controller recreated the evicted pod; it is Pending and
			// must not hold the slice.
			name:        "pending replacement pod does not hold the slice",
			pods:        []client.Object{drainPod("w-0", "s", "", corev1.PodPending)},
			wantUnbound: true,
		},
		{
			name: "pending replacement alongside a running pod waits for the running pod",
			pods: []client.Object{
				drainPod("w-0", "s", "", corev1.PodPending),
				drainPod("w-1", "s", "node-2", corev1.PodRunning),
			},
			wantEviction: true,
			wantRequeued: true,
		},
		{
			name:        "terminal pod still on its node does not hold the slice",
			pods:        []client.Object{drainPod("w-0", "s", "node-1", corev1.PodSucceeded)},
			wantUnbound: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newDrainTestEnv(t, nil, append(tt.pods, drainSlice("s"))...)
			res, err := env.reconcile(t, "s")
			if err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}
			if got := env.sub.lastEviction != nil; got != tt.wantEviction {
				t.Errorf("eviction = %v, want %v", got, tt.wantEviction)
			}
			if got := len(env.partitionIds(t, "s")) == 0; got != tt.wantUnbound {
				t.Errorf("unbound = %v, want %v", got, tt.wantUnbound)
			}
			if got := res.RequeueAfter > 0; got != tt.wantRequeued {
				t.Errorf("requeued = %v, want %v", got, tt.wantRequeued)
			}
		})
	}
}

// TestHandleEviction_ConfirmsDrainAgainstAPIServer covers a cache that still shows
// a pod as Pending after this reconciler bound it.
func TestHandleEviction_ConfirmsDrainAgainstAPIServer(t *testing.T) {
	live := fake.NewClientBuilder().WithScheme(newTestScheme(t)).
		WithObjects(drainPod("w-0", "s", "node-1", corev1.PodRunning)).Build()
	env := newDrainTestEnv(t, live, drainSlice("s"), drainPod("w-0", "s", "", corev1.PodPending))

	res, err := env.reconcile(t, "s")
	if err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if got := env.partitionIds(t, "s"); len(got) != 1 {
		t.Errorf("PartitionIds = %v, want [part-1] while the API server shows a placed pod", got)
	}
	if env.sub.lastEviction == nil || env.sub.lastEviction.Name != "w-0" {
		t.Errorf("eviction = %v, want w-0 evicted", env.sub.lastEviction)
	}
	if res.RequeueAfter == 0 {
		t.Error("Reconcile() did not requeue while a pod is placed")
	}
}

func TestHandleEviction_LiveReadFailureLeavesSliceBound(t *testing.T) {
	live := interceptor.NewClient(fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return fmt.Errorf("injected")
		},
	})
	env := newDrainTestEnv(t, live, drainSlice("s"))

	if _, err := env.reconcile(t, "s"); err == nil {
		t.Error("Reconcile() error = nil, want the live read failure")
	}
	if got := env.partitionIds(t, "s"); len(got) != 1 {
		t.Errorf("PartitionIds = %v, want [part-1] when the drain can't be confirmed", got)
	}
}

func TestReconcile_SkipsTerminalPods(t *testing.T) {
	bound := drainSlice("s")
	delete(bound.Annotations, interfaces.EvictPodsAnnotation)
	unplaced := drainSlice("s")
	delete(unplaced.Annotations, interfaces.EvictPodsAnnotation)
	unplaced.Spec.PartitionIds = nil

	tests := []struct {
		name  string
		slice *slicev1beta1.Slice
		pods  []client.Object
	}{
		{
			name:  "failed pod off its target node is not evicted",
			slice: bound,
			pods:  []client.Object{drainPod("w-0", "s", "node-2", corev1.PodFailed)},
		},
		{
			name:  "failed unbound pod is not bound",
			slice: bound,
			pods:  []client.Object{drainPod("w-1", "s", "", corev1.PodFailed)},
		},
		{
			name:  "succeeded pod of an unplaced slice is not evicted",
			slice: unplaced,
			pods:  []client.Object{drainPod("w-0", "s", "node-1", corev1.PodSucceeded)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newDrainTestEnv(t, nil, append(tt.pods, tt.slice.DeepCopy())...)
			if _, err := env.reconcile(t, "s"); err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}
			if env.sub.lastEviction != nil {
				t.Errorf("evicted %s, want no eviction", env.sub.lastEviction.Name)
			}
			if env.sub.lastBinding != nil {
				t.Errorf("bound %s, want no binding", env.sub.lastBinding.Name)
			}
		})
	}
}

// events returns the events recorded so far.
func (e *drainTestEnv) events() []string {
	var out []string
	recorder := e.r.Recorder.(*events.FakeRecorder)
	for {
		select {
		case ev := <-recorder.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func (e *drainTestEnv) slice(t *testing.T, name string) *slicev1beta1.Slice {
	t.Helper()
	slice := &slicev1beta1.Slice{}
	if err := e.client.Get(context.Background(), types.NamespacedName{Name: name}, slice); err != nil {
		t.Fatalf("failed to get slice %s: %v", name, err)
	}
	return slice
}

// backdateDrain moves the slice's recorded drain start to before the deadline.
func (e *drainTestEnv) backdateDrain(t *testing.T, name string) {
	t.Helper()
	slice := e.slice(t, name)
	if _, ok := slice.Annotations[DrainStartedAtAnnotation]; !ok {
		t.Fatalf("slice %s has no %s", name, DrainStartedAtAnnotation)
	}
	slice.Annotations[DrainStartedAtAnnotation] = time.Now().Add(-2 * e.r.DrainTimeout).UTC().Format(time.RFC3339)
	if err := e.client.Update(context.Background(), slice); err != nil {
		t.Fatalf("failed to backdate drain: %v", err)
	}
}

func (e *drainTestEnv) podExists(t *testing.T, name string) bool {
	t.Helper()
	err := e.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &corev1.Pod{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("failed to get pod %s: %v", name, err)
	}
	return err == nil
}

func hasEvent(events []string, reason string) bool {
	for _, ev := range events {
		if strings.Contains(ev, " "+reason+" ") {
			return true
		}
	}
	return false
}

func pdbRefusal() error {
	return apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 0)
}

// TestDrain_ForceDeletesAfterDeadline drives both drain paths through a pod a
// PodDisruptionBudget won't let go: the drain keeps requeueing without an
// error until the deadline, then force-deletes the pod.
func TestDrain_ForceDeletesAfterDeadline(t *testing.T) {
	unplaced := placedSlice("s")
	unplaced.Spec.PartitionIds = nil

	tests := []struct {
		name  string
		slice *slicev1beta1.Slice
		// wantUnbound is whether the slice ends with no partitions and no evict
		// annotation once the pod is gone.
		wantUnbound bool
	}{
		{name: "evicted slice", slice: drainSlice("s"), wantUnbound: true},
		{name: "slice with no partitions", slice: unplaced},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newDrainTestEnv(t, nil, tt.slice, drainPod("w-0", "s", "node-1", corev1.PodRunning))
			env.r.DrainTimeout = 5 * time.Minute
			env.sub.failFor = map[string]error{"w-0": pdbRefusal()}

			// Refused eviction: no error, a requeue, and the drain start recorded.
			res, err := env.reconcile(t, "s")
			if err != nil {
				t.Fatalf("Reconcile() error = %v, want the refusal absorbed", err)
			}
			if res.RequeueAfter == 0 {
				t.Error("Reconcile() did not requeue while the pod is placed")
			}
			if !hasEvent(env.events(), "EvictionFailed") {
				t.Error("no EvictionFailed event for the refused eviction")
			}
			if !env.podExists(t, "w-0") {
				t.Fatal("pod deleted before the deadline")
			}

			// A second pass before the deadline still only tries to evict.
			if _, err := env.reconcile(t, "s"); err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}
			if !env.podExists(t, "w-0") {
				t.Fatal("pod deleted before the deadline")
			}

			// Past the deadline the pod is force-deleted.
			env.backdateDrain(t, "s")
			if _, err := env.reconcile(t, "s"); err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}
			if env.podExists(t, "w-0") {
				t.Fatal("pod not force-deleted after the deadline")
			}
			if !hasEvent(env.events(), "ForceDeleted") {
				t.Error("no ForceDeleted event")
			}

			// Drained: the drain start is cleared, and an evicted slice is unbound.
			if _, err := env.reconcile(t, "s"); err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}
			got := env.slice(t, "s")
			if _, ok := got.Annotations[DrainStartedAtAnnotation]; ok {
				t.Errorf("%s left on the drained slice", DrainStartedAtAnnotation)
			}
			if tt.wantUnbound {
				if len(got.Spec.PartitionIds) != 0 {
					t.Errorf("PartitionIds = %v, want empty", got.Spec.PartitionIds)
				}
				if _, ok := got.Annotations[interfaces.EvictPodsAnnotation]; ok {
					t.Errorf("%s left on the unbound slice", interfaces.EvictPodsAnnotation)
				}
			}
		})
	}
}

func TestDrain_ZeroTimeoutNeverForceDeletes(t *testing.T) {
	slice := drainSlice("s")
	slice.Annotations[DrainStartedAtAnnotation] = time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	env := newDrainTestEnv(t, nil, slice, drainPod("w-0", "s", "node-1", corev1.PodRunning))
	env.sub.failFor = map[string]error{"w-0": pdbRefusal()}

	if _, err := env.reconcile(t, "s"); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if !env.podExists(t, "w-0") {
		t.Error("pod force-deleted with DrainTimeout 0")
	}
}

// TestDrain_NoPartitionsContinuesPastFailures checks that a refused eviction
// doesn't stop the other pods of a slice with no partitions from being evicted,
// and that the reconciler comes back while pods remain.
func TestDrain_NoPartitionsContinuesPastFailures(t *testing.T) {
	slice := placedSlice("s")
	slice.Spec.PartitionIds = nil
	env := newDrainTestEnv(t, nil, slice,
		drainPod("w-0", "s", "node-1", corev1.PodRunning),
		drainPod("w-1", "s", "node-2", corev1.PodRunning))
	env.sub.failFor = map[string]error{"w-0": pdbRefusal()}

	res, err := env.reconcile(t, "s")
	if err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if env.sub.lastEviction == nil || env.sub.lastEviction.Name != "w-1" {
		t.Errorf("eviction = %v, want w-1 evicted", env.sub.lastEviction)
	}
	if res.RequeueAfter == 0 {
		t.Error("Reconcile() did not requeue while pods are placed")
	}
}

func TestDrain_StartClearedWhenSliceIsScheduledAgain(t *testing.T) {
	slice := placedSlice("s")
	slice.Annotations[DrainStartedAtAnnotation] = time.Now().UTC().Format(time.RFC3339)
	env := newDrainTestEnv(t, nil, slice, drainPod("w-0", "s", "node-1", corev1.PodRunning))

	if _, err := env.reconcile(t, "s"); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if _, ok := env.slice(t, "s").Annotations[DrainStartedAtAnnotation]; ok {
		t.Errorf("%s left on a slice that is placed and not evicting", DrainStartedAtAnnotation)
	}
}

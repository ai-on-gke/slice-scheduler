/*
Copyright 2026.

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

// Tests for keeping the partition cache in step with the Slices the API shows: rebuilding it
// in Start, and reclaiming partitions for Slices it dropped after a lost conflict.
package scheduler

import (
	"context"
	"testing"
	"time"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
	"github.com/go-logr/logr"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// TestStart_PreemptorKeepsPartitionsOverDrainingVictim checks that a rebuild gives partitions
// held by both a draining victim and its preemptor to the preemptor, as the live path does,
// whichever order the LIST returns them in, and that the victim's later unbind leaves them
// reserved.
func TestStart_PreemptorKeepsPartitionsOverDrainingVictim(t *testing.T) {
	tests := []struct {
		name              string
		victim, preemptor string
	}{
		{name: "victim listed first", victim: "a-victim", preemptor: "b-preemptor"},
		{name: "preemptor listed first", victim: "b-victim", preemptor: "a-preemptor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			victim := markedSlice(tt.victim, "cube-a")
			preemptor := taintTestSlice(tt.preemptor, TPU6e, interfaces.SliceSchedulerName, "cube-a")
			var sliceLists int
			fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).
				WithObjects(noExecuteTree(), victim.DeepCopy(), preemptor.DeepCopy()).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if _, ok := list.(*slicev1beta1.SliceList); ok {
							sliceLists++
						}
						return c.List(ctx, list, opts...)
					},
				}).Build()
			r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, events.NewFakeRecorder(10),
				false, nil, watcher.NewPartitionCache(false), 0, time.Second, policy.NewCloudPolicy(), 0)
			if err != nil {
				t.Fatalf("NewSliceReconciler() error: %v", err)
			}
			if err := r.Start(ctx); err != nil {
				t.Fatalf("Start() error: %v", err)
			}

			holder := func() string {
				node, ok := r.cache.GetNode("cube-a")
				if !ok || node.InUseBy == nil {
					return ""
				}
				return node.InUseBy.Name
			}
			if got := holder(); got != tt.preemptor {
				t.Fatalf("after Start, cube-a held by %q, want %q", got, tt.preemptor)
			}

			// The PodReconciler unbinds the victim once its pods are gone. The victim
			// holds nothing in the cache, so its release frees nothing and must not
			// trigger a reclaim scan.
			sliceLists = 0
			unbound := victim.DeepCopy()
			unbound.Spec.PartitionIds = []string{}
			delete(unbound.Annotations, interfaces.EvictPodsAnnotation)
			r.handleSliceUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: victim, ObjectNew: unbound})

			if got := holder(); got != tt.preemptor {
				t.Errorf("after the victim's unbind, cube-a held by %q, want %q", got, tt.preemptor)
			}
			if sliceLists != 0 {
				t.Errorf("victim's unbind listed Slices %d times, want 0", sliceLists)
			}
		})
	}
}

// TestReclaimFreedPartitions checks that once the slice holding contested partitions releases
// them, another slice the API still shows on that hardware gets them back in the cache.
func TestReclaimFreedPartitions(t *testing.T) {
	bound := func(name string, ids ...string) *slicev1beta1.Slice {
		return taintTestSlice(name, TPU6e, interfaces.SliceSchedulerName, ids...)
	}
	tests := []struct {
		name    string
		slices  []*slicev1beta1.Slice
		release string // slice whose release frees partitions
		delete  bool   // release by deletion rather than unbind
		check   string // partition to inspect afterwards
		want    string // its holder
	}{
		{
			name:    "deleted holder hands the partition to the other claimant",
			slices:  []*slicev1beta1.Slice{bound("a-first", "cube-a"), bound("b-second", "cube-a")},
			release: "a-first", delete: true, check: "cube-a", want: "b-second",
		},
		{
			name:    "unbound parent hands a child partition to its claimant",
			slices:  []*slicev1beta1.Slice{bound("a-cube", "cube-a"), bound("b-half", "cube-a-0")},
			release: "a-cube", check: "cube-a-0", want: "b-half",
		},
		{
			// The freed half overlaps the victim's cube, so it is considered, but the
			// preemptor still holds the other half.
			name: "draining victim stays out while its preemptor holds part of it",
			slices: []*slicev1beta1.Slice{
				bound("a-preemptor", "cube-a-0"), markedSlice("b-victim", "cube-a"), bound("c-other", "cube-a-1"),
			},
			release: "c-other", delete: true, check: "cube-a", want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			objs := []client.Object{noExecuteTree()}
			byName := map[string]*slicev1beta1.Slice{}
			for _, s := range tt.slices {
				objs = append(objs, s.DeepCopy())
				byName[s.Name] = s
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build()
			r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, events.NewFakeRecorder(10),
				false, nil, watcher.NewPartitionCache(false), 0, time.Second, policy.NewCloudPolicy(), 0)
			if err != nil {
				t.Fatalf("NewSliceReconciler() error: %v", err)
			}
			if err := r.Start(ctx); err != nil {
				t.Fatalf("Start() error: %v", err)
			}

			released := byName[tt.release]
			if tt.delete {
				if err := fakeClient.Delete(ctx, released.DeepCopy()); err != nil {
					t.Fatalf("Delete() error: %v", err)
				}
				r.handleSliceDelete(ctx, event.TypedDeleteEvent[client.Object]{Object: released})
			} else {
				unbound := released.DeepCopy()
				unbound.Spec.PartitionIds = []string{}
				r.handleSliceUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: released, ObjectNew: unbound})
			}

			node, ok := r.cache.GetNode(tt.check)
			if !ok {
				t.Fatalf("partition %q not in cache", tt.check)
			}
			got := ""
			if node.InUseBy != nil {
				got = node.InUseBy.Name
			}
			if got != tt.want {
				t.Errorf("%s held by %q, want %q", tt.check, got, tt.want)
			}
		})
	}
}

// TestReclaimFreedPartitions_AfterLostUpdate drives the path from the bug through
// handleSliceUpdate alone: a slice placed 0->N loses the conflict to the current holder and
// is dropped from the cache; once the holder unbinds, the slice gets the partitions back.
// The fake client is updated before each handler call, as the informer would be.
func TestReclaimFreedPartitions_AfterLostUpdate(t *testing.T) {
	tests := []struct {
		name          string
		ownerIDs      []string
		lateIDs       []string
		check         string
		wantAfterLoss string
	}{
		{name: "same partition", ownerIDs: []string{"cube-a"}, lateIDs: []string{"cube-a"}, check: "cube-a", wantAfterLoss: "a-owner"},
		{name: "child of the held cube", ownerIDs: []string{"cube-a"}, lateIDs: []string{"cube-a-0"}, check: "cube-a-0", wantAfterLoss: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			owner := taintTestSlice("a-owner", TPU6e, interfaces.SliceSchedulerName, tt.ownerIDs...)
			late := taintTestSlice("b-late", TPU6e, interfaces.SliceSchedulerName)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).
				WithObjects(noExecuteTree(), owner, late).Build()
			r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, events.NewFakeRecorder(10),
				false, nil, watcher.NewPartitionCache(false), 0, time.Second, policy.NewCloudPolicy(), 0)
			if err != nil {
				t.Fatalf("NewSliceReconciler() error: %v", err)
			}
			if err := r.Start(ctx); err != nil {
				t.Fatalf("Start() error: %v", err)
			}

			// setPartitions writes the new spec to the API, then delivers the update event.
			setPartitions := func(name string, ids ...string) {
				t.Helper()
				old := &slicev1beta1.Slice{}
				if err := fakeClient.Get(ctx, client.ObjectKey{Name: name}, old); err != nil {
					t.Fatalf("Get(%s) error: %v", name, err)
				}
				updated := old.DeepCopy()
				updated.Spec.PartitionIds = ids
				if err := fakeClient.Update(ctx, updated); err != nil {
					t.Fatalf("Update(%s) error: %v", name, err)
				}
				r.handleSliceUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: old, ObjectNew: updated})
			}
			holder := func() string {
				node, ok := r.cache.GetNode(tt.check)
				if !ok {
					t.Fatalf("partition %q not in cache", tt.check)
				}
				if node.InUseBy == nil {
					return ""
				}
				return node.InUseBy.Name
			}

			setPartitions("b-late", tt.lateIDs...)
			if got := holder(); got != tt.wantAfterLoss {
				t.Fatalf("after the lost update, %s held by %q, want %q", tt.check, got, tt.wantAfterLoss)
			}
			placed := late.DeepCopy()
			placed.Spec.PartitionIds = tt.lateIDs
			if r.cache.Holds(sliceInfo(ctx, placed)) {
				t.Fatalf("b-late reserved despite the conflict")
			}

			setPartitions("a-owner")
			if got := holder(); got != "b-late" {
				t.Errorf("after the owner unbinds, %s held by %q, want %q", tt.check, got, "b-late")
			}
		})
	}
}

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
	"time"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// gainedOrShortenedPartitions returns the partition IDs in now that were absent in was
// or whose eviction deadline moved earlier, or nil if there are none.
func gainedOrShortenedPartitions(was, now map[string]time.Time) map[string]bool {
	var changed map[string]bool
	for id, nowDue := range now {
		wasDue, ok := was[id]
		if !ok || nowDue.Before(wasDue) {
			if changed == nil {
				changed = make(map[string]bool)
			}
			changed[id] = true
		}
	}
	return changed
}

// requeueSlicesOnNoExecutePartitions sends every slice we own that holds an evictable
// NoExecute-tainted partition of the named tree through Reconcile. A bound slice is not in
// the scheduling queue and a taint changes the PartitionTree rather than the Slice, so
// nothing else would pick it up. Reconcile either defers via RequeueAfter while a positive
// toleration window remains or queues the group after rescheduleUnhealthyDelay once due,
// and the scheduling loop then marks the slice for eviction. onlyPartitions restricts the
// sweep to slices holding one of those partitions; nil sweeps the whole tree.
func (r *SliceReconciler) requeueSlicesOnNoExecutePartitions(treeName string, q workqueue.TypedRateLimitingInterface[reconcile.Request], onlyPartitions map[string]bool) {
	if r.cache == nil || q == nil {
		return
	}
	occupants := r.cache.SlicesOnNoExecutePartitions(treeName)
	if len(occupants) == 0 {
		return
	}
	logf := r.log.WithValues("partitionTree", treeName)
	queued := make(map[string]bool, len(occupants))
	for _, sInfo := range occupants {
		if !r.ownsReservation(sInfo) || queued[sInfo.Name] {
			continue
		}
		if onlyPartitions != nil && !holdsAnyPartition(sInfo, onlyPartitions) {
			continue
		}
		queued[sInfo.Name] = true
		// Slices are cluster-scoped, so the name alone identifies the object.
		q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Name: sInfo.Name}})
		logf.Info("Requeued slice holding a NoExecute partition", "sliceName", sInfo.Name)
	}
}

// holdsAnyPartition reports whether the reservation covers one of the given partitions.
func holdsAnyPartition(sInfo *interfaces.SliceInfo, ids map[string]bool) bool {
	for _, pID := range sInfo.PartitionIds {
		if ids[pID] {
			return true
		}
	}
	return false
}

// ownsSlice and ownsReservation report whether this scheduler manages a Slice or a cached
// reservation. shouldReconcileSlice uses ownsSlice in the For() predicate; the NoExecute
// sweeps enqueue by name, which bypasses that predicate, so they apply ownsReservation
// themselves. The cache also holds reservations belonging to other schedulers and TPU types.
func (r *SliceReconciler) ownsSlice(slice *slicev1beta1.Slice) bool {
	return interfaces.OwnsSlice(slice.Annotations[ManagedByAnnotation], string(slice.Spec.Type), r.tpuType)
}

func (r *SliceReconciler) ownsReservation(sInfo *interfaces.SliceInfo) bool {
	return sInfo != nil && interfaces.OwnsSlice(sInfo.ManagedBy, sInfo.Type, r.tpuType)
}

// shouldReconcileSlice reports whether the For(&Slice{}) predicate should admit slice:
// either it is ours and needs scheduling, or it is an eviction candidate currently
// waiting inside a positive NoExecute toleration window (so Reconcile arms RequeueAfter,
// including when a slice's bind finishes after its partition's window already started).
func (r *SliceReconciler) shouldReconcileSlice(slice *slicev1beta1.Slice) bool {
	if !r.ownsSlice(slice) || !slice.DeletionTimestamp.IsZero() {
		return false
	}
	if r.needsScheduling(slice) {
		return true
	}
	_, waiting := r.noExecuteTolerationRemaining(slice)
	return waiting && r.isEvictionCandidate(slice)
}

// isSliceOnNoExecutePartition reports whether the slice holds a partition that has, or sits
// above, a NoExecute taint whose eviction deadline has expired. A taint on a partition above
// it doesn't count: an interior partition-state taint says the larger partition can't form a
// working group, not that every partition inside it is bad. A slice already marked for
// eviction doesn't count: the pod reconciler is moving it, and counting it would send every
// Slice update during the drain back through the scheduling queue. Always false unless
// --enable-taint-filtering is set.
func (r *SliceReconciler) isSliceOnNoExecutePartition(slice *slicev1beta1.Slice) bool {
	tainted, wait := r.sliceNoExecuteWait(slice)
	return tainted && wait == 0
}

// noExecuteTolerationRemaining reports the remaining time before the earliest NoExecute
// toleration deadline on slice expires, and true when slice is currently within a positive
// toleration window (not yet due for eviction).
func (r *SliceReconciler) noExecuteTolerationRemaining(slice *slicev1beta1.Slice) (time.Duration, bool) {
	tainted, wait := r.sliceNoExecuteWait(slice)
	return wait, tainted && wait > 0
}

func (r *SliceReconciler) sliceNoExecuteWait(slice *slicev1beta1.Slice) (bool, time.Duration) {
	if r.cache == nil || slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr {
		return false, 0
	}
	tainted := false
	var minWait time.Duration
	for _, pID := range slice.Spec.PartitionIds {
		pTainted, wait := r.cache.NoExecuteWait(pID)
		if !pTainted {
			continue
		}
		if wait == 0 {
			return true, 0
		}
		if !tainted || wait < minWait {
			minWait = wait
			tainted = true
		}
	}
	return tainted, minWait
}

// anyMemberAwaitingNoExecuteEviction reports whether a member is still bound to a NoExecute
// partition whose toleration has expired and is not yet marked for eviction.
func (r *SliceReconciler) anyMemberAwaitingNoExecuteEviction(sliceNames []string,
	groupSlices map[string]*slicev1beta1.Slice) bool {
	for _, name := range sliceNames {
		slice, ok := groupSlices[name]
		if ok && r.isEvictionCandidate(slice) && r.isSliceOnNoExecutePartition(slice) {
			return true
		}
	}
	return false
}

// minNoExecuteTolerationRemaining returns the shortest positive remaining NoExecute
// toleration window across eviction-candidate members of a group.
func (r *SliceReconciler) minNoExecuteTolerationRemaining(sliceNames []string,
	groupSlices map[string]*slicev1beta1.Slice) (time.Duration, bool) {
	var minWait time.Duration
	found := false
	for _, name := range sliceNames {
		slice, ok := groupSlices[name]
		if !ok || !r.isEvictionCandidate(slice) {
			continue
		}
		if wait, waiting := r.noExecuteTolerationRemaining(slice); waiting {
			if !found || wait < minWait {
				minWait = wait
				found = true
			}
		}
	}
	return minWait, found
}

// noExecuteStartupDelay reports whether a slice listed during Start needs a delayed
// scheduling-queue wakeup for NoExecute handling: either rescheduleUnhealthyDelay when its
// deadline has already expired, or the remaining toleration window from PartitionTaint.TimeAdded.
func (r *SliceReconciler) noExecuteStartupDelay(slice *slicev1beta1.Slice) (time.Duration, bool) {
	if !r.isEvictionCandidate(slice) {
		return 0, false
	}
	if r.isSliceOnNoExecutePartition(slice) {
		return r.rescheduleUnhealthyDelay, true
	}
	if wait, waiting := r.noExecuteTolerationRemaining(slice); waiting {
		return wait, true
	}
	return 0, false
}

// skippedByPolicy reports whether the SlicePolicy skips reconciling this slice.
// CloudPolicy only skips unbound slices during N->0 deactivation, so this is false
// for bound slices under CloudPolicy, but keeps isEvictionCandidate policy-agnostic.
func (r *SliceReconciler) skippedByPolicy(slice *slicev1beta1.Slice) bool {
	skip, _, _ := r.policy.ShouldSkipReconcile(slice)
	return skip
}

// isEvictionCandidate reports whether the scheduler may mark the slice for eviction at all:
// it holds partitions, is ours, isn't being deleted or left alone by the policy, and isn't
// marked already.
func (r *SliceReconciler) isEvictionCandidate(slice *slicev1beta1.Slice) bool {
	return len(slice.Spec.PartitionIds) > 0 && slice.DeletionTimestamp.IsZero() && r.ownsSlice(slice) &&
		!r.skippedByPolicy(slice) && slice.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr
}

// evictMembersOnNoExecutePartitions marks every bound member of the group that holds a
// NoExecute partition whose toleration deadline has expired for eviction, and reports
// whether it marked any. A member the policy skips is left for prepareGroupAllocationRequest,
// which requeues the group.
func (r *SliceReconciler) evictMembersOnNoExecutePartitions(ctx context.Context, logf logr.Logger,
	sliceNames []string, groupSlices map[string]*slicev1beta1.Slice) (bool, error) {
	marked := false
	for _, name := range sliceNames {
		slice, ok := groupSlices[name]
		if !ok || !r.isEvictionCandidate(slice) || !r.isSliceOnNoExecutePartition(slice) {
			continue
		}
		didMark, err := r.markSliceForEviction(ctx, logf, name, r.isSliceOnNoExecutePartition,
			r.recordNoExecuteEvictionEvent)
		if err != nil {
			return marked, err
		}
		marked = marked || didMark
	}
	return marked, nil
}

// markSliceForEviction sets the evict-pods annotation, so the pod reconciler evicts the
// slice's pods and then clears its partitions, records the event with record, and reports
// whether it did. It re-checks stillApplies against the fetched slice, since the cached
// copy the caller looked at may be stale.
func (r *SliceReconciler) markSliceForEviction(ctx context.Context, logf logr.Logger, sliceName string,
	stillApplies func(*slicev1beta1.Slice) bool, record func(*slicev1beta1.Slice)) (bool, error) {
	for attempt := 0; attempt < maxBindRetries; attempt++ {
		slice := &slicev1beta1.Slice{}
		if err := r.Get(ctx, types.NamespacedName{Name: sliceName}, slice); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if !r.isEvictionCandidate(slice) || !stillApplies(slice) {
			return false, nil
		}
		if slice.Annotations == nil {
			slice.Annotations = make(map[string]string)
		}
		slice.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
		if err := r.Update(ctx, slice); err != nil {
			if errors.IsConflict(err) {
				time.Sleep(10 * time.Millisecond * time.Duration(attempt+1))
				continue
			}
			return false, fmt.Errorf("failed to annotate slice %s for eviction: %w", sliceName, err)
		}
		logf.Info("Marked slice for eviction", "sliceName", sliceName, "partitionIds", slice.Spec.PartitionIds)
		record(slice)
		return true, nil
	}
	return false, fmt.Errorf("failed to annotate slice %s for eviction after %d retries due to conflicts", sliceName, maxBindRetries)
}

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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

// ---------- partitionsMatch tests ----------

func TestPartitionsMatch(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want bool
	}{
		{
			name: "both empty",
			a:    []string{},
			b:    []string{},
			want: true,
		},
		{
			name: "both nil",
			a:    nil,
			b:    nil,
			want: true,
		},
		{
			name: "identical sets",
			a:    []string{"p1", "p2", "p3"},
			b:    []string{"p1", "p2", "p3"},
			want: true,
		},
		{
			name: "same elements different order",
			a:    []string{"p3", "p1", "p2"},
			b:    []string{"p1", "p2", "p3"},
			want: true,
		},
		{
			name: "different lengths",
			a:    []string{"p1", "p2"},
			b:    []string{"p1", "p2", "p3"},
			want: false,
		},
		{
			name: "same length different content",
			a:    []string{"p1", "p2"},
			b:    []string{"p1", "p3"},
			want: false,
		},
		{
			name: "duplicate partition IDs in b - false positive guard",
			a:    []string{"p1", "p2"},
			b:    []string{"p1", "p1"},
			want: false,
		},
		{
			name: "duplicate partition IDs in a - false positive guard",
			a:    []string{"p1", "p1"},
			b:    []string{"p1", "p2"},
			want: false,
		},
		{
			name: "duplicates in both - matching",
			a:    []string{"p1", "p1", "p2"},
			b:    []string{"p1", "p2", "p1"},
			want: true,
		},
		{
			name: "duplicates in both - not matching",
			a:    []string{"p1", "p1", "p2"},
			b:    []string{"p1", "p2", "p2"},
			want: false,
		},
		{
			name: "single element match",
			a:    []string{"p1"},
			b:    []string{"p1"},
			want: true,
		},
		{
			name: "single element mismatch",
			a:    []string{"p1"},
			b:    []string{"p2"},
			want: false,
		},
		{
			name: "empty vs non-empty",
			a:    []string{},
			b:    []string{"p1"},
			want: false,
		},
		{
			name: "nil vs non-empty",
			a:    nil,
			b:    []string{"p1"},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := partitionsMatch(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("partitionsMatch(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// ---------- helpers ----------

// newTestReconciler creates a SliceReconciler with a fake client for testing.
// Returns the reconciler and the underlying fake client (WithWatch).
func newTestReconciler(t *testing.T, s *runtime.Scheme, objs ...client.Object) (*SliceReconciler, client.WithWatch) {
	t.Helper()
	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	recorder := events.NewFakeRecorder(100)
	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e", recorder,
		false, nil, watcher.NewPartitionCache(false),
		0, 0, policy.NewCloudPolicy(), 0,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler failed: %v", err)
	}
	return reconciler, fakeClient
}

// newTestReconcilerWithRecorder creates a SliceReconciler and returns
// both the underlying fake client and the FakeRecorder so tests can
// inspect emitted events.
func newTestReconcilerWithRecorder(t *testing.T, s *runtime.Scheme, objs ...client.Object) (*SliceReconciler, client.WithWatch, *events.FakeRecorder) {
	t.Helper()
	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	recorder := events.NewFakeRecorder(100)
	reconciler, err := NewSliceReconciler(
		fakeClient, s, ctrl.Log.WithName("test"), "v6e", recorder,
		false, nil, watcher.NewPartitionCache(false),
		0, 0, policy.NewCloudPolicy(), 0,
	)
	if err != nil {
		t.Fatalf("NewSliceReconciler failed: %v", err)
	}
	return reconciler, fakeClient, recorder
}

// conflictOnFirstUpdate wraps a fake client to return a conflict error on the
// first N Update calls, then delegate to the real client.
func conflictOnFirstUpdate(inner client.WithWatch, conflictCount int) client.Client {
	updateCount := 0
	return interceptor.NewClient(inner, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updateCount++
			if updateCount <= conflictCount {
				return fmt.Errorf("the object has been modified; please apply your changes to the latest version then try again: %w",
					&conflictError{})
			}
			return c.Update(ctx, obj, opts...)
		},
	})
}

// nonConflictUpdateClient wraps a fake client to return a generic (non-conflict)
// error on every Update call.
func nonConflictUpdateClient(inner client.WithWatch) client.Client {
	return interceptor.NewClient(inner, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			return fmt.Errorf("network error")
		},
	})
}

// failGetAfterFirstCall wraps a fake client so that:
//   - The first Get call succeeds (initial fetch).
//   - The first Update call returns a conflict error (triggers retry).
//   - The second Get call returns a generic network error.
func failGetAfterFirstCall(inner client.WithWatch) client.Client {
	getCount := 0
	updateCount := 0
	return interceptor.NewClient(inner, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			getCount++
			if getCount > 1 {
				return fmt.Errorf("network error on Get")
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updateCount++
			if updateCount <= 1 {
				return fmt.Errorf("the object has been modified; please apply your changes to the latest version then try again: %w",
					&conflictError{})
			}
			return c.Update(ctx, obj, opts...)
		},
	})
}

// conflictError implements errors.IsConflict detection.
type conflictError struct{}

func (e *conflictError) Error() string { return "Conflict" }
func (e *conflictError) Status() metav1.Status {
	return metav1.Status{
		Reason: metav1.StatusReasonConflict,
		Code:   409,
	}
}

func makeSlice(name string, partitions []string) *slicev1beta1.Slice {
	return &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				ManagedByAnnotation: interfaces.SliceSchedulerName,
			},
		},
		Spec: slicev1beta1.SliceSpec{
			Type:         "v6e",
			Topology:     "4x4",
			PartitionIds: partitions,
		},
	}
}

// ---------- clearVictimWithRetry tests ----------

func TestClearVictimWithRetry(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()
	logf := ctrl.Log.WithName("test")

	t.Run("success on first try", func(t *testing.T) {
		victim := makeSlice("victim-1", []string{"p1", "p2"})
		r, fc := newTestReconciler(t, s, victim)

		err := r.clearVictimWithRetry(ctx, logf, "victim-1", "preemptor-group")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify eviction annotation set (partitions preserved for pod reconciler)
		updated := &slicev1beta1.Slice{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "victim-1"}, updated); err != nil {
			t.Fatalf("failed to get slice: %v", err)
		}
		if updated.Annotations == nil || updated.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
			t.Errorf("expected eviction annotation, got %v", updated.Annotations)
		}
		if len(updated.Spec.PartitionIds) != 2 {
			t.Errorf("expected partitions preserved, got %v", updated.Spec.PartitionIds)
		}
	})

	t.Run("conflict triggers retry and succeeds", func(t *testing.T) {
		victim := makeSlice("victim-2", []string{"p1"})
		r, fc := newTestReconciler(t, s, victim)
		// Wrap client to fail on first update
		r.Client = conflictOnFirstUpdate(fc, 1)

		err := r.clearVictimWithRetry(ctx, logf, "victim-2", "preemptor-group")
		if err != nil {
			t.Fatalf("expected success after retry, got: %v", err)
		}
	})

	t.Run("all retries exhausted", func(t *testing.T) {
		victim := makeSlice("victim-3", []string{"p1"})
		r, fc := newTestReconciler(t, s, victim)
		// Fail on all retries
		r.Client = conflictOnFirstUpdate(fc, maxBindRetries+1)

		err := r.clearVictimWithRetry(ctx, logf, "victim-3", "preemptor-group")
		if err == nil {
			t.Fatal("expected error when all retries exhausted")
		}
	})

	t.Run("non-conflict error fails immediately", func(t *testing.T) {
		victim := makeSlice("victim-4", []string{"p1"})
		r, fc := newTestReconciler(t, s, victim)
		r.Client = nonConflictUpdateClient(fc)

		err := r.clearVictimWithRetry(ctx, logf, "victim-4", "preemptor-group")
		if err == nil {
			t.Fatal("expected error on non-conflict failure")
		}
	})

	t.Run("slice not found - already cleared", func(t *testing.T) {
		// Don't create the slice, so Get returns NotFound
		r, _ := newTestReconciler(t, s)

		err := r.clearVictimWithRetry(ctx, logf, "nonexistent", "preemptor-group")
		if err != nil {
			t.Fatalf("expected nil for NotFound, got: %v", err)
		}
	})

	t.Run("partitions already empty - idempotent", func(t *testing.T) {
		victim := makeSlice("victim-5", []string{})
		r, _ := newTestReconciler(t, s, victim)

		err := r.clearVictimWithRetry(ctx, logf, "victim-5", "preemptor-group")
		if err != nil {
			t.Fatalf("expected nil for already-cleared, got: %v", err)
		}
	})

	t.Run("multiple conflicts before success", func(t *testing.T) {
		victim := makeSlice("victim-mc", []string{"p1", "p2"})
		r, fc := newTestReconciler(t, s, victim)
		// Fail on first 2 updates, succeed on the 3rd
		r.Client = conflictOnFirstUpdate(fc, 2)

		err := r.clearVictimWithRetry(ctx, logf, "victim-mc", "preemptor-group")
		if err != nil {
			t.Fatalf("expected success after 2 conflicts, got: %v", err)
		}

		// Verify eviction annotation set (partitions preserved)
		updated := &slicev1beta1.Slice{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "victim-mc"}, updated); err != nil {
			t.Fatalf("failed to get slice: %v", err)
		}
		if updated.Annotations == nil || updated.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
			t.Errorf("expected eviction annotation, got %v", updated.Annotations)
		}
		if len(updated.Spec.PartitionIds) != 2 {
			t.Errorf("expected partitions preserved, got %v", updated.Spec.PartitionIds)
		}
	})

	t.Run("Get fails on re-fetch during retry", func(t *testing.T) {
		victim := makeSlice("victim-gf", []string{"p1"})
		r, fc := newTestReconciler(t, s, victim)
		// First Get succeeds, first Update returns conflict, second Get returns error
		r.Client = failGetAfterFirstCall(fc)

		err := r.clearVictimWithRetry(ctx, logf, "victim-gf", "preemptor-group")
		if err == nil {
			t.Fatal("expected error when Get fails on re-fetch")
		}
		// Verify the error is from the Get, not from the Update
		if !strings.Contains(err.Error(), "network error on Get") {
			t.Errorf("expected Get error, got: %v", err)
		}
	})
}

// ---------- assignPartitionsWithRetry tests ----------

func TestAssignPartitionsWithRetry(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()
	logf := ctrl.Log.WithName("test")

	t.Run("success on first try", func(t *testing.T) {
		slice := makeSlice("slice-1", nil)
		r, fc := newTestReconciler(t, s, slice)

		err := r.assignPartitionsWithRetry(ctx, logf, "slice-1", []string{"p1", "p2"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &slicev1beta1.Slice{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "slice-1"}, updated); err != nil {
			t.Fatalf("failed to get slice: %v", err)
		}
		if !partitionsMatch(updated.Spec.PartitionIds, []string{"p1", "p2"}) {
			t.Errorf("expected partitions [p1, p2], got %v", updated.Spec.PartitionIds)
		}
	})

	t.Run("conflict retry succeeds", func(t *testing.T) {
		slice := makeSlice("slice-2", nil)
		r, fc := newTestReconciler(t, s, slice)
		r.Client = conflictOnFirstUpdate(fc, 1)

		err := r.assignPartitionsWithRetry(ctx, logf, "slice-2", []string{"p1"})
		if err != nil {
			t.Fatalf("expected success after retry, got: %v", err)
		}
	})

	t.Run("idempotency guard - partitions already match", func(t *testing.T) {
		slice := makeSlice("slice-3", []string{"p1", "p2"})
		r, _ := newTestReconciler(t, s, slice)

		err := r.assignPartitionsWithRetry(ctx, logf, "slice-3", []string{"p1", "p2"})
		if err != nil {
			t.Fatalf("expected success for idempotent case, got: %v", err)
		}
	})

	t.Run("N to M rejection - existing partitions differ from requested", func(t *testing.T) {
		slice := makeSlice("slice-4", []string{"p1", "p2"})
		r, _ := newTestReconciler(t, s, slice)

		err := r.assignPartitionsWithRetry(ctx, logf, "slice-4", []string{"p3", "p4"})
		if err == nil {
			t.Fatal("expected error for N→M transition")
		}
	})

	t.Run("all retries exhausted", func(t *testing.T) {
		slice := makeSlice("slice-5", nil)
		r, fc := newTestReconciler(t, s, slice)
		r.Client = conflictOnFirstUpdate(fc, maxBindRetries+1)

		err := r.assignPartitionsWithRetry(ctx, logf, "slice-5", []string{"p1"})
		if err == nil {
			t.Fatal("expected error when all retries exhausted")
		}
	})

	t.Run("non-conflict error fails immediately", func(t *testing.T) {
		slice := makeSlice("slice-nc", nil)
		r, fc := newTestReconciler(t, s, slice)
		r.Client = nonConflictUpdateClient(fc)

		err := r.assignPartitionsWithRetry(ctx, logf, "slice-nc", []string{"p1"})
		if err == nil {
			t.Fatal("expected error on non-conflict failure")
		}
	})
}

// ---------- evictSliceWithRetry tests ----------

func TestEvictSliceWithRetry(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()
	logf := logr.Discard()

	t.Run("success on first try", func(t *testing.T) {
		slice := makeSlice("evict-1", []string{"p1"})
		r, fc := newTestReconciler(t, s, slice)

		err := r.evictSliceWithRetry(ctx, logf, "evict-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &slicev1beta1.Slice{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "evict-1"}, updated); err != nil {
			t.Fatalf("failed to get slice: %v", err)
		}
		if len(updated.Spec.PartitionIds) != 0 {
			t.Errorf("expected empty partitions, got %v", updated.Spec.PartitionIds)
		}
	})

	t.Run("conflict retry succeeds", func(t *testing.T) {
		slice := makeSlice("evict-2", []string{"p1"})
		r, fc := newTestReconciler(t, s, slice)
		r.Client = conflictOnFirstUpdate(fc, 1)

		err := r.evictSliceWithRetry(ctx, logf, "evict-2")
		if err != nil {
			t.Fatalf("expected success after retry, got: %v", err)
		}
	})

	t.Run("already cleared - noop", func(t *testing.T) {
		slice := makeSlice("evict-3", []string{})
		r, _ := newTestReconciler(t, s, slice)

		err := r.evictSliceWithRetry(ctx, logf, "evict-3")
		if err != nil {
			t.Fatalf("expected nil for already-cleared, got: %v", err)
		}
	})

	t.Run("not found - noop", func(t *testing.T) {
		r, _ := newTestReconciler(t, s)

		err := r.evictSliceWithRetry(ctx, logf, "nonexistent")
		if err != nil {
			t.Fatalf("expected nil for NotFound, got: %v", err)
		}
	})

	t.Run("all retries exhausted", func(t *testing.T) {
		slice := makeSlice("evict-4", []string{"p1"})
		r, fc := newTestReconciler(t, s, slice)
		r.Client = conflictOnFirstUpdate(fc, maxBindRetries+1)

		err := r.evictSliceWithRetry(ctx, logf, "evict-4")
		if err == nil {
			t.Fatal("expected error when all retries exhausted")
		}
	})

	t.Run("non-conflict error fails immediately", func(t *testing.T) {
		slice := makeSlice("evict-nc", []string{"p1"})
		r, fc := newTestReconciler(t, s, slice)
		r.Client = nonConflictUpdateClient(fc)

		err := r.evictSliceWithRetry(ctx, logf, "evict-nc")
		if err == nil {
			t.Fatal("expected error on non-conflict failure")
		}
	})
}

// ---------- bindPartitionsToGroup tests ----------

func TestBindPartitionsToGroup(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()

	t.Run("end-to-end: victim clear then assign", func(t *testing.T) {
		victim := makeSlice("victim-e2e", []string{"p1", "p2"})
		group := makeSlice("group-e2e", nil)
		r, fc := newTestReconciler(t, s, victim, group)

		victims := []*interfaces.SliceInfo{
			{Name: "victim-e2e"},
		}
		slicesToReserve := []*interfaces.SliceInfo{
			{Name: "group-e2e", PartitionIds: []string{"p3", "p4"}},
		}
		groupSlices := map[string]*slicev1beta1.Slice{
			"group-e2e": group,
		}

		err := r.bindPartitionsToGroup(ctx, "test-group", groupSlices, slicesToReserve, victims)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify victim annotated for eviction (partitions preserved)
		updatedVictim := &slicev1beta1.Slice{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "victim-e2e"}, updatedVictim); err != nil {
			t.Fatalf("failed to get victim: %v", err)
		}
		if updatedVictim.Annotations == nil || updatedVictim.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
			t.Errorf("expected eviction annotation on victim, got %v", updatedVictim.Annotations)
		}

		// Verify group partitions assigned
		updatedGroup := &slicev1beta1.Slice{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "group-e2e"}, updatedGroup); err != nil {
			t.Fatalf("failed to get group: %v", err)
		}
		if !partitionsMatch(updatedGroup.Spec.PartitionIds, []string{"p3", "p4"}) {
			t.Errorf("expected group partitions [p3, p4], got %v", updatedGroup.Spec.PartitionIds)
		}
	})

	t.Run("victim clear fails stops before assign", func(t *testing.T) {
		victim := makeSlice("victim-fail", []string{"p1", "p2"})
		group := makeSlice("group-fail", nil)
		r, fc := newTestReconciler(t, s, victim, group)
		// Make Update fail with non-conflict error
		r.Client = nonConflictUpdateClient(fc)

		victims := []*interfaces.SliceInfo{
			{Name: "victim-fail"},
		}
		slicesToReserve := []*interfaces.SliceInfo{
			{Name: "group-fail", PartitionIds: []string{"p3", "p4"}},
		}
		groupSlices := map[string]*slicev1beta1.Slice{
			"group-fail": group,
		}

		err := r.bindPartitionsToGroup(ctx, "test-group", groupSlices, slicesToReserve, victims)
		if err == nil {
			t.Fatal("expected error when victim clear fails")
		}

		// Verify group was NOT modified (partitions still empty)
		updatedGroup := &slicev1beta1.Slice{}
		if err := fc.Get(ctx, types.NamespacedName{Name: "group-fail"}, updatedGroup); err != nil {
			t.Fatalf("failed to get group: %v", err)
		}
		if len(updatedGroup.Spec.PartitionIds) != 0 {
			t.Errorf("expected group partitions unchanged (empty), got %v", updatedGroup.Spec.PartitionIds)
		}
	})
}

// timeoutOnBind wraps a fake client so that the first Update assigning partitions
// returns a timeout. When commit is true the Update is applied before the timeout is
// returned, as when the API server commits a write whose response is lost.
func timeoutOnBind(inner client.WithWatch, commit bool) client.Client {
	timedOut := false
	return interceptor.NewClient(inner, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			sl, ok := obj.(*slicev1beta1.Slice)
			if !ok || timedOut || len(sl.Spec.PartitionIds) == 0 {
				return c.Update(ctx, obj, opts...)
			}
			timedOut = true
			if commit {
				if err := c.Update(ctx, obj, opts...); err != nil {
					return err
				}
			}
			return apierrors.NewTimeoutError("request did not complete within requested timeout", 0)
		},
	})
}

// ---------- ambiguous bind error tests ----------

func TestAssignPartitionsWithRetry_AmbiguousError(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()
	logf := ctrl.Log.WithName("test")

	t.Run("committed write is kept", func(t *testing.T) {
		r, fc := newTestReconciler(t, s, makeSlice("slice-to-c", nil))
		r.Client = timeoutOnBind(fc, true)

		if err := r.assignPartitionsWithRetry(ctx, logf, "slice-to-c", []string{"p1"}); err != nil {
			t.Fatalf("expected a committed assignment to succeed, got: %v", err)
		}
	})

	t.Run("uncommitted write fails", func(t *testing.T) {
		r, fc := newTestReconciler(t, s, makeSlice("slice-to-u", nil))
		r.Client = timeoutOnBind(fc, false)

		if err := r.assignPartitionsWithRetry(ctx, logf, "slice-to-u", []string{"p1"}); err == nil {
			t.Fatal("expected an error for an assignment that was not committed")
		}
	})

	t.Run("assignment that cannot be confirmed fails", func(t *testing.T) {
		r, fc := newTestReconciler(t, s, makeSlice("slice-to-x", nil))
		r.Client = timeoutOnBind(fc, true)
		r.apiReader = interceptor.NewClient(fc, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return apierrors.NewServiceUnavailable("unavailable")
			},
		})

		if err := r.assignPartitionsWithRetry(ctx, logf, "slice-to-x", []string{"p1"}); err == nil {
			t.Fatal("expected an error when the assignment cannot be confirmed")
		}
	})

	t.Run("confirmation reads from the API reader", func(t *testing.T) {
		r, fc := newTestReconciler(t, s, makeSlice("slice-to-r", nil))
		r.Client = timeoutOnBind(fc, true)
		reads := 0
		r.apiReader = interceptor.NewClient(fc, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				reads++
				return c.Get(ctx, key, obj, opts...)
			},
		})

		if err := r.assignPartitionsWithRetry(ctx, logf, "slice-to-r", []string{"p1"}); err != nil {
			t.Fatalf("expected a committed assignment to succeed, got: %v", err)
		}
		if reads != 1 {
			t.Errorf("expected one read through the API reader, got %d", reads)
		}
	})
}

// A bind that times out after it was committed must keep its cache reservation: the
// Slice holds the partitions, and releasing them would let another group be placed on
// them. A bind that was not committed is rolled back as before.
func TestUpdateAPIAndHandleErrors_TimedOutBind(t *testing.T) {
	for _, tc := range []struct {
		name      string
		committed bool
	}{
		{name: "committed", committed: true},
		{name: "not committed", committed: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := scheme.Scheme
			slice := makeSlice("timeout-slice", nil)
			slice.UID = "timeout-slice-uid"
			tree := &partitiontreev1.PartitionTree{
				ObjectMeta: metav1.ObjectMeta{Name: "pt-timeout", Labels: map[string]string{TpuPodIDLabel: "pt-timeout"}},
				Status: partitiontreev1.PartitionTreeStatus{
					AcceleratorType: TPU6e,
					Root:            &partitiontreev1.PartitionInfo{PartitionID: "p1", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy},
				},
			}
			r, fc := newTestReconciler(t, s, slice)
			r.Client = timeoutOnBind(fc, tc.committed)
			r.cache.UpdateTree(ctx, tree, nil)

			toReserve := []*interfaces.SliceInfo{{Name: "timeout-slice", Type: TPU6e, Topology: "4x4", PartitionIds: []string{"p1"}}}
			if err := r.watcher.Reserve(ctx, toReserve); err != nil {
				t.Fatalf("Reserve() error: %v", err)
			}

			requeue, _ := r.updateAPIAndHandleErrors(ctx, "timeout-group",
				map[string]*slicev1beta1.Slice{"timeout-slice": slice}, toReserve, nil)

			node, _ := r.cache.GetNode("p1")
			if tc.committed {
				if requeue {
					t.Error("expected no requeue for a committed bind")
				}
				if node.InUseBy == nil || node.InUseBy.Name != "timeout-slice" {
					t.Errorf("expected p1 to stay reserved for timeout-slice, got %+v", node.InUseBy)
				}
			} else {
				if !requeue {
					t.Error("expected a requeue for a bind that was not committed")
				}
				if node.InUseBy != nil {
					t.Errorf("expected p1 to be released, held by %q", node.InUseBy.Name)
				}
			}
		})
	}
}

// ---------- Event recording tests ----------

func TestClearVictimWithRetry_EventRecording(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()
	logf := ctrl.Log.WithName("test")

	t.Run("emits preemption event on success", func(t *testing.T) {
		victim := makeSlice("victim-ev-1", []string{"p1"})
		r, _, recorder := newTestReconcilerWithRecorder(t, s, victim)

		err := r.clearVictimWithRetry(ctx, logf, "victim-ev-1", "preemptor-group")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case event := <-recorder.Events:
			if !strings.Contains(event, Preempted) {
				t.Errorf("expected Preempted event, got: %s", event)
			}
			if !strings.Contains(event, "preemptor-group") {
				t.Errorf("expected event to mention preemptor-group, got: %s", event)
			}
		default:
			t.Fatal("expected a preemption event but none was emitted")
		}
	})

	t.Run("no event on failure", func(t *testing.T) {
		victim := makeSlice("victim-ev-2", []string{"p1"})
		r, fc, recorder := newTestReconcilerWithRecorder(t, s, victim)
		r.Client = nonConflictUpdateClient(fc)

		_ = r.clearVictimWithRetry(ctx, logf, "victim-ev-2", "preemptor-group")

		select {
		case event := <-recorder.Events:
			t.Errorf("expected no event on failure, but got: %s", event)
		default:
			// Good — no event
		}
	})
}

func TestEvictSliceWithRetry_EventRecording(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()
	logf := logr.Discard()

	t.Run("emits eviction event on success", func(t *testing.T) {
		slice := makeSlice("evict-ev-1", []string{"p1"})
		r, _, recorder := newTestReconcilerWithRecorder(t, s, slice)

		err := r.evictSliceWithRetry(ctx, logf, "evict-ev-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case event := <-recorder.Events:
			if !strings.Contains(event, Evicted) {
				t.Errorf("expected Evicted event, got: %s", event)
			}
		default:
			t.Fatal("expected an eviction event but none was emitted")
		}
	})

	t.Run("no event on failure", func(t *testing.T) {
		slice := makeSlice("evict-ev-2", []string{"p1"})
		r, fc, recorder := newTestReconcilerWithRecorder(t, s, slice)
		r.Client = nonConflictUpdateClient(fc)

		_ = r.evictSliceWithRetry(ctx, logf, "evict-ev-2")

		select {
		case event := <-recorder.Events:
			t.Errorf("expected no event on failure, but got: %s", event)
		default:
			// Good — no event
		}
	})
}

// ---------- Concurrent race scenario test ----------

// TestConcurrentPreemptionNoDoubleAssignment verifies that concurrent calls to
// bindPartitionsToGroup with overlapping victims do not result in any partition
// being double-assigned.
func TestConcurrentPreemptionNoDoubleAssignment(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()

	// Set up shared victim and two distinct group slices.
	victim := makeSlice("shared-victim", []string{"p1", "p2"})
	group1 := makeSlice("group-1", nil)
	group2 := makeSlice("group-2", nil)

	r, fc := newTestReconciler(t, s, victim, group1, group2)

	// Each goroutine tries to clear the same victim and assign different partitions.
	victims1 := []*interfaces.SliceInfo{{Name: "shared-victim"}}
	slicesToReserve1 := []*interfaces.SliceInfo{
		{Name: "group-1", PartitionIds: []string{"p3", "p4"}, Topology: "2x4", Type: "v6e"},
	}
	victims2 := []*interfaces.SliceInfo{{Name: "shared-victim"}}
	slicesToReserve2 := []*interfaces.SliceInfo{
		{Name: "group-2", PartitionIds: []string{"p5", "p6"}, Topology: "2x4", Type: "v6e"},
	}

	groupSlices1 := map[string]*slicev1beta1.Slice{"group-1": group1}
	groupSlices2 := map[string]*slicev1beta1.Slice{"group-2": group2}

	// Barrier pattern: all goroutines start at the same time.
	var wg sync.WaitGroup
	var barrier sync.WaitGroup
	barrier.Add(1)

	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errs[0] = r.bindPartitionsToGroup(ctx, "group-1", groupSlices1, slicesToReserve1, victims1)
	}()
	go func() {
		defer wg.Done()
		barrier.Wait()
		errs[1] = r.bindPartitionsToGroup(ctx, "group-2", groupSlices2, slicesToReserve2, victims2)
	}()

	// Release the barrier so both goroutines start concurrently.
	barrier.Done()
	wg.Wait()

	// At least one should succeed (the victim clear is idempotent after first success).
	// Both may succeed since clearVictimWithRetry is idempotent when partitions are
	// already empty.
	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		}
	}
	if successCount == 0 {
		t.Fatalf("expected at least one goroutine to succeed, got errors: %v, %v", errs[0], errs[1])
	}

	// Verify no partition is double-assigned: collect all partition IDs across all slices.
	allPartitions := make(map[string]string) // partitionID -> sliceName
	for _, sliceName := range []string{"shared-victim", "group-1", "group-2"} {
		slice := &slicev1beta1.Slice{}
		if err := fc.Get(ctx, types.NamespacedName{Name: sliceName}, slice); err != nil {
			t.Fatalf("failed to get slice %s: %v", sliceName, err)
		}
		for _, pid := range slice.Spec.PartitionIds {
			if existing, ok := allPartitions[pid]; ok {
				t.Errorf("partition %q is double-assigned: found in both %q and %q", pid, existing, sliceName)
			}
			allPartitions[pid] = sliceName
		}
	}

	// Verify victim was annotated for eviction (partitions preserved for pod reconciler).
	victimSlice := &slicev1beta1.Slice{}
	if err := fc.Get(ctx, types.NamespacedName{Name: "shared-victim"}, victimSlice); err != nil {
		t.Fatalf("failed to get victim: %v", err)
	}
	if victimSlice.Annotations == nil || victimSlice.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
		t.Errorf("expected eviction annotation on victim, got %v", victimSlice.Annotations)
	}
}

// ---------- Watch handler mutex contention test ----------

// TestWatchHandlerMutexContention verifies that watch handlers block while the
// schedulingMutex is held, so watch handlers cannot modify
// the partition cache during an in-flight schedule→reserve→bind cycle.
func TestWatchHandlerMutexContention(t *testing.T) {
	s := scheme.Scheme
	ctx := context.Background()

	// Create a slice with partitions so handleSliceDelete will attempt to
	// acquire the schedulingMutex.
	slice := makeSlice("mutex-test", []string{"p1", "p2"})
	slice.Spec.Type = "v6e" // Must match reconciler's tpuType
	r, _ := newTestReconciler(t, s, slice)

	// Acquire the schedulingMutex in the test to simulate an in-flight scheduling cycle.
	r.schedulingMutex.Lock()

	// Spawn a goroutine that calls handleSliceDelete.
	// It should block on the schedulingMutex.
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		e := event.TypedDeleteEvent[client.Object]{
			Object: slice,
		}
		r.handleSliceDelete(ctx, e)
	}()

	// Verify the handler is blocked (should NOT complete within a short timeout).
	select {
	case <-handlerDone:
		t.Fatal("handleSliceDelete completed while schedulingMutex was held; expected it to block")
	case <-time.After(100 * time.Millisecond):
		// Good — handler is blocked as expected.
	}

	// Release the mutex and verify the handler completes.
	r.schedulingMutex.Unlock()

	select {
	case <-handlerDone:
		// Good — handler completed after mutex release.
	case <-time.After(2 * time.Second):
		t.Fatal("handleSliceDelete did not complete within 2s after mutex release")
	}
}

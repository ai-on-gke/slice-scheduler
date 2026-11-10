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

// Package policy abstracts environment-specific Slice CR interpretation.
// Environments can have different status models, annotations, and node
// health signaling. This package isolates those differences behind
// the SlicePolicy interface so reconcilers can remain environment-agnostic.
package policy

import (
	"context"
	"time"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConditionTypeReady is the condition type used to signal slice readiness.
const ConditionTypeReady = "Ready"

// SlicePolicy abstracts environment-specific Slice CR interpretation.
// Environments can have different status models, annotations, and node
// health signaling. This interface isolates those differences.
type SlicePolicy interface {
	// NeedsRescheduling returns true if a Slice's status indicates it
	// should enter the scheduling loop.
	NeedsRescheduling(slice *slicev1beta1.Slice) bool

	// ShouldBackoffOnFailure determines whether the scheduler should
	// backoff (let controller retry) or proceed with rescheduling.
	// Called when NeedsRescheduling=true AND partitions are healthy in cache.
	// Returns true to backoff, false to reschedule.
	ShouldBackoffOnFailure(slice *slicev1beta1.Slice) bool

	// ShouldEvictPod returns true if pods on this slice should be evicted.
	// The partitionBroken parameter indicates whether any of the slice's
	// partitions are broken (missing from cache or unhealthy). This allows
	// environment-specific policies to incorporate partition health into
	// eviction decisions without leaking cloud-specific logic to callers.
	ShouldEvictPod(slice *slicev1beta1.Slice, partitionBroken bool) bool

	// IsSliceReady returns true if the slice state represents a ready/active
	// state where pods can be bound.
	IsSliceReady(sliceState string) bool

	// ShouldSkipReconcile returns true if this slice should be skipped during
	// reconciliation (e.g., deactivation in progress in cloud).
	// Returns a requeue duration (0 = no requeue).
	ShouldSkipReconcile(slice *slicev1beta1.Slice) (skip bool, reason string, requeueAfter time.Duration)

	// IsNodeReadyForBinding checks whether a target node has the required
	// health labels/state for pod binding.
	IsNodeReadyForBinding(ctx context.Context, k8sClient client.Reader,
		nodeName string, topology string, sliceName string) (ready bool, retryAfter time.Duration, err error)
}

// FindReadyCondition returns the Ready condition from the slice, or nil.
func FindReadyCondition(slice *slicev1beta1.Slice) *metav1.Condition {
	for i := range slice.Status.Conditions {
		if slice.Status.Conditions[i].Type == ConditionTypeReady {
			return &slice.Status.Conditions[i]
		}
	}
	return nil
}

// GetSliceState extracts the slice state from the Ready condition's Reason field.
func GetSliceState(slice *slicev1beta1.Slice) string {
	cond := FindReadyCondition(slice)
	if cond == nil {
		return ""
	}
	return cond.Reason
}

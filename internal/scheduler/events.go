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
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	corev1 "k8s.io/api/core/v1"
)

const (
	// Event reasons
	SuccessfulScheduled = "SuccessfulScheduled"
	Preempted           = "Preempted"
	Evicted             = "Evicted"
	FailedToSchedule    = "FailedToSchedule"

	// Event actions
	ActionScheduling = "Scheduling"
	ActionPreempting = "Preempting"
	ActionEvicting   = "Evicting"
)

// recordSuccessfulRescheduleEvent records an event when a slice is successfully rescheduled.
func (r *SliceReconciler) recordSuccessfulScheduleEvent(slice *slicev1beta1.Slice, newPartitionIDs []string) {
	r.Recorder.Eventf(slice, nil, corev1.EventTypeNormal, SuccessfulScheduled, ActionScheduling, "Slice scheduled successfully to partitions %v", newPartitionIDs)
}

// recordPreemptionEvent records an event on a victim slice that was preempted.
func (r *SliceReconciler) recordPreemptionEvent(victimSlice *slicev1beta1.Slice, preemptingSliceName string) {
	r.Recorder.Eventf(victimSlice, nil, corev1.EventTypeWarning, Preempted, ActionPreempting, "Slice was preempted to make space for slice %s", preemptingSliceName)
}

// recordEvictionEvent records an event when a slice is evicted due to lack of capacity.
func (r *SliceReconciler) recordEvictionEvent(slice *slicev1beta1.Slice) {
	r.Recorder.Eventf(slice, nil, corev1.EventTypeWarning, Evicted, ActionEvicting, "Slice evicted due to lack of available partitions for rescheduling")
}

// recordNoExecuteEvictionEvent records an event when a slice is evicted from a partition with a NoExecute taint.
func (r *SliceReconciler) recordNoExecuteEvictionEvent(slice *slicev1beta1.Slice) {
	r.Recorder.Eventf(slice, nil, corev1.EventTypeWarning, Evicted, ActionEvicting, "Slice evicted from partitions %v, which have a NoExecute taint", slice.Spec.PartitionIds)
}

// recordMoveEvictionEvent records an event when a slice is evicted so it can be placed on
// partitions other than the ones it holds.
func (r *SliceReconciler) recordMoveEvictionEvent(slice *slicev1beta1.Slice, assigned []string) {
	r.Recorder.Eventf(slice, nil, corev1.EventTypeWarning, Evicted, ActionEvicting, "Slice evicted from partitions %v so it can be rescheduled onto %v", slice.Spec.PartitionIds, assigned)
}

// recordSchedulingFailure records an event when a slice fail to schedule due to lack of capacity.
func (r *SliceReconciler) recordSchedulingFailure(slice *slicev1beta1.Slice) {
	r.Recorder.Eventf(slice, nil, corev1.EventTypeWarning, FailedToSchedule, ActionScheduling, "Slice failed to schedule due to lack of available partitions")
}

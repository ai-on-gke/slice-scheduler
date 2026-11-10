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

package plugins

import (
	"context"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/util"
)

// TestingFilter prunes partitions that contain nodes marked with the
// testing label cloud.google.com/gke-scheduler-testing=NotReady.
type TestingFilter struct{}

func (f *TestingFilter) Name() string { return "TestingFilter" }

func (f *TestingFilter) Filter(ctx context.Context, req interfaces.GroupAllocationRequest, node *interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	return !node.TestingNotReady
}

// HealthyFilter prunes nodes that are marked as broken or unavailable.
type HealthyFilter struct{}

func (f *HealthyFilter) Name() string { return "HealthyFilter" }

func (f *HealthyFilter) Filter(ctx context.Context, req interfaces.GroupAllocationRequest, node *interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	return !node.IsBroken
}

// ManagedFilter prunes nodes occupied by slices not managed by the slice-scheduler.
type ManagedFilter struct{}

func (f *ManagedFilter) Name() string { return "ManagedFilter" }

func (f *ManagedFilter) Filter(ctx context.Context, req interfaces.GroupAllocationRequest, node *interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	if node.InUseBy != nil {
		// We only consider preempting slices managed by our own scheduler.
		return node.InUseBy.ManagedBy == interfaces.SliceSchedulerName
	}
	if node.IsOccupied && len(node.Children) == 0 {
		// If it's occupied but we don't know by whom (e.g. legacy or other controller),
		// and it has no children, we can't preempt it.
		return false
	}
	return true
}

// PriorityFilter prunes nodes occupied by slices with equal or higher priority than the request.
type PriorityFilter struct{}

func (f *PriorityFilter) Name() string { return "PriorityFilter" }

func (f *PriorityFilter) Filter(ctx context.Context, req interfaces.GroupAllocationRequest, node *interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	if node.InUseBy == nil {
		return true
	}
	// Bypass priority check if the partition is occupied by a slice belonging to the current rescheduling request group.
	for _, s := range req.Slices {
		if s.SliceName == node.InUseBy.Name {
			return true
		}
	}
	// A slice can only preempt victims with lower priority.
	return int32(node.InUseBy.Priority) < req.Priority
}

// ExcludeDegradedFilter blocks degraded partitions for workloads that
// do not tolerate OCS degradation (tolerance=none).
type ExcludeDegradedFilter struct{}

func (f *ExcludeDegradedFilter) Name() string { return "ExcludeDegradedFilter" }

func (f *ExcludeDegradedFilter) Filter(ctx context.Context, req interfaces.GroupAllocationRequest,
	node *interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	if len(req.Slices) == 0 {
		return true
	}
	// Use the most restrictive degradation tolerance across all slices in the group.
	// In practice groups are homogeneous, but we iterate defensively to prevent
	// a tolerance=none slice from being placed on a degraded partition if a group
	// were to contain mixed requirements.
	tolerance := interfaces.DegradationToleranceAll
	for _, sliceReq := range req.Slices {
		// OCS degradation only applies to full-cube (≥64 chips) tpu7x slices.
		if sliceReq.Type != "tpu7x" || util.ChipCount(sliceReq.Topology) < util.ChipsPerCube {
			continue
		}
		// Only "all" is permissive; any other value (empty, "none", or unknown)
		// defaults to the restrictive "none" behavior.
		if sliceReq.DegradationTolerance != interfaces.DegradationToleranceAll {
			tolerance = interfaces.DegradationToleranceNone
			break // Most restrictive — no need to check further.
		}
	}
	if tolerance == interfaces.DegradationToleranceAll {
		return true
	}
	if node.DegradationState != nil &&
		node.DegradationState.Mode == interfaces.DegradationModeDegraded {
		return false
	}
	return true
}

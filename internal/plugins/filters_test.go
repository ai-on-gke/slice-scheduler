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
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

func TestTestingFilter(t *testing.T) {
	filter := &TestingFilter{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{}

	tests := []struct {
		name     string
		node     *interfaces.PartitionInfo
		expected bool
	}{
		{
			name:     "nil node -> false",
			node:     nil,
			expected: false,
		},
		{
			name:     "Normal node (TestingNotReady: false) -> pass",
			node:     &interfaces.PartitionInfo{TestingNotReady: false},
			expected: true,
		},
		{
			name:     "Testing NotReady node -> filter out",
			node:     &interfaces.PartitionInfo{TestingNotReady: true},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filter.Filter(ctx, req, tt.node); got != tt.expected {
				t.Errorf("TestingFilter.Filter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHealthyFilter(t *testing.T) {
	filter := &HealthyFilter{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{}

	tests := []struct {
		name     string
		node     *interfaces.PartitionInfo
		expected bool
	}{
		{
			name:     "Healthy node",
			node:     &interfaces.PartitionInfo{IsBroken: false},
			expected: true,
		},
		{
			name:     "Broken node",
			node:     &interfaces.PartitionInfo{IsBroken: true},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filter.Filter(ctx, req, tt.node); got != tt.expected {
				t.Errorf("HealthyFilter.Filter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestManagedFilter(t *testing.T) {
	filter := &ManagedFilter{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{}

	tests := []struct {
		name     string
		node     *interfaces.PartitionInfo
		expected bool
	}{
		{
			name:     "Empty node",
			node:     &interfaces.PartitionInfo{InUseBy: nil},
			expected: true,
		},
		{
			name: "Managed by our scheduler",
			node: &interfaces.PartitionInfo{
				InUseBy: &interfaces.SliceInfo{ManagedBy: interfaces.SliceSchedulerName},
			},
			expected: true,
		},
		{
			name: "Managed by other scheduler",
			node: &interfaces.PartitionInfo{
				InUseBy: &interfaces.SliceInfo{ManagedBy: "other-scheduler"},
			},
			expected: false,
		},
		{
			name:     "Occupied by unknown",
			node:     &interfaces.PartitionInfo{IsOccupied: true},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filter.Filter(ctx, req, tt.node); got != tt.expected {
				t.Errorf("ManagedFilter.Filter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestPriorityFilter(t *testing.T) {
	filter := &PriorityFilter{}
	ctx := context.Background()

	tests := []struct {
		name     string
		req      interfaces.GroupAllocationRequest
		node     *interfaces.PartitionInfo
		expected bool
	}{
		{
			name:     "Empty node",
			req:      interfaces.GroupAllocationRequest{Priority: 100},
			node:     &interfaces.PartitionInfo{InUseBy: nil},
			expected: true,
		},
		{
			name: "Lower priority victim",
			req:  interfaces.GroupAllocationRequest{Priority: 100},
			node: &interfaces.PartitionInfo{
				InUseBy: &interfaces.SliceInfo{Priority: 50},
			},
			expected: true,
		},
		{
			name: "Same priority victim",
			req:  interfaces.GroupAllocationRequest{Priority: 100},
			node: &interfaces.PartitionInfo{
				InUseBy: &interfaces.SliceInfo{Priority: 100},
			},
			expected: false,
		},
		{
			name: "Higher priority victim",
			req:  interfaces.GroupAllocationRequest{Priority: 100},
			node: &interfaces.PartitionInfo{
				InUseBy: &interfaces.SliceInfo{Priority: 200},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filter.Filter(ctx, tt.req, tt.node); got != tt.expected {
				t.Errorf("PriorityFilter.Filter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestExcludeDegradedFilter(t *testing.T) {
	filter := &ExcludeDegradedFilter{}
	ctx := context.Background()

	tests := []struct {
		name     string
		req      interfaces.GroupAllocationRequest
		node     *interfaces.PartitionInfo
		expected bool
	}{
		{
			name:     "nil node → false",
			req:      interfaces.GroupAllocationRequest{},
			node:     nil,
			expected: false,
		},
		{
			name: "sub-cube topology (2x2x1) → pass",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{Type: "tpu7x", Topology: "2x2x1"},
				},
			},
			node: &interfaces.PartitionInfo{
				DegradationState: &interfaces.DegradationState{
					Mode: interfaces.DegradationModeDegraded,
				},
			},
			expected: true,
		},
		{
			name: "non-tpu7x type → pass",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{Type: "v6e", Topology: "4x4x4"},
				},
			},
			node: &interfaces.PartitionInfo{
				DegradationState: &interfaces.DegradationState{
					Mode: interfaces.DegradationModeDegraded,
				},
			},
			expected: true,
		},
		{
			name: "tolerance=none + degraded → BLOCK",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "none"},
				},
			},
			node: &interfaces.PartitionInfo{
				DegradationState: &interfaces.DegradationState{
					Mode: interfaces.DegradationModeDegraded,
				},
			},
			expected: false,
		},
		{
			name: "tolerance=none + normal → pass",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "none"},
				},
			},
			node: &interfaces.PartitionInfo{
				DegradationState: &interfaces.DegradationState{
					Mode: interfaces.DegradationModeNormal,
				},
			},
			expected: true,
		},
		{
			name: "tolerance=all + degraded → pass",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "all"},
				},
			},
			node: &interfaces.PartitionInfo{
				DegradationState: &interfaces.DegradationState{
					Mode: interfaces.DegradationModeDegraded,
				},
			},
			expected: true,
		},
		{
			name: "tolerance=prefer-none treated as invalid (defaults to none) + degraded → block",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "prefer-none"},
				},
			},
			node: &interfaces.PartitionInfo{
				DegradationState: &interfaces.DegradationState{
					Mode: interfaces.DegradationModeDegraded,
				},
			},
			expected: false,
		},
		{
			name: "tolerance=none + nil DegradationState → pass",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "none"},
				},
			},
			node:     &interfaces.PartitionInfo{},
			expected: true,
		},
		{
			name: "empty tolerance (defaults to none) + degraded → BLOCK",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: ""},
				},
			},
			node: &interfaces.PartitionInfo{
				DegradationState: &interfaces.DegradationState{
					Mode: interfaces.DegradationModeDegraded,
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filter.Filter(ctx, tt.req, tt.node); got != tt.expected {
				t.Errorf("ExcludeDegradedFilter.Filter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

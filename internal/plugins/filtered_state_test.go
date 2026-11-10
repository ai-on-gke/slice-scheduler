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

package plugins

import (
	"context"
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

// mockFilterPlugin is a filter that accepts partitions in the allowSet.
type mockFilterPlugin struct {
	allowSet map[string]bool
}

func (f *mockFilterPlugin) Name() string { return "MockFilter" }
func (f *mockFilterPlugin) Filter(ctx context.Context, req interfaces.GroupAllocationRequest, node *interfaces.PartitionInfo) bool {
	return f.allowSet[node.ID]
}

func TestGetEligiblePartitions(t *testing.T) {
	tests := []struct {
		name    string
		state   interfaces.ClusterState
		filters []interfaces.FilterPlugin
		want    map[string]struct{}
	}{
		{
			name:    "nil state returns empty",
			state:   nil,
			filters: nil,
			want:    map[string]struct{}{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetEligiblePartitions(context.Background(), interfaces.GroupAllocationRequest{}, tt.state, tt.filters)
			if len(got) != len(tt.want) {
				t.Errorf("GetEligiblePartitions() returned %d partitions, want %d", len(got), len(tt.want))
			}
		})
	}
}

func TestCollectEligibleRecursive(t *testing.T) {
	// Build a tree: root -> child1, child2
	child1 := &interfaces.PartitionInfo{ID: "c1", Topology: "2x4"}
	child2 := &interfaces.PartitionInfo{ID: "c2", Topology: "2x4"}
	root := &interfaces.PartitionInfo{
		ID:       "root",
		Topology: "4x4",
		Children: []*interfaces.PartitionInfo{child1, child2},
	}

	tests := []struct {
		name    string
		node    *interfaces.PartitionInfo
		filters []interfaces.FilterPlugin
		wantIDs map[string]struct{}
	}{
		{
			name:    "no filters, all eligible",
			node:    root,
			filters: nil,
			wantIDs: map[string]struct{}{
				"root": {},
				"c1":   {},
				"c2":   {},
			},
		},
		{
			name: "filter allows only children",
			node: root,
			filters: []interfaces.FilterPlugin{
				&mockFilterPlugin{allowSet: map[string]bool{"c1": true, "c2": true}},
			},
			wantIDs: map[string]struct{}{
				"c1": {},
				"c2": {},
			},
		},
		{
			name: "filter allows nothing",
			node: root,
			filters: []interfaces.FilterPlugin{
				&mockFilterPlugin{allowSet: map[string]bool{}},
			},
			wantIDs: map[string]struct{}{},
		},
		{
			name: "filter allows only root",
			node: root,
			filters: []interfaces.FilterPlugin{
				&mockFilterPlugin{allowSet: map[string]bool{"root": true}},
			},
			wantIDs: map[string]struct{}{
				"root": {},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eligible := make(map[string]struct{})
			collectEligibleRecursive(context.Background(), interfaces.GroupAllocationRequest{}, tt.node, tt.filters, eligible)
			if len(eligible) != len(tt.wantIDs) {
				t.Errorf("collected %d partitions, want %d. Got: %v", len(eligible), len(tt.wantIDs), eligible)
			}
			for id := range tt.wantIDs {
				if _, ok := eligible[id]; !ok {
					t.Errorf("expected partition %q to be eligible, but it wasn't", id)
				}
			}
		})
	}
}

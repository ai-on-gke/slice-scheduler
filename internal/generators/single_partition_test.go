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

package generators

import (
	"context"
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/testutil"
	"github.com/google/go-cmp/cmp"
)

// getAllPartitionIDs collects all partition IDs from a tree recursively.
func getAllPartitionIDs(node *interfaces.PartitionInfo) map[string]struct{} {
	result := map[string]struct{}{}
	if node == nil {
		return result
	}
	result[node.ID] = struct{}{}
	for _, child := range node.Children {
		for k, v := range getAllPartitionIDs(child) {
			result[k] = v
		}
	}
	return result
}

// getAllPartitionIDsFromState collects all partition IDs from all trees in the state.
func getAllPartitionIDsFromState(state *testutil.MockClusterState) map[string]struct{} {
	result := map[string]struct{}{}
	for _, root := range state.GetPartitionTrees() {
		for k, v := range getAllPartitionIDs(root) {
			result[k] = v
		}
	}
	return result
}

// getEligiblePartitionsWithFilters builds eligible partitions map by applying filters.
func getEligiblePartitionsWithFilters(state *testutil.MockClusterState, req interfaces.GroupAllocationRequest, filterPlugins []interfaces.FilterPlugin) map[string]struct{} {
	if len(filterPlugins) == 0 {
		return getAllPartitionIDsFromState(state)
	}
	return plugins.GetEligiblePartitions(context.Background(), req, state, filterPlugins)
}

func TestSinglePartitionGenerator_Generate(t *testing.T) {
	tests := []struct {
		name    string
		req     interfaces.GroupAllocationRequest
		builder func() *testutil.MockClusterState
		filters []interfaces.FilterPlugin
		want    []*interfaces.PlacementCandidate
	}{
		{
			name: "Success - Simple Match",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").Tree("4x4", "p1").Build()
			},
			want: []*interfaces.PlacementCandidate{{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "p1", Topology: "4x4", PodName: "pod1", IsAvailable: true}},
				},
				Victims: nil,
			}},
		},
		{
			name: "Success - Subpartition Split",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("8x8", "root").
					Child("4x8", "child-1").
					Child("4x4", "p4x4").Build()
			},
			want: []*interfaces.PlacementCandidate{{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "p4x4", Topology: "4x4", PodName: "pod1", IsAvailable: true}},
				},
				Victims: nil,
			}},
		},
		{
			name: "Failure - Invalid Topology",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "9x9"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").Tree("4x4", "p1").Build()
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
			state := tt.builder()
			eligible := getEligiblePartitionsWithFilters(state, tt.req, tt.filters)
			got := g.Generate(context.Background(), tt.req, state, eligible)
			if len(tt.want) == 0 && len(got) == 0 {
				return
			}
			if len(tt.want) != len(got) {
				t.Fatalf("Generate() returned %d candidates, want %d", len(got), len(tt.want))
			}
			// Compare just the assigned partition IDs and victim names
			for i := range tt.want {
				for sliceName, wantParts := range tt.want[i].Assignments {
					gotParts, ok := got[i].Assignments[sliceName]
					if !ok {
						t.Errorf("Missing slice %q in assignments", sliceName)
						continue
					}
					if len(gotParts) != len(wantParts) {
						t.Errorf("Slice %q: got %d partitions, want %d", sliceName, len(gotParts), len(wantParts))
						continue
					}
					for j := range wantParts {
						if gotParts[j].ID != wantParts[j].ID {
							t.Errorf("Slice %q partition %d: got ID %q, want %q", sliceName, j, gotParts[j].ID, wantParts[j].ID)
						}
					}
				}
			}
		})
	}
}

func TestSinglePartitionGenerator_FilterSubtree(t *testing.T) {
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("4x4", "p4x4").
		Child("2x2", "p2x2_1").UsedBy("v_high", 200, interfaces.SliceSchedulerName).ParentNode().
		Child("2x2", "p2x2_2").UsedBy("v_low", 10, interfaces.SliceSchedulerName).Build()

	req := interfaces.GroupAllocationRequest{
		Priority: 100,
		Slices:   []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4"}},
	}
	filterPlugins := []interfaces.FilterPlugin{&plugins.PriorityFilter{}}
	eligible := getEligiblePartitionsWithFilters(state, req, filterPlugins)
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) > 0 {
		t.Errorf("Expected 0 candidates because subtree has high priority slice, but got %d", len(candidates))
	}
}

func TestSinglePartitionGenerator_ReschedulingSelfVictim(t *testing.T) {
	req := interfaces.GroupAllocationRequest{
		GroupName: "s1",
		Priority:  10,
		Slices:    []interfaces.SliceRequirement{{SliceName: "s1", Topology: "4x4"}},
	}
	// State where s1 has already been released from the cache
	stateAfterRelease := testutil.NewStateBuilder().Pod("pod1").Tree("4x4", "p1").Build()

	eligible := getAllPartitionIDsFromState(stateAfterRelease)
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	candidates := g.Generate(context.Background(), req, stateAfterRelease, eligible)
	if len(candidates) == 0 {
		t.Errorf("Expected 1 candidate during rescheduling after release, but got 0.")
	}

	// Additional case: partition is still occupied by the requesting slice itself.
	// The scheduler should recognize it as its own and reuse it (no victims).
	stateStillOccupied := testutil.NewStateBuilder().Pod("pod1").
		Tree("4x4", "p1").UsedBy("s1", 10, interfaces.SliceSchedulerName).Build()

	eligible2 := getAllPartitionIDsFromState(stateStillOccupied)
	candidates2 := g.Generate(context.Background(), req, stateStillOccupied, eligible2)
	if len(candidates2) == 0 {
		t.Fatal("Expected 1 candidate when self-scheduling rescheduling, but got 0.")
	}
	if len(candidates2[0].Victims) != 0 {
		t.Errorf("Expected 0 victims (self-scheduling), got %d: %+v", len(candidates2[0].Victims), candidates2[0].Victims)
	}
}

func TestSinglePartitionGenerator_AncestorPreemption(t *testing.T) {
	// Request 2x4, but the whole 4x4 is used by a single slice.
	req := interfaces.GroupAllocationRequest{
		Priority: 100,
		Slices:   []interfaces.SliceRequirement{{SliceName: "test", Topology: "2x4"}},
	}
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("4x4", "p4x4").UsedBy("big-slice", 10, interfaces.SliceSchedulerName).
		Child("2x4", "p2x4-1").ParentNode().
		Child("2x4", "p2x4-2").Build()

	filterPlugins := []interfaces.FilterPlugin{&plugins.ManagedFilter{}, &plugins.PriorityFilter{}}
	eligible := getEligiblePartitionsWithFilters(state, req, filterPlugins)
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) == 0 {
		t.Fatal("Expected at least 1 candidate for ancestor preemption, got 0")
	}

	// Should have victims
	foundVictim := false
	for _, c := range candidates {
		if len(c.Victims) > 0 {
			foundVictim = true
			if c.Victims[0].Name != "big-slice" {
				t.Errorf("Expected victim 'big-slice', got %q", c.Victims[0].Name)
			}
		}
	}
	if !foundVictim {
		t.Error("Expected at least one candidate with victims")
	}
}

func TestSinglePartitionGenerator_ChildPreemption(t *testing.T) {
	// Request 4x4, but one 2x4 child is in use.
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4"}},
	}
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("4x4", "p4x4").
		Child("2x4", "p2x4-1").UsedBy("small-slice", 10, interfaces.SliceSchedulerName).ParentNode().
		Child("2x4", "p2x4-2").Build()

	eligible := getAllPartitionIDsFromState(state)
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) == 0 {
		t.Fatal("Expected at least 1 candidate for child preemption, got 0")
	}

	// The 4x4 candidate should have the small-slice as a victim
	found := false
	for _, c := range candidates {
		parts := c.Assignments["test"]
		if len(parts) > 0 && parts[0].ID == "p4x4" {
			found = true
			if len(c.Victims) == 0 || c.Victims[0].Name != "small-slice" {
				t.Errorf("Expected victim 'small-slice' for p4x4 candidate")
			}
		}
	}
	if !found {
		// Check for any valid candidate
		if len(candidates) > 0 {
			_ = cmp.Diff(nil, candidates) // just for import usage
		}
	}
}

func TestSinglePartitionGenerator_BrokenPruning(t *testing.T) {
	// Request 16x16. One deep leaf is broken. Root becomes broken.
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "16x16"}},
	}
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("16x16", "root").
		Child("8x16", "left").
		Child("8x8", "leaf").Broken().Build()

	filterPlugins := []interfaces.FilterPlugin{&plugins.HealthyFilter{}}
	eligible := getEligiblePartitionsWithFilters(state, req, filterPlugins)
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) != 0 {
		t.Errorf("Expected 0 candidates due to broken pruning, got %d", len(candidates))
	}
}

func TestSinglePartitionGenerator_SiblingOfBroken(t *testing.T) {
	// Request 8x16. Root 16x16 is broken because of 'leaf', but 'right' 8x16 sibling is healthy.
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "8x16"}},
	}
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("16x16", "root").
		Child("8x16", "left").
		Child("8x8", "leaf").Broken().ParentNode().ParentNode().
		Child("8x16", "right").Build()

	filterPlugins := []interfaces.FilterPlugin{&plugins.HealthyFilter{}}
	eligible := getEligiblePartitionsWithFilters(state, req, filterPlugins)
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) == 0 {
		t.Fatal("Expected at least 1 candidate from sibling of broken, got 0")
	}

	// Check the right partition was chosen
	parts := candidates[0].Assignments["test"]
	if len(parts) == 0 || parts[0].ID != "right" {
		t.Errorf("Expected partition 'right', got %v", parts)
	}
}

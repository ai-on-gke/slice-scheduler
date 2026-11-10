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
	"sort"
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/cost"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/testutil"
)

func TestMultiCubeGenerator_Generate(t *testing.T) {
	tests := []struct {
		name    string
		req     interfaces.GroupAllocationRequest
		builder func() *testutil.MockClusterState
		filters []interfaces.FilterPlugin
		wantIDs []string // expected assigned partition IDs (sorted)
		wantNil bool     // expect nil result
	}{
		{
			name: "Success - Single Pod Enough Cubes",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("4x4x16", "root1").
					Child("4x4x4", "c1").ParentNode().
					Child("4x4x4", "c2").ParentNode().
					Child("4x4x4", "c3").Build()
			},
			wantIDs: []string{"c1", "c2"},
		},
		{
			name: "Failure - Capacity Split Across Pods",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod1").Tree("4x4x4", "pod1-c1").ParentPod().
					Pod("pod2").Tree("4x4x4", "pod2-c1").Build()
			},
			wantNil: true,
		},
		{
			name: "Success - Sub-Cube Preemption",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("4x4x16", "root1").
					Child("4x4x4", "c1").ParentNode().
					Child("4x4x4", "c2").
					Child("2x2x2", "c2-sub").UsedBy("tiny", 5, interfaces.SliceSchedulerName).Build()
			},
			wantIDs: []string{"c1", "c2"},
		},
		{
			name: "Success - Sink to Degraded (Same Preemption Cost)",
			req:  interfaces.GroupAllocationRequest{Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}}}, // 2 cubes
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("4x4x4", "c1").ParentPod().                  // Normal
					Tree("4x4x4", "c2").Degraded("ocs1").ParentPod(). // Degraded
					Tree("4x4x4", "c3").Degraded("ocs1").Build()      // Degraded
			},
			wantIDs: []string{"c2", "c3"},
		},
		{
			name: "Success - Domain Isolation (Mix of Degraded and Normal)",
			req:  interfaces.GroupAllocationRequest{Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}}}, // 2 cubes
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("4x4x4", "c1").Degraded("ocs1").ParentPod(). // Degraded 1
					Tree("4x4x4", "c2").Degraded("ocs2").ParentPod(). // Degraded 2
					Tree("4x4x4", "c3").ParentPod().                  // Normal
					Tree("4x4x4", "c4").Build()                       // Normal
			},
			wantIDs: []string{"c1", "c3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
			state := tt.builder()
			eligible := getEligiblePartitionsWithFilters(state, tt.req, tt.filters)
			got := g.Generate(context.Background(), tt.req, state, eligible)

			if tt.wantNil {
				if len(got) != 0 {
					t.Errorf("Expected 0 candidates, got %d", len(got))
				}
				return
			}

			if len(got) == 0 {
				t.Fatal("Expected at least 1 candidate, got 0")
			}

			// Check the first candidate's assignments
			c := got[0]
			parts := c.Assignments["test"]
			var gotIDs []string
			for _, p := range parts {
				gotIDs = append(gotIDs, p.ID)
			}
			sort.Strings(gotIDs)
			sort.Strings(tt.wantIDs)

			if len(gotIDs) != len(tt.wantIDs) {
				t.Fatalf("Got %d partition IDs %v, want %d %v", len(gotIDs), gotIDs, len(tt.wantIDs), tt.wantIDs)
			}
			for i := range tt.wantIDs {
				if gotIDs[i] != tt.wantIDs[i] {
					t.Errorf("Partition ID %d: got %q, want %q", i, gotIDs[i], tt.wantIDs[i])
				}
			}
		})
	}
}

func TestMultiCubeGenerator_SinkToDegraded(t *testing.T) {
	// 3 cubes: c1 (normal), c2 (degraded ocs1), c3 (degraded ocs1).
	// Requesting 2-cube (4x4x8). The solver should prefer c2+c3 (degraded)
	// over c1+c2 or c1+c3 because degraded cubes have lower DegradationCost.
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}},
	}
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("4x4x4", "c1").ParentPod().                  // Normal
		Tree("4x4x4", "c2").Degraded("ocs1").ParentPod(). // Degraded
		Tree("4x4x4", "c3").Degraded("ocs1").Build()      // Degraded

	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	eligible := getAllPartitionIDsFromState(state)
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) == 0 {
		t.Fatal("Expected at least 1 candidate, got 0")
	}

	c := candidates[0]
	parts := c.Assignments["test"]
	var gotIDs []string
	for _, p := range parts {
		gotIDs = append(gotIDs, p.ID)
	}
	sort.Strings(gotIDs)
	expected := []string{"c2", "c3"}

	if len(gotIDs) != len(expected) {
		t.Fatalf("Got %d partition IDs %v, want %d %v", len(gotIDs), gotIDs, len(expected), expected)
	}
	for i := range expected {
		if gotIDs[i] != expected[i] {
			t.Errorf("Partition ID %d: got %q, want %q", i, gotIDs[i], expected[i])
		}
	}
}

func TestMultiCubeGenerator_DomainIsolation(t *testing.T) {
	// 4 cubes: c1 (degraded ocs1), c2 (degraded ocs2), c3 (normal), c4 (normal).
	// Requesting 2-cube (4x4x8). The solver should prefer c1+c3 (ocs1 domain + normal pool)
	// over c3+c4 (normal only, higher DegradationCost).
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}},
	}
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("4x4x4", "c1").Degraded("ocs1").ParentPod(). // Degraded 1
		Tree("4x4x4", "c2").Degraded("ocs2").ParentPod(). // Degraded 2
		Tree("4x4x4", "c3").ParentPod().                  // Normal
		Tree("4x4x4", "c4").Build()                       // Normal

	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	eligible := getAllPartitionIDsFromState(state)
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) == 0 {
		t.Fatal("Expected at least 1 candidate, got 0")
	}

	c := candidates[0]
	parts := c.Assignments["test"]
	var gotIDs []string
	for _, p := range parts {
		gotIDs = append(gotIDs, p.ID)
	}
	sort.Strings(gotIDs)
	// Should pick from a single degraded domain + normal pool, minimizing normal count.
	// ocs1 domain: c1 (degraded) + c3 (normal) → normalCount=1
	// ocs2 domain: c2 (degraded) + c3 (normal) → normalCount=1
	// Both have same cost, sorted domains pick ocs1 first → c1+c3
	expected := []string{"c1", "c3"}

	if len(gotIDs) != len(expected) {
		t.Fatalf("Got %d partition IDs %v, want %d %v", len(gotIDs), gotIDs, len(expected), expected)
	}
	for i := range expected {
		if gotIDs[i] != expected[i] {
			t.Errorf("Partition ID %d: got %q, want %q", i, gotIDs[i], expected[i])
		}
	}
}

func TestMultiCubeGenerator_DuplicateVictims(t *testing.T) {
	req := interfaces.GroupAllocationRequest{
		Priority: 100,
		Slices:   []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}},
	}

	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("8x8x12", "root").
		Child("4x4x4", "cube1").UsedBy("v1", 10, interfaces.SliceSchedulerName).ParentNode().
		Child("4x4x4", "cube2").UsedBy("v1", 10, interfaces.SliceSchedulerName).Build()

	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	eligible := getAllPartitionIDsFromState(state)
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) != 1 {
		t.Fatalf("Expected 1 candidate, got %d", len(candidates))
	}

	candidate := candidates[0]
	// Victims should be deduplicated by Name even if they appear in multiple cubes.
	if len(candidate.Victims) != 1 {
		t.Errorf("Expected 1 victim (deduplicated), got %d: %+v", len(candidate.Victims), candidate.Victims)
	}
}

func TestMultiCubeGenerator_AggressivePruning(t *testing.T) {
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("4x4x16", "root").
		Child("4x4x4", "c1").ParentNode().
		Child("4x4x4", "c2").ParentNode().
		Child("4x4x4", "c3").ParentNode().
		Child("4x4x4", "c4").Broken().Build()

	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}},
	}

	filterPlugins := []interfaces.FilterPlugin{&plugins.HealthyFilter{}}
	eligible := getEligiblePartitionsWithFilters(state, req, filterPlugins)
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	candidates := g.Generate(context.Background(), req, state, eligible)

	// It should find candidates from c1, c2, c3 even if c4 is broken.
	if len(candidates) == 0 {
		t.Errorf("Expected at least 1 candidate (from c1, c2, c3), but got 0 due to aggressive pruning of partially broken trees")
	}
}

func TestMultiCubeGenerator_TieBreakOnNumSlices(t *testing.T) {
	// Both sets have MaxPriority 10.
	// Set A: c1, c2 (2 slices: v1, v2)
	// Set B: c3, c4 (1 slice: v3 spanning both)
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test", Topology: "4x4x8"}},
	}
	state := testutil.NewStateBuilder().Pod("pod1").
		Tree("4x4x16", "root1").
		Child("4x4x4", "c1").UsedBy("v1", 10, interfaces.SliceSchedulerName).ParentNode().
		Child("4x4x4", "c2").UsedBy("v2", 10, interfaces.SliceSchedulerName).ParentNode().
		Child("4x4x4", "c3").UsedBy("v3", 10, interfaces.SliceSchedulerName).ParentNode().
		Child("4x4x4", "c4").UsedBy("v3", 10, interfaces.SliceSchedulerName).Build()

	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	eligible := getAllPartitionIDsFromState(state)
	candidates := g.Generate(context.Background(), req, state, eligible)

	if len(candidates) == 0 {
		t.Fatal("Expected at least 1 candidate, got 0")
	}

	// Algorithm should prefer c3, c4 because it only evicts ONE slice (v3).
	c := candidates[0]
	parts := c.Assignments["test"]
	var gotIDs []string
	for _, p := range parts {
		gotIDs = append(gotIDs, p.ID)
	}
	sort.Strings(gotIDs)
	expected := []string{"c3", "c4"}
	sort.Strings(expected)

	if len(gotIDs) != len(expected) {
		t.Fatalf("Got %d partition IDs %v, want %d %v", len(gotIDs), gotIDs, len(expected), expected)
	}
	for i := range expected {
		if gotIDs[i] != expected[i] {
			t.Errorf("Partition ID %d: got %q, want %q", i, gotIDs[i], expected[i])
		}
	}
}

func TestGetOcsDomain(t *testing.T) {
	tests := []struct {
		name string
		part *interfaces.PartitionInfo
		want string
	}{
		{
			name: "Normal partition",
			part: &interfaces.PartitionInfo{ID: "p1"},
			want: "",
		},
		{
			name: "Degraded with OCS Name",
			part: &interfaces.PartitionInfo{
				ID: "p2",
				DegradationState: &interfaces.DegradationState{
					Mode:             interfaces.DegradationModeDegraded,
					ToleratedOcsName: "ocs-fault-1",
				},
			},
			want: "ocs-fault-1",
		},
		{
			name: "Cloud compatible degraded state",
			part: &interfaces.PartitionInfo{
				ID:    "p3",
				State: interfaces.PartitionStateDegraded,
			},
			want: defaultDegradationDomain,
		},
		{
			name: "Degraded with empty ToleratedOcsName",
			part: &interfaces.PartitionInfo{
				ID: "p4",
				DegradationState: &interfaces.DegradationState{
					Mode:             interfaces.DegradationModeDegraded,
					ToleratedOcsName: "",
				},
			},
			want: defaultDegradationDomain,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getOcsDomain(tt.part)
			if got != tt.want {
				t.Errorf("getOcsDomain() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrepareSolverItems_SwapCost(t *testing.T) {
	availableCubes := []*interfaces.PlacementOption{
		{Partitions: []*interfaces.PartitionInfo{{ID: "c1", Topology: "4x4x4"}}},
		{Partitions: []*interfaces.PartitionInfo{{ID: "c2", Topology: "4x4x4"}}},
	}
	currentIDs := []string{"c1"}
	currentIDSet := make(map[string]bool, len(currentIDs))
	for _, id := range currentIDs {
		currentIDSet[id] = true
	}

	items := prepareSolverItems(availableCubes, nil, currentIDSet)
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	for _, item := range items {
		dpc, ok := item.Cost.(cost.DegradationPreemptionCost)
		if !ok {
			t.Fatalf("expected DegradationPreemptionCost, got %T", item.Cost)
		}
		if item.Name == "c1" && dpc.SwapCost != 0 {
			t.Errorf("expected c1 to have 0 SwapCost, got %v", dpc.SwapCost)
		}
		if item.Name == "c2" && dpc.SwapCost != 1 {
			t.Errorf("expected c2 to have 1 SwapCost, got %v", dpc.SwapCost)
		}
	}
}

// degradedCube builds a single-cube option in the named OCS degradation domain,
// with no victims, so options differ only in their domain and ID.
func degradedCube(id, domain string) *interfaces.PlacementOption {
	return &interfaces.PlacementOption{
		Partitions: []*interfaces.PartitionInfo{{
			ID:       id,
			Topology: "4x4x4",
			DegradationState: &interfaces.DegradationState{
				Mode:             interfaces.DegradationModeDegraded,
				ToleratedOcsName: domain,
			},
		}},
	}
}

func TestGetOptionCost_CountsSwaps(t *testing.T) {
	currentIDSet := map[string]bool{"held": true}
	opt := &interfaces.PlacementOption{
		Partitions: []*interfaces.PartitionInfo{
			{ID: "held", Topology: "4x4x4"},
			{ID: "new", Topology: "4x4x4"},
		},
	}
	if got := getOptionCost(opt, currentIDSet).SwapCost; got != 1 {
		t.Errorf("SwapCost = %v, want 1 (only \"new\" is a relocation)", got)
	}
	if got := getOptionCost(opt, nil).SwapCost; got != 2 {
		t.Errorf("SwapCost with no current partitions = %v, want 2", got)
	}
}

// The winning option of each OCS domain is compared against the other domains'
// winners by getOptionCost. Omitting SwapCost there left that comparison unable
// to see relocations at all, so two domains that tie on preemption and
// degradation were separated by nothing but the sorted domain order -- and a
// reschedule that could have kept the slice where it already runs lost to an
// alphabetically earlier domain.
func TestFindDisjointPlacementOptions_SwapBreaksCrossDomainTie(t *testing.T) {
	ctx := context.Background()
	// domain-a sorts first and would win any tie on position alone; domain-b is
	// the one the slice already occupies.
	options := []*interfaces.PlacementOption{
		degradedCube("cube-a", "domain-a"),
		degradedCube("cube-b", "domain-b"),
	}

	got := findDisjointPlacementOptions(ctx, options, 1, 1, []string{"cube-b"})

	if len(got) != 1 {
		t.Fatalf("findDisjointPlacementOptions returned %d options, want 1", len(got))
	}
	if len(got[0].Partitions) != 1 {
		t.Fatalf("option holds %d partitions, want 1", len(got[0].Partitions))
	}
	if id := got[0].Partitions[0].ID; id != "cube-b" {
		t.Errorf("selected %q, want \"cube-b\": the two domains tie on preemption and degradation, so the option that relocates nothing must win", id)
	}
}

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

package schedulerlib

import (
	"context"
	"sort"
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/generators"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/testutil"
)

// makeReq creates a single-slice GroupAllocationRequest from simple parameters.
func makeReq(sliceName, topology, tpuType string, priority int32) interfaces.GroupAllocationRequest {
	return interfaces.GroupAllocationRequest{
		GroupName: sliceName,
		Priority:  priority,
		Slices: []interfaces.SliceRequirement{
			{
				SliceName: sliceName,
				Topology:  topology,
				Type:      tpuType,
			},
		},
	}
}

// extractResult extracts assigned partition IDs and preempted slice names from a GroupAllocationResult.
type simplifiedResult struct {
	IsSchedulable      bool
	AssignedPartitions []string
	PreemptedSlices    []string
}

func simplifyResult(res interfaces.GroupAllocationResult, sliceName string) simplifiedResult {
	sr := simplifiedResult{
		IsSchedulable: res.IsSchedulable,
	}
	if !res.IsSchedulable || res.Candidate == nil {
		return sr
	}
	for _, parts := range res.Candidate.Assignments {
		for _, p := range parts {
			sr.AssignedPartitions = append(sr.AssignedPartitions, p.ID)
		}
	}
	for _, v := range res.Candidate.Victims {
		sr.PreemptedSlices = append(sr.PreemptedSlices, v.Name)
	}
	return sr
}

func sortSimplified(sr *simplifiedResult) {
	if sr.AssignedPartitions != nil {
		sort.Strings(sr.AssignedPartitions)
	}
	if sr.PreemptedSlices != nil {
		sort.Strings(sr.PreemptedSlices)
	}
}

func TestDefaultScheduler_Comprehensive(t *testing.T) {
	tests := []struct {
		name    string
		req     interfaces.GroupAllocationRequest
		builder func() *testutil.MockClusterState
		want    simplifiedResult
	}{
		// ====================================================================
		// Group 1: Trillium Placement & Bin-Packing
		// ====================================================================
		{
			name: "Scenario: Trillium Exact Match",
			req:  makeReq("test", "4x4", TPU6e, 0),
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod1").Tree("4x4", "p1").Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"p1"},
			},
		},
		{
			name: "Scenario: Trillium Subpartition Split",
			req:  makeReq("test", "2x4", TPU6e, 0),
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod1").Tree("4x4", "parent").
					Child("2x4", "p1").ParentNode().
					Child("2x4", "p2").Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"p1"},
			},
		},
		{
			name: "Scenario: Trillium No Available Partitions (Failure)",
			req:  makeReq("test", "16x16", TPU6e, 0),
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod1").Tree("8x16", "p1").Build()
			},
			want: simplifiedResult{
				IsSchedulable: false,
			},
		},

		// ====================================================================
		// Group 2: Ironwood Multi-Cube Placement
		// ====================================================================
		{
			name: "Scenario: Ironwood Multi-Cube",
			req:  makeReq("test", "4x4x8", TPU7x, 0),
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod1").
					Tree("4x4x4", "c1").ParentPod().
					Tree("4x4x4", "c2").Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c2"},
			},
		},
		{
			name: "Scenario: Ironwood Pod-Local Constraint (Hard Fail)",
			req:  makeReq("test", "4x4x8", TPU7x, 100),
			builder: func() *testutil.MockClusterState {
				// Multi-cube slices cannot span multiple pods.
				return testutil.NewStateBuilder().
					Pod("pod-a").Tree("4x4x4", "a1").ParentPod().
					Pod("pod-b").Tree("4x4x4", "b1").
					Build()
			},
			want: simplifiedResult{
				IsSchedulable: false,
			},
		},
		{
			name: "Scenario: Ironwood Not Enough Cubes (Failure)",
			req:  makeReq("test", "4x4x12", TPU7x, 0),
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod1").
					Tree("4x4x4", "c1").ParentPod().
					Tree("4x4x4", "c2").Build()
			},
			want: simplifiedResult{
				IsSchedulable: false,
			},
		},

		// ====================================================================
		// Group 3: Preemption & Priority Policies
		// ====================================================================
		{
			name: "Scenario: Trillium Preemption (Single Victim)",
			req:  makeReq("test", "4x4", TPU6e, 100),
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod1").Tree("4x4", "p1").UsedBy("victim", 10, interfaces.SliceSchedulerName).Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"p1"},
				PreemptedSlices:    []string{"victim"},
			},
		},
		{
			name: "Scenario: Fragmented Preemption (Multi-Victim)",
			req:  makeReq("test", "8x8", "", 100),
			builder: func() *testutil.MockClusterState {
				// Root is free, but all children are used by separate slices.
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("8x8", "root").
					Child("4x4", "c1").UsedBy("v1", 10, interfaces.SliceSchedulerName).ParentNode().
					Child("4x4", "c2").UsedBy("v2", 10, interfaces.SliceSchedulerName).ParentNode().
					Child("4x4", "c3").UsedBy("v3", 10, interfaces.SliceSchedulerName).ParentNode().
					Child("4x4", "c4").UsedBy("v4", 10, interfaces.SliceSchedulerName).
					Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"root"},
				PreemptedSlices:    []string{"v1", "v2", "v3", "v4"},
			},
		},
		{
			name: "Scenario: Ironwood Multi-Cube Preemption",
			req:  makeReq("test", "4x4x8", TPU7x, 100),
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod1").
					Tree("4x4x4", "c1").UsedBy("v1", 10, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c2").UsedBy("v2", 10, interfaces.SliceSchedulerName).Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c2"},
				PreemptedSlices:    []string{"v1", "v2"},
			},
		},
		{
			name: "Scenario: Unmanaged Shielding (External Prevents Preemption)",
			req:  makeReq("test", "16x16", "", 999),
			builder: func() *testutil.MockClusterState {
				// External slices block the entire path to the root from being preempted.
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("16x16", "root").
					Child("8x16", "left").ParentNode().
					Child("8x16", "right").
					Child("8x8", "r1").ParentNode().
					Child("8x8", "r2").UsedBy("manual-v", 1, "manual").
					Build()
			},
			want: simplifiedResult{
				IsSchedulable: false,
			},
		},

		// ====================================================================
		// Group 4: Health & Rescheduling
		// ====================================================================
		{
			name: "Scenario: Bottom-Up Broken Pruning",
			req:  makeReq("test", "16x16", "", 0),
			builder: func() *testutil.MockClusterState {
				// If a leaf is broken, the entire ancestor chain is unusable.
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("16x16", "root").
					Child("8x16", "left").ParentNode().
					Child("8x16", "right").
					Child("8x8", "r1").ParentNode().
					Child("8x8", "r2").
					Child("4x8", "r2a").ParentNode().
					Child("4x8", "r2b").Broken().
					Build()
			},
			want: simplifiedResult{
				IsSchedulable: false,
			},
		},
		{
			name: "Scenario: Ironwood Partial Health (Resilience)",
			req:  makeReq("test", "4x4x8", TPU7x, 0),
			builder: func() *testutil.MockClusterState {
				// Pod has 4 cubes. One is broken.
				// A 4x4x8 request (2 cubes) should still succeed using the healthy ones.
				return testutil.NewStateBuilder().
					Pod("pod1").
					Tree("4x4x4", "c1").ParentPod().
					Tree("4x4x4", "c2").ParentPod().
					Tree("4x4x4", "c3").ParentPod().
					Tree("4x4x4", "c4").Broken().
					Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c2"},
			},
		},
		{
			name: "Scenario: Move from Broken (Rescheduling)",
			req:  makeReq("s1", "2x4", "", 0),
			builder: func() *testutil.MockClusterState {
				// Current partition p1 is broken, move to free p2.
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("4x4", "root").
					Child("2x4", "p1").Broken().ParentNode().
					Child("2x4", "p2").
					Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"p2"},
			},
		},
		{
			name: "Scenario: Evict to Nowhere (Rescheduling Failure)",
			req:  makeReq("s1", "2x4", "", 0),
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("2x4", "p1").Broken().
					Build()
			},
			want: simplifiedResult{
				IsSchedulable: false,
			},
		},
		{
			name: "Scenario: Ironwood Self-Scheduling Reschedule (Success)",
			req:  makeReq("s1", "4x4x8", TPU7x, 100),
			builder: func() *testutil.MockClusterState {
				// 3 cubes available. c1 and c2 are used by the request slice "s1".
				// c2 is broken. c3 is free.
				// The scheduler should reuse c1 and pick c3 for the 2-cube request.
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("4x4x4", "c1").UsedBy("s1", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c2").UsedBy("s1", 100, interfaces.SliceSchedulerName).Broken().ParentPod().
					Tree("4x4x4", "c3").Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c3"},
			},
		},
		{
			name: "Scenario: Ironwood Self-Scheduling Reschedule with External Preemption",
			req:  makeReq("s1", "4x4x8", TPU7x, 100),
			builder: func() *testutil.MockClusterState {
				// 3 cubes: c1 self-owned (healthy), c2 self-owned (broken), c3 occupied by lower-priority "other".
				// Scheduler should reuse c1 and preempt "other" on c3.
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("4x4x4", "c1").UsedBy("s1", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c2").UsedBy("s1", 100, interfaces.SliceSchedulerName).Broken().ParentPod().
					Tree("4x4x4", "c3").UsedBy("other", 50, interfaces.SliceSchedulerName).Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c3"},
				PreemptedSlices:    []string{"other"},
			},
		},
		{
			name: "Scenario: Multi-Slice Gang Rescheduling (Success)",
			req: interfaces.GroupAllocationRequest{
				GroupName: "gang",
				Priority:  100,
				Slices: []interfaces.SliceRequirement{
					{SliceName: "gang-s0", Topology: "4x4x4", Type: TPU7x},
					{SliceName: "gang-s1", Topology: "4x4x4", Type: TPU7x},
				},
			},
			builder: func() *testutil.MockClusterState {
				// 3 cubes: c1 self-owned by gang-s0, c2 self-owned by gang-s1, c3 broken.
				// Both self-owned cubes are healthy, scheduler should reuse both.
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("4x4x4", "c1").UsedBy("gang-s0", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c2").UsedBy("gang-s1", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c3").Broken().Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c2"},
			},
		},
		{
			name: "Scenario: Swap Only One Cube On Failure (Multi-Cube Reschedule)",
			req: interfaces.GroupAllocationRequest{
				GroupName: "s1",
				Priority:  100,
				Slices: []interfaces.SliceRequirement{{
					SliceName:           "s1",
					Topology:            "4x4x8",
					Type:                TPU7x,
					CurrentPartitionIDs: []string{"c3", "c4"},
				}},
			},
			builder: func() *testutil.MockClusterState {
				// 4 cubes available: c1, c2, c3, c4.
				// c3 and c4 are currently owned by s1.
				// c4 is broken. c1 and c2 are both free and healthy.
				// Without SwapCost, default scheduler would pick [c1, c2] (2 swaps).
				// With SwapCost, scheduler must keep c3 and pick [c1, c3].
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("4x4x4", "c1").ParentPod().
					Tree("4x4x4", "c2").ParentPod().
					Tree("4x4x4", "c3").UsedBy("s1", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c4").UsedBy("s1", 100, interfaces.SliceSchedulerName).Broken().Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c3"},
			},
		},
		{
			name: "Scenario: Swap Only One Cube On Failure (Gang Request)",
			req: interfaces.GroupAllocationRequest{
				GroupName: "gang-1",
				Priority:  100,
				Slices: []interfaces.SliceRequirement{
					{
						SliceName:           "s1",
						Topology:            "4x4x8",
						Type:                TPU7x,
						CurrentPartitionIDs: []string{"c3", "c4"},
					},
					{
						SliceName: "s2",
						Topology:  "4x4x8",
						Type:      TPU7x,
					},
				},
			},
			builder: func() *testutil.MockClusterState {
				// 6 cubes: c1, c2, c3, c4, c5, c6 in pod-1.
				// s1 is currently on c3 and c4. c4 is broken.
				// s2 is a new slice requesting 2 cubes.
				// s1 must keep healthy c3 (swap cost 1) and take c1 -> [c1, c3].
				// s2 will take remaining free cubes -> [c2, c5].
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("4x4x4", "c1").ParentPod().
					Tree("4x4x4", "c2").ParentPod().
					Tree("4x4x4", "c3").UsedBy("s1", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c4").UsedBy("s1", 100, interfaces.SliceSchedulerName).Broken().ParentPod().
					Tree("4x4x4", "c5").ParentPod().
					Tree("4x4x4", "c6").Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c2", "c3", "c5"},
			},
		},
		{
			name: "Scenario: 4-Cube Slice Partial Failure (Keep 3 Healthy Cubes, Swap 1)",
			req: interfaces.GroupAllocationRequest{
				GroupName: "s1",
				Priority:  100,
				Slices: []interfaces.SliceRequirement{{
					SliceName:           "s1",
					Topology:            "4x4x16",
					Type:                TPU7x,
					CurrentPartitionIDs: []string{"c1", "c2", "c3", "c4"},
				}},
			},
			builder: func() *testutil.MockClusterState {
				// c1, c2, c3 healthy; c4 broken. c5 free.
				// Scheduler must keep c1, c2, c3 and pick c5 -> [c1, c2, c3, c5].
				return testutil.NewStateBuilder().
					Pod("pod-1").
					Tree("4x4x4", "c1").UsedBy("s1", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c2").UsedBy("s1", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c3").UsedBy("s1", 100, interfaces.SliceSchedulerName).ParentPod().
					Tree("4x4x4", "c4").UsedBy("s1", 100, interfaces.SliceSchedulerName).Broken().ParentPod().
					Tree("4x4x4", "c5").Build()
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"c1", "c2", "c3", "c5"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createDefaultScheduler()
			state := tt.builder()
			got := s.Schedule(context.Background(), tt.req, state)

			sr := simplifyResult(got, tt.req.Slices[0].SliceName)

			if sr.IsSchedulable != tt.want.IsSchedulable {
				t.Fatalf("IsSchedulable = %v, want %v. FailureReason: %v", sr.IsSchedulable, tt.want.IsSchedulable, got.FailureReason)
			}

			if !tt.want.IsSchedulable {
				return
			}

			sortSimplified(&sr)
			sortSimplified(&tt.want)

			// Compare assigned partitions
			if len(sr.AssignedPartitions) != len(tt.want.AssignedPartitions) {
				t.Fatalf("AssignedPartitions count = %d %v, want %d %v", len(sr.AssignedPartitions), sr.AssignedPartitions, len(tt.want.AssignedPartitions), tt.want.AssignedPartitions)
			}
			for i := range tt.want.AssignedPartitions {
				if sr.AssignedPartitions[i] != tt.want.AssignedPartitions[i] {
					t.Errorf("AssignedPartitions[%d] = %v, want %v", i, sr.AssignedPartitions[i], tt.want.AssignedPartitions[i])
				}
			}

			// Compare preempted slices
			if len(sr.PreemptedSlices) != len(tt.want.PreemptedSlices) {
				t.Fatalf("PreemptedSlices count = %d %v, want %d %v", len(sr.PreemptedSlices), sr.PreemptedSlices, len(tt.want.PreemptedSlices), tt.want.PreemptedSlices)
			}
			for i := range tt.want.PreemptedSlices {
				if sr.PreemptedSlices[i] != tt.want.PreemptedSlices[i] {
					t.Errorf("PreemptedSlices[%d] = %v, want %v", i, sr.PreemptedSlices[i], tt.want.PreemptedSlices[i])
				}
			}
		})
	}
}

// createDefaultScheduler initializes a scheduler with all standard plugins.
func createDefaultScheduler() *DefaultScheduler {
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 10}),
	}

	filters := []interfaces.FilterPlugin{
		&plugins.HealthyFilter{},
		&plugins.ManagedFilter{},
		&plugins.PriorityFilter{},
	}

	scorers := []interfaces.ScorePlugin{
		&plugins.PreemptionScorer{},
		&plugins.MinimizeSwappedCubesScorer{},
		&plugins.TightestFitScorer{},
	}

	return NewDefaultScheduler(gens, filters, scorers)
}

type mockGenerator struct {
	candidates []*interfaces.PlacementCandidate
}

func (g *mockGenerator) Name() string { return "MockGenerator" }
func (g *mockGenerator) Generate(ctx context.Context, req interfaces.GroupAllocationRequest, state interfaces.ClusterState, eligiblePartitions map[string]struct{}) []*interfaces.PlacementCandidate {
	return g.candidates
}

type mockScorer struct {
	scoreCost mockCost
}

type mockCost int

func (c mockCost) Less(other interfaces.Score) bool {
	o, ok := other.(mockCost)
	if !ok {
		return false
	}
	return int(c) < int(o)
}

func (s *mockScorer) Name() string { return "MockScorer" }
func (s *mockScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	return s.scoreCost
}

func TestDefaultScheduler_OCSDegradationAll(t *testing.T) {
	// Setup: 2 cubes on pod-a and pod-b. pod-a has normal cube. pod-b has degraded cube (ocs1).
	// Request: tpu7x, 4x4x4, tolerance=all
	// Expected: Schedulable, picks the degraded cube (lower DegradationCost)
	state := testutil.NewStateBuilder().
		Pod("pod-a").Tree("4x4x4", "c1-normal").ParentPod().
		Pod("pod-b").Tree("4x4x4", "c9-degraded").Degraded("ocs1").ParentPod().Build()

	req := interfaces.GroupAllocationRequest{
		GroupName: "test-group",
		Slices: []interfaces.SliceRequirement{
			{SliceName: "slice1", Topology: "4x4x4", Type: TPU7x, DegradationTolerance: interfaces.DegradationToleranceAll},
		},
		Priority: 100,
	}

	s := createDegradationScheduler()
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected group allocation to succeed, but failed: %s", result.FailureReason)
	}

	winningPod := result.Candidate.Assignments["slice1"][0].PodName
	if winningPod != "pod-b" {
		t.Errorf("Policy Failure! Expected scheduler to select pod-b (degraded) to preserve normal cubes, but selected %s", winningPod)
	}
}

func TestDefaultScheduler_OCSDegradationNone(t *testing.T) {
	// Setup: 2 cubes. cube-1 normal (used by high-priority victim). cube-2 degraded.
	// Request: tpu7x, 4x4x4, tolerance=none (default)
	// Expected: NOT schedulable (degraded filtered out, normal occupied by higher priority)
	state := testutil.NewStateBuilder().
		Pod("pod-a").Tree("4x4x4", "c1-normal").UsedBy("victim-hp", 255, interfaces.SliceSchedulerName).ParentPod().
		Pod("pod-b").Tree("4x4x4", "c9-degraded").Degraded("ocs1").ParentPod().Build()

	req := interfaces.GroupAllocationRequest{
		GroupName: "test-group",
		Slices: []interfaces.SliceRequirement{
			{SliceName: "slice1", Topology: "4x4x4", Type: TPU7x, DegradationTolerance: interfaces.DegradationToleranceNone},
		},
		Priority: 100,
	}

	s := createDegradationScheduler()
	result := s.Schedule(context.Background(), req, state)

	if result.IsSchedulable {
		t.Errorf("Policy Failure! strict none workload was incorrectly scheduled on degraded pod-b: %v", result.Candidate.Assignments["slice1"])
	}
}

func TestDefaultScheduler_SubCubeDegradationSink(t *testing.T) {
	// Setup: 2 cubes with children. One under normal root, one under degraded root.
	// Request: tpu7x, 2x2x1 (sub-cube), tolerance=all (implicit for sub-cube)
	// Expected: Schedulable, prefers degraded sub-cube (DegradationSinkScorer)
	state := testutil.NewStateBuilder().
		Pod("pod-a").
		Tree("4x4x4", "c1-normal").
		Child("2x2x2", "p1-normal-half").
		Child("2x2x1", "target-normal").ParentNode().ParentNode().ParentPod().
		Pod("pod-b").
		Tree("4x4x4", "c9-degraded").Degraded("ocs1").
		Child("2x2x2", "p1-degraded-half").
		Child("2x2x1", "target-degraded").End().Build()

	req := interfaces.GroupAllocationRequest{
		GroupName: "test-group",
		Slices: []interfaces.SliceRequirement{
			{SliceName: "slice1", Topology: "2x2x1", Type: TPU7x},
		},
		Priority: 100,
	}

	s := createDegradationScheduler()
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected scheduling to succeed, got failure: %s", result.FailureReason)
	}

	winningPod := result.Candidate.Assignments["slice1"][0].PodName
	if winningPod != "pod-b" {
		t.Errorf("Policy Failure! Sub-cube should have scheduled on degraded pod-b to sink onto degraded hardware, but allocated %s (normal free)", winningPod)
	}
}

func TestDefaultScheduler_SubCubeDegradationIgnoredNone(t *testing.T) {
	// Setup: 1 degraded cube with sub-cube children.
	// Request: tpu7x, 2x2x1 (sub-cube, <64 chips), tolerance=none
	// Expected: Schedulable — sub-cube bypasses ExcludeDegradedFilter (sub-cube < ChipsPerCube)
	state := testutil.NewStateBuilder().
		Pod("pod-a").
		Tree("4x4x4", "c1-degraded").Degraded("ocs1").
		Child("2x2x2", "p1-degraded-half").
		Child("2x2x1", "target-degraded").End().Build()

	req := interfaces.GroupAllocationRequest{
		GroupName: "test-group",
		Slices: []interfaces.SliceRequirement{
			{SliceName: "slice1", Topology: "2x2x1", Type: TPU7x, DegradationTolerance: interfaces.DegradationToleranceNone},
		},
		Priority: 100,
	}

	s := createDegradationScheduler()
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected scheduling to succeed for sub-cube under tolerance none, but failed: %s", result.FailureReason)
	}

	winningPod := result.Candidate.Assignments["slice1"][0].PodName
	if winningPod != "pod-a" {
		t.Errorf("Expected sub-cube to schedule inside pod-a (degraded cube), but allocated %s", winningPod)
	}
}

// createDegradationScheduler initializes a scheduler with degradation-aware plugins.
func createDegradationScheduler() *DefaultScheduler {
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 10}),
	}

	filters := []interfaces.FilterPlugin{
		&plugins.ExcludeDegradedFilter{},
		&plugins.HealthyFilter{},
		&plugins.ManagedFilter{},
		&plugins.PriorityFilter{},
	}

	scorers := []interfaces.ScorePlugin{
		&plugins.PreemptionScorer{},
		&plugins.DegradationSinkScorer{},
		&plugins.MinimizeSwappedCubesScorer{},
	}

	return NewDefaultScheduler(gens, filters, scorers)
}

func TestDefaultScheduler_Framework(t *testing.T) {
	tests := []struct {
		name       string
		generators []interfaces.PlacementGenerator
		scorers    []interfaces.ScorePlugin
		req        interfaces.GroupAllocationRequest
		state      interfaces.ClusterState
		want       simplifiedResult
	}{
		{
			name: "Success - Single Generator, No Plugins",
			generators: []interfaces.PlacementGenerator{
				&mockGenerator{
					candidates: []*interfaces.PlacementCandidate{
						{
							Assignments: map[string][]*interfaces.PartitionInfo{
								"test": {{ID: "p1", Topology: "4x4"}},
							},
							Victims: []*interfaces.SliceInfo{},
						},
					},
				},
			},
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "test"}},
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"p1"},
			},
		},
		{
			name: "Success - Highest Score Selected",
			generators: []interfaces.PlacementGenerator{
				&mockGenerator{
					candidates: []*interfaces.PlacementCandidate{
						{
							Assignments: map[string][]*interfaces.PartitionInfo{
								"test": {{ID: "p1", Topology: "4x4"}},
							},
							Victims: []*interfaces.SliceInfo{},
						},
						{
							Assignments: map[string][]*interfaces.PartitionInfo{
								"test": {{ID: "p2", Topology: "4x4"}},
							},
							Victims: []*interfaces.SliceInfo{},
						},
					},
				},
			},
			scorers: []interfaces.ScorePlugin{
				&mockScorer{scoreCost: mockCost(10)},
			},
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "test"}},
			},
			want: simplifiedResult{
				IsSchedulable:      true,
				AssignedPartitions: []string{"p1"}, // p1 is first
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scorers := tt.scorers
			if len(scorers) == 0 {
				scorers = []interfaces.ScorePlugin{&mockScorer{scoreCost: mockCost(0)}}
			}
			s := NewDefaultScheduler(tt.generators, nil, scorers)
			got := s.Schedule(context.Background(), tt.req, tt.state)

			sr := simplifyResult(got, tt.req.Slices[0].SliceName)

			if sr.IsSchedulable != tt.want.IsSchedulable {
				t.Errorf("IsSchedulable got %v, want %v", sr.IsSchedulable, tt.want.IsSchedulable)
			}

			sortSimplified(&sr)
			sortSimplified(&tt.want)

			if len(sr.AssignedPartitions) != len(tt.want.AssignedPartitions) {
				t.Errorf("AssignedPartitions count = %d %v, want %d %v", len(sr.AssignedPartitions), sr.AssignedPartitions, len(tt.want.AssignedPartitions), tt.want.AssignedPartitions)
			}
		})
	}
}

func TestDefaultScheduler_SwapMinimization(t *testing.T) {
	// Scenario: Swap Only One Cube On Failure (Multi-Cube Reschedule)
	// 4 cubes: c1, c2 free; c3 used by s1; c4 used by s1 but broken.
	// Without SwapCost, scheduler picks [c1, c2] (2 swaps).
	// With SwapCost, scheduler keeps c3 and picks [c1, c3] (1 swap).
	req := interfaces.GroupAllocationRequest{
		GroupName: "s1",
		Priority:  100,
		Slices: []interfaces.SliceRequirement{{
			SliceName:           "s1",
			Topology:            "4x4x8",
			Type:                TPU7x,
			CurrentPartitionIDs: []string{"c3", "c4"},
		}},
	}
	state := testutil.NewStateBuilder().
		Pod("pod-1").
		Tree("4x4x4", "c1").ParentPod().
		Tree("4x4x4", "c2").ParentPod().
		Tree("4x4x4", "c3").UsedBy("s1", 100, interfaces.SliceSchedulerName).ParentPod().
		Tree("4x4x4", "c4").UsedBy("s1", 100, interfaces.SliceSchedulerName).Broken().Build()

	s := createDefaultScheduler()
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected scheduling to succeed, got failure: %s", result.FailureReason)
	}

	assigned := result.Candidate.Assignments["s1"]
	var gotIDs []string
	for _, p := range assigned {
		gotIDs = append(gotIDs, p.ID)
	}
	sort.Strings(gotIDs)
	wantIDs := []string{"c1", "c3"}

	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("Got %d partition IDs %v, want %d %v", len(gotIDs), gotIDs, len(wantIDs), wantIDs)
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Errorf("Partition ID %d: got %q, want %q", i, gotIDs[i], wantIDs[i])
		}
	}
}

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
	"fmt"
	"sync"
	"testing"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/cost"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
)

type mockState struct {
	trees []*interfaces.PartitionInfo
}

func (m *mockState) GetPartitionTrees() []*interfaces.PartitionInfo {
	return m.trees
}

func (m *mockState) GetPartitionInfo(ids []string) []*interfaces.PartitionInfo {
	return nil
}

func TestPreemptionScorer(t *testing.T) {
	scorer := &PreemptionScorer{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test"}},
	}
	state := &mockState{}

	tests := []struct {
		name      string
		candidate *interfaces.PlacementCandidate
		wantZero  bool // true if we expect zero preemption cost
	}{
		{
			name: "No victims",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "p1", Topology: "4x4"}},
				},
				Victims: nil,
			},
			wantZero: true,
		},
		{
			name: "One victim",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "p1", Topology: "4x4"}},
				},
				Victims: []*interfaces.SliceInfo{{Name: "v1", Topology: "4x4", Priority: 10}},
			},
			wantZero: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scorer.Score(ctx, req, tt.candidate, state)
			pc, ok := got.(cost.PreemptionCost)
			if tt.wantZero {
				if ok && len(pc) > 0 {
					t.Errorf("PreemptionScorer.Score() expected zero cost, got %v", got)
				}
			} else {
				if !ok || len(pc) == 0 {
					t.Errorf("PreemptionScorer.Score() expected non-zero cost, got %v", got)
				}
			}
		})
	}
}

func TestTightestFitScorer(t *testing.T) {
	scorer := &TightestFitScorer{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test"}},
	}

	// Setup function to get a fresh tree for basic fit tests.
	getTree := func() *interfaces.PartitionInfo {
		return &interfaces.PartitionInfo{
			ID:       "root",
			Topology: "16x16",
			PodName:  "pod1",
			Children: []*interfaces.PartitionInfo{
				{
					ID:       "child1",
					Topology: "4x4",
					PodName:  "pod1",
					Children: []*interfaces.PartitionInfo{
						{
							ID:       "leaf1",
							Topology: "2x2",
							PodName:  "pod1",
						},
					},
				},
				{
					ID:       "child2",
					Topology: "4x4",
					PodName:  "pod1",
				},
			},
		}
	}

	tests := []struct {
		name      string
		candidate *interfaces.PlacementCandidate
		setupFunc func(*interfaces.PartitionInfo) *interfaces.PartitionInfo
		expected  cost.TightestFitCost
	}{
		{
			name: "Tighter fit (dirty 4x4)",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "leaf1", Topology: "2x2", PodName: "pod1"}},
				},
			},
			setupFunc: func(root *interfaces.PartitionInfo) *interfaces.PartitionInfo {
				// Mark child2 as broken to make root not "completely free".
				root.Children[1].IsBroken = true
				return root
			},
			// Primary Fit: child1 = 16 chips
			// Density: 0 free roots, 16 free chips -> 0*10000 + 16 = 16
			expected: cost.TightestFitCost{FitScore: 16, DensityScore: 16},
		},
		{
			name: "Looser fit (clean root)",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "leaf1", Topology: "2x2", PodName: "pod1"}},
				},
			},
			setupFunc: func(root *interfaces.PartitionInfo) *interfaces.PartitionInfo {
				return root
			},
			// Primary Fit: root = 256 chips
			// Density: 1 free root, 256 free chips -> 1*10000 + 256 = 10256
			expected: cost.TightestFitCost{FitScore: 256, DensityScore: 10256},
		},
		{
			name: "Multi-cube Ironwood tie-break (Pod B)",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "c1-p1-b", Topology: "4x4x4", PodName: "podB"}, {ID: "c1-p2-b", Topology: "4x4x4", PodName: "podB"}},
				},
			},
			setupFunc: func(root *interfaces.PartitionInfo) *interfaces.PartitionInfo {
				return root
			},
			// Pod B root is 4x4x8 (128 chips) with 2 children, all free
			// FitScore: min of each assigned partition's largest free ancestor = podB root (128 chips)
			// DensityScore: 1 free root (podB) * 10000 + 128 free chips = 10128
			expected: cost.TightestFitCost{FitScore: 128, DensityScore: 10128},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var state interfaces.ClusterState
			if tt.name == "Multi-cube Ironwood tie-break (Pod B)" {
				// Custom state for the multi-pod density tie-break scenario.
				podA := &interfaces.PartitionInfo{ID: "podA", Topology: "4x4x16", PodName: "podA", Children: []*interfaces.PartitionInfo{
					{ID: "c1-p1", Topology: "4x4x4", PodName: "podA"}, {ID: "c1-p2", Topology: "4x4x4", PodName: "podA"},
					{ID: "c1-p3", Topology: "4x4x4", PodName: "podA"}, {ID: "c1-p4", Topology: "4x4x4", PodName: "podA"},
				}}
				podB := &interfaces.PartitionInfo{ID: "podB", Topology: "4x4x8", PodName: "podB", Children: []*interfaces.PartitionInfo{
					{ID: "c1-p1-b", Topology: "4x4x4", PodName: "podB"}, {ID: "c1-p2-b", Topology: "4x4x4", PodName: "podB"},
				}}
				state = &mockState{trees: []*interfaces.PartitionInfo{podA, podB}}
			} else {
				root := tt.setupFunc(getTree())
				state = &mockState{trees: []*interfaces.PartitionInfo{root}}
			}

			got := scorer.Score(ctx, req, tt.candidate, state)
			tfc, ok := got.(cost.TightestFitCost)
			if !ok {
				t.Fatalf("Expected TightestFitCost, got %T", got)
			}
			if tfc.FitScore != tt.expected.FitScore {
				t.Errorf("FitScore = %d, want %d", tfc.FitScore, tt.expected.FitScore)
			}
			if tfc.DensityScore != tt.expected.DensityScore {
				t.Errorf("DensityScore = %d, want %d", tfc.DensityScore, tt.expected.DensityScore)
			}
		})
	}
}

// Occupancy is a CRD field the partition controller sets on interior
// partitions, not only on leaves. A 4x4 that is occupied as a unit, with
// descendants that carry no flag of their own, must not be reported free --
// otherwise the walk in Score widens largestFreeAncestor straight past it to
// the root, and the pod's density is counted as untouched.
func TestTightestFitScorer_InteriorOccupiedPartitionIsNotFree(t *testing.T) {
	scorer := &TightestFitScorer{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test"}},
	}

	root := &interfaces.PartitionInfo{
		ID: "root", Topology: "16x16", PodName: "pod1",
		Children: []*interfaces.PartitionInfo{
			{
				ID: "child1", Topology: "4x4", PodName: "pod1",
				Children: []*interfaces.PartitionInfo{
					{ID: "leaf1", Topology: "2x2", PodName: "pod1"},
				},
			},
			{
				// Occupied as a whole 4x4; its halves say nothing.
				ID: "child2", Topology: "4x4", PodName: "pod1", IsOccupied: true,
				Children: []*interfaces.PartitionInfo{
					{ID: "leaf2a", Topology: "2x2", PodName: "pod1"},
					{ID: "leaf2b", Topology: "2x2", PodName: "pod1"},
				},
			},
		},
	}
	state := &mockState{trees: []*interfaces.PartitionInfo{root}}
	candidate := &interfaces.PlacementCandidate{
		Assignments: map[string][]*interfaces.PartitionInfo{
			"test": {{ID: "leaf1", Topology: "2x2", PodName: "pod1"}},
		},
	}

	got, ok := scorer.Score(ctx, req, candidate, state).(cost.TightestFitCost)
	if !ok {
		t.Fatalf("Score returned %T, want cost.TightestFitCost", got)
	}

	// child2 blocks the walk at child1, so the fit is child1's 16 chips rather
	// than the root's 256.
	if want := 16; got.FitScore != want {
		t.Errorf("FitScore = %d, want %d", got.FitScore, want)
	}
	// child1's 16 free chips plus child2's two unflagged 2x2 halves. The pod has
	// no free root, so the freeRoots term is 0. Counting child2 as free would
	// give a free root and 256 chips: 10256.
	if want := 24; got.DensityScore != want {
		t.Errorf("DensityScore = %d, want %d", got.DensityScore, want)
	}
}

func TestCountFreeChips_StopsAtInUseSubtree(t *testing.T) {
	t.Run("held partition contributes nothing, descendants included", func(t *testing.T) {
		// PartitionWatcher.GetState stamps InUseBy on every descendant of a held
		// partition, so a production snapshot never has an unflagged child under a
		// flagged parent. The flag is set on the parent alone here to isolate the
		// early return: without it the walk descends and the two halves, which
		// carry no flag of their own, report themselves completely free.
		root := &interfaces.PartitionInfo{
			ID: "root", Topology: "4x4", InUseBy: &interfaces.SliceInfo{Name: "holder"},
			Children: []*interfaces.PartitionInfo{
				{ID: "leafA", Topology: "2x2"},
				{ID: "leafB", Topology: "2x2"},
			},
		}

		if got := countFreeChips(root); got != 0 {
			t.Errorf("countFreeChips(held root) = %d, want 0", got)
		}
	})

	t.Run("free subtrees beside a held one still count", func(t *testing.T) {
		// The early return must not swallow siblings of a held partition.
		root := &interfaces.PartitionInfo{
			ID: "root", Topology: "4x4",
			Children: []*interfaces.PartitionInfo{
				{ID: "held", Topology: "2x2", InUseBy: &interfaces.SliceInfo{Name: "holder"}},
				{ID: "free", Topology: "2x2"},
			},
		}

		if want, got := 4, countFreeChips(root); got != want {
			t.Errorf("countFreeChips(root) = %d, want %d", got, want)
		}
	})
}

func TestDegradationSinkScorer(t *testing.T) {
	scorer := &DegradationSinkScorer{}
	ctx := context.Background()

	tests := []struct {
		name     string
		req      interfaces.GroupAllocationRequest
		cand     *interfaces.PlacementCandidate
		state    interfaces.ClusterState
		wantCost cost.DegradationCost
	}{
		{
			name:     "No slices → zero cost",
			req:      interfaces.GroupAllocationRequest{},
			cand:     &interfaces.PlacementCandidate{},
			state:    &mockState{},
			wantCost: 0,
		},
		{
			name: "Non-tpu7x → zero cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "v6e", Topology: "4x4x4", DegradationTolerance: "all"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}},
			}},
			wantCost: 0,
		},
		{
			name: "tolerance=all, 1 normal partition → cost 1",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "all"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeNormal}},
			}},
			wantCost: 1,
		},
		{
			name: "tolerance=all, 1 degraded partition → cost 0",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "all"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}},
			}},
			wantCost: 0,
		},
		{
			name: "tolerance=prefer-none treated as none, full-cube → zero cost (not eligible)",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "4x4x8", DegradationTolerance: "prefer-none"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}, {ID: "p2", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}},
				{ID: "p2", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeNormal}},
			}},
			wantCost: 0,
		},
		{
			name: "tolerance=none, full-cube → zero cost (not eligible)",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "none"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}},
			}},
			wantCost: 0,
		},
		{
			name: "sub-cube + any tolerance → eligible, cost per normal partitions",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "2x2x4"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "2x2x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "2x2x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeNormal}},
			}},
			wantCost: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scorer.Score(ctx, tt.req, tt.cand, tt.state)
			dc, ok := got.(cost.DegradationCost)
			if !ok {
				t.Fatalf("Expected DegradationCost, got %T", got)
			}
			if dc != tt.wantCost {
				t.Errorf("DegradationSinkScorer.Score() = %d, want %d", dc, tt.wantCost)
			}
		})
	}
}

// degradedTree returns a 4x4x4 root over one 2x2x2 child, with the degradation
// state attached to whichever level the caller names. Cloud's watcher only
// populates DegradationState for State == Degraded, so a healthy level is nil
// rather than Mode: NORMAL.
func degradedTree(rootDegraded, childDegraded bool) *interfaces.PartitionInfo {
	degraded := func(on bool) *interfaces.DegradationState {
		if !on {
			return nil
		}
		return &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}
	}
	return &interfaces.PartitionInfo{
		ID: "root", Topology: "4x4x4", DegradationState: degraded(rootDegraded),
		Children: []*interfaces.PartitionInfo{
			{ID: "child", Topology: "2x2x2", DegradationState: degraded(childDegraded)},
		},
	}
}

// degradationReq scores one tolerant tpu7x slice holding the partition named by
// id, so the returned cost is 1 when that partition counts as normal and 0 when
// it counts as degraded.
func degradationReq(id string) (interfaces.GroupAllocationRequest, *interfaces.PlacementCandidate) {
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{
			SliceName: "s", Type: "tpu7x", Topology: "4x4x4",
			DegradationTolerance: interfaces.DegradationToleranceAll,
		}},
	}
	cand := &interfaces.PlacementCandidate{
		Assignments: map[string][]*interfaces.PartitionInfo{
			"s": {{ID: id, Topology: "2x2x2"}},
		},
	}
	return req, cand
}

// TestDegradationSinkScorer_ResolvesAgainstRootNotSelf pins the semantic the
// index has to preserve: degradation is a property of the OCS domain, which is
// the root cube, so a descendant is scored by its root and never by its own
// State. The existing table above only ever builds single-node trees, so
// nothing covered this before.
func TestDegradationSinkScorer_ResolvesAgainstRootNotSelf(t *testing.T) {
	ctx := context.Background()
	req, cand := degradationReq("child")

	tests := []struct {
		name                        string
		rootDegraded, childDegraded bool
		want                        cost.DegradationCost
	}{
		{"degraded root, unmarked child → child is degraded", true, false, 0},
		{"healthy root, degraded child → child is normal", false, true, 1},
		{"healthy root and child → normal", false, false, 1},
		{"degraded root and child → degraded", true, true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scorer := &DegradationSinkScorer{}
			state := &mockState{trees: []*interfaces.PartitionInfo{degradedTree(tt.rootDegraded, tt.childDegraded)}}
			got, ok := scorer.Score(ctx, req, cand, state).(cost.DegradationCost)
			if !ok {
				t.Fatalf("Score returned %T, want cost.DegradationCost", got)
			}
			if got != tt.want {
				t.Errorf("Score() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestDegradationSinkScorer_IndexFollowsTheSnapshot guards the scorer's index
// memoization. The index is keyed on ClusterState identity, so a scorer reused
// across scheduling cycles -- which is how cmd/main.go wires it, one instance
// for the process -- must not answer the next cycle from the last one's trees.
func TestDegradationSinkScorer_IndexFollowsTheSnapshot(t *testing.T) {
	ctx := context.Background()
	req, cand := degradationReq("child")
	scorer := &DegradationSinkScorer{}

	score := func(state interfaces.ClusterState) cost.DegradationCost {
		t.Helper()
		got, ok := scorer.Score(ctx, req, cand, state).(cost.DegradationCost)
		if !ok {
			t.Fatalf("Score returned %T, want cost.DegradationCost", got)
		}
		return got
	}

	degradedSnapshot := &mockState{trees: []*interfaces.PartitionInfo{degradedTree(true, false)}}
	healthySnapshot := &mockState{trees: []*interfaces.PartitionInfo{degradedTree(false, false)}}

	if got := score(degradedSnapshot); got != 0 {
		t.Fatalf("first cycle (degraded root) = %d, want 0", got)
	}
	if got := score(healthySnapshot); got != 1 {
		t.Errorf("second cycle (healthy root) = %d, want 1 -- stale index", got)
	}
	// Back to the first snapshot: a rebuild, not a resurrected entry.
	if got := score(degradedSnapshot); got != 0 {
		t.Errorf("third cycle (degraded root again) = %d, want 0", got)
	}
	// Repeat reads of one snapshot are served from the index and must agree.
	if got := score(degradedSnapshot); got != 0 {
		t.Errorf("cached read = %d, want 0", got)
	}
}

// TestDegradationSinkScorer_ConcurrentScore covers the lock the index needs.
// ScorePhase fans candidates out across goroutines against one scorer instance,
// so both the build and the read race unless guarded. Meaningful under -race.
func TestDegradationSinkScorer_ConcurrentScore(t *testing.T) {
	ctx := context.Background()
	req, cand := degradationReq("child")
	scorer := &DegradationSinkScorer{}

	snapshots := []interfaces.ClusterState{
		&mockState{trees: []*interfaces.PartitionInfo{degradedTree(true, false)}},
		&mockState{trees: []*interfaces.PartitionInfo{degradedTree(false, false)}},
	}
	want := []cost.DegradationCost{0, 1}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			idx := i % len(snapshots)
			if got := scorer.Score(ctx, req, cand, snapshots[idx]); got != want[idx] {
				t.Errorf("goroutine %d: Score() = %v, want %v", i, got, want[idx])
			}
		}(i)
	}
	wg.Wait()
}

// benchDegradationCluster builds trees roots deep enough to exercise the walk
// the index replaces: a 4x4x4 root over the 2x4x4 / 2x2x4 / 2x2x2 / 2x2x1
// ladder, 31 partitions per tree.
func benchDegradationCluster(roots int) *mockState {
	var build func(id string, topos []string) *interfaces.PartitionInfo
	build = func(id string, topos []string) *interfaces.PartitionInfo {
		node := &interfaces.PartitionInfo{ID: id, Topology: topos[0]}
		if len(topos) > 1 {
			for i := 0; i < 2; i++ {
				node.Children = append(node.Children, build(fmt.Sprintf("%s-%d", id, i), topos[1:]))
			}
		}
		return node
	}

	ladder := []string{"4x4x4", "2x4x4", "2x2x4", "2x2x2", "2x2x1"}
	trees := make([]*interfaces.PartitionInfo, roots)
	for r := range trees {
		root := build(fmt.Sprintf("cube%d", r), ladder)
		if r%4 == 0 {
			root.DegradationState = &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}
		}
		trees[r] = root
	}
	return &mockState{trees: trees}
}

// BenchmarkDegradationSinkScorer_Score measures the per-candidate cost of the
// root-degradation lookup over one snapshot, which is the shape of the real
// workload: a snapshot is built per scheduling cycle and then scored against
// every candidate the generator produced.
func BenchmarkDegradationSinkScorer_Score(b *testing.B) {
	ctx := context.Background()
	state := benchDegradationCluster(128)

	// A sub-cube slice assigned the deepest partition of the last cube, so the
	// scan this replaces pays close to its worst case.
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{
			SliceName: "s", Type: "tpu7x", Topology: "2x2x1",
			DegradationTolerance: interfaces.DegradationToleranceAll,
		}},
	}
	cand := &interfaces.PlacementCandidate{
		Assignments: map[string][]*interfaces.PartitionInfo{
			"s": {{ID: "cube127-1-1-1-1", Topology: "2x2x1"}},
		},
	}

	scorer := &DegradationSinkScorer{}
	b.ReportAllocs()
	for b.Loop() {
		scorer.Score(ctx, req, cand, state)
	}
}

func TestMinimizeSwappedCubesScorer_Score(t *testing.T) {
	testCases := []struct {
		name      string
		req       interfaces.GroupAllocationRequest
		candidate *interfaces.PlacementCandidate
		want      cost.SwapCost
	}{
		{
			name: "New slice (no current partitions) has 0 swap cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: nil},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p1"}, {ID: "p2"}},
				},
			},
			want: cost.SwapCost(0),
		},
		{
			name: "Rescheduling slice with identical partitions has 0 swap cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1", "p2"}},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p1"}, {ID: "p2"}},
				},
			},
			want: cost.SwapCost(0),
		},
		{
			name: "Rescheduling slice swapping 1 cube has 1 swap cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1", "p2"}},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p1"}, {ID: "p3"}},
				},
			},
			want: cost.SwapCost(1),
		},
		{
			name: "Rescheduling slice relocating completely has 2 swap cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1", "p2"}},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p3"}, {ID: "p4"}},
				},
			},
			want: cost.SwapCost(2),
		},
		{
			// The case that separates per-slice scoring from scoring against the
			// union of the group's current partitions. Both slices keep a
			// partition the group already holds, but they trade: slice-1 takes
			// slice-2's cube and vice versa. Under the union every assigned
			// partition is "current" and this scores 0, which would make an
			// unbindable shuffle the cheapest candidate in the request.
			name: "Slices trading partitions within the group costs 1 swap each",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1"}},
					{SliceName: "slice-2", CurrentPartitionIDs: []string{"p2"}},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p2"}},
					"slice-2": {{ID: "p1"}},
				},
			},
			want: cost.SwapCost(2),
		},
		{
			// Same group, no trade: each slice is handed back exactly what it
			// already had. Per-slice and union agree here, and must stay 0 so
			// rescheduling in place remains free.
			name: "Slices keeping their own partitions costs 0 swaps",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1"}},
					{SliceName: "slice-2", CurrentPartitionIDs: []string{"p2"}},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p1"}},
					"slice-2": {{ID: "p2"}},
				},
			},
			want: cost.SwapCost(0),
		},
		{
			// A new slice joining a group that already holds partitions must not
			// inherit its siblings' partitions as "free". slice-2 has no current
			// partitions so it is skipped entirely; slice-1 moving off p1 is the
			// only cost.
			name: "New sibling does not make a placed slice's move free",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1"}},
					{SliceName: "slice-2", CurrentPartitionIDs: nil},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p3"}},
					"slice-2": {{ID: "p1"}},
				},
			},
			want: cost.SwapCost(1),
		},
	}

	scorer := &MinimizeSwappedCubesScorer{}
	if scorer.Name() != "MinimizeSwappedCubesScorer" {
		t.Errorf("unexpected scorer name: %s", scorer.Name())
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := scorer.Score(context.Background(), tc.req, tc.candidate, nil)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Score() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestIsCompletelyFree_BlockedTaint(t *testing.T) {
	taint := func(key string, effect partitiontreev1.TaintEffect) []*partitiontreev1.PartitionTaint {
		return []*partitiontreev1.PartitionTaint{{Key: key, Effect: effect}}
	}

	tests := []struct {
		name string
		node *interfaces.PartitionInfo
		want bool
	}{
		{
			name: "untainted and unoccupied is free",
			node: &interfaces.PartitionInfo{ID: "p1", Topology: "4x4"},
			want: true,
		},
		{
			name: "blocked taint is not free",
			node: &interfaces.PartitionInfo{
				ID:       "p1",
				Topology: "4x4",
				Taints:   taint(corev1.TaintNodeUnschedulable, partitiontreev1.TaintEffectNoSchedule),
			},
			want: false,
		},
		{
			name: "PreferNoSchedule is a soft preference, still free",
			node: &interfaces.PartitionInfo{
				ID:       "p1",
				Topology: "4x4",
				Taints:   taint(interfaces.TaintKeyPartitionState, partitiontreev1.TaintEffectPreferNoSchedule),
			},
			want: true,
		},
		{
			name: "a tainted child makes the parent not free",
			node: &interfaces.PartitionInfo{
				ID:       "parent",
				Topology: "4x8",
				Children: []*interfaces.PartitionInfo{
					{ID: "c1", Topology: "4x4"},
					{
						ID:       "c2",
						Topology: "4x4",
						Taints:   taint(corev1.TaintNodeNotReady, partitiontreev1.TaintEffectNoExecute),
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCompletelyFree(tt.node); got != tt.want {
				t.Errorf("isCompletelyFree() = %v, want %v", got, tt.want)
			}
		})
	}
}

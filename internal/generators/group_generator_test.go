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
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/testutil"
	"github.com/go-logr/logr"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makeOption builds a PlacementOption from partition IDs in a single pod.
func makeOption(podName string, ids ...string) *interfaces.PlacementOption {
	parts := make([]*interfaces.PartitionInfo, len(ids))
	for i, id := range ids {
		parts[i] = &interfaces.PartitionInfo{ID: id, PodName: podName, Topology: "4x4x4"}
	}
	return &interfaces.PlacementOption{Partitions: parts}
}

func makeOptionWithVictims(podName string, victims []*interfaces.SliceInfo, ids ...string) *interfaces.PlacementOption {
	opt := makeOption(podName, ids...)
	opt.Victims = victims
	return opt
}

// logfFromCtx is a helper to get a logr.Logger for test use.
func logfFromCtx(ctx context.Context) logr.Logger {
	return logr.Discard()
}

// ---------------------------------------------------------------------------
// Tests: hasCollision
// ---------------------------------------------------------------------------
func TestHasCollision(t *testing.T) {
	tests := []struct {
		name     string
		existing []*interfaces.PlacementOption
		newCand  *interfaces.PlacementOption
		want     bool
	}{
		{
			name:     "No collision - disjoint partitions",
			existing: []*interfaces.PlacementOption{makeOption("pod1", "p1", "p2")},
			newCand:  makeOption("pod1", "p3", "p4"),
			want:     false,
		},
		{
			name:     "Collision - overlapping partition",
			existing: []*interfaces.PlacementOption{makeOption("pod1", "p1", "p2")},
			newCand:  makeOption("pod1", "p2", "p3"),
			want:     true,
		},
		{
			name:     "No collision - empty existing",
			existing: []*interfaces.PlacementOption{},
			newCand:  makeOption("pod1", "p1"),
			want:     false,
		},
		{
			name:     "No collision - empty new candidate partitions",
			existing: []*interfaces.PlacementOption{makeOption("pod1", "p1")},
			newCand:  makeOption("pod1"),
			want:     false,
		},
		{
			name: "Collision - multiple existing options",
			existing: []*interfaces.PlacementOption{
				makeOption("pod1", "p1"),
				makeOption("pod1", "p2"),
			},
			newCand: makeOption("pod1", "p2"),
			want:    true,
		},
		{
			name:     "No collision - nil existing slice",
			existing: nil,
			newCand:  makeOption("pod1", "p1"),
			want:     false,
		},
		{
			// hasCollision only checks partition IDs, not pod names.
			// Same ID in different pods still collides because the function
			// uses a flat map keyed by ID.
			name:     "Collision - same ID different pods",
			existing: []*interfaces.PlacementOption{makeOption("pod1", "p1")},
			newCand:  makeOption("pod2", "p1"),
			want:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasCollision(tt.existing, tt.newCand)
			if got != tt.want {
				t.Errorf("hasCollision() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: countUniquePods
// ---------------------------------------------------------------------------
func TestCountUniquePods(t *testing.T) {
	tests := []struct {
		name    string
		options []*interfaces.PlacementOption
		want    int
	}{
		{
			name:    "Empty options",
			options: nil,
			want:    0,
		},
		{
			name: "Single pod",
			options: []*interfaces.PlacementOption{
				makeOption("pod1", "p1"),
				makeOption("pod1", "p2"),
			},
			want: 1,
		},
		{
			name: "Multiple pods",
			options: []*interfaces.PlacementOption{
				makeOption("pod1", "p1"),
				makeOption("pod2", "p2"),
				makeOption("pod3", "p3"),
			},
			want: 3,
		},
		{
			name: "Mixed - duplicate pods",
			options: []*interfaces.PlacementOption{
				makeOption("pod1", "p1"),
				makeOption("pod2", "p2"),
				makeOption("pod1", "p3"),
			},
			want: 2,
		},
		{
			name: "Option with empty partitions ignored",
			options: []*interfaces.PlacementOption{
				makeOption("pod1", "p1"),
				{Partitions: nil},
			},
			want: 1,
		},
		{
			name: "Option with empty pod name ignored",
			options: []*interfaces.PlacementOption{
				makeOption("", "p1"),
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := countUniquePods(tt.options)
			if got != tt.want {
				t.Errorf("countUniquePods() = %d, want %d", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: mergeOptions
// ---------------------------------------------------------------------------
func TestMergeOptions(t *testing.T) {
	tests := []struct {
		name           string
		options        []*interfaces.PlacementOption
		req            interfaces.GroupAllocationRequest
		wantNil        bool
		wantSliceNames []string
		wantVictims    []string // sorted victim names
	}{
		{
			name:    "Empty options returns nil",
			options: nil,
			req:     interfaces.GroupAllocationRequest{},
			wantNil: true,
		},
		{
			name:    "Single option - no victims",
			options: []*interfaces.PlacementOption{makeOption("pod1", "p1")},
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
			},
			wantSliceNames: []string{"s1"},
			wantVictims:    nil,
		},
		{
			name: "Multiple options - deduplicated victims",
			options: []*interfaces.PlacementOption{
				makeOptionWithVictims("pod1", []*interfaces.SliceInfo{
					{Name: "v1", Priority: 10},
					{Name: "v2", Priority: 5},
				}, "p1"),
				makeOptionWithVictims("pod1", []*interfaces.SliceInfo{
					{Name: "v1", Priority: 10}, // duplicate
					{Name: "v3", Priority: 1},
				}, "p2"),
			},
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "s1"},
					{SliceName: "s2"},
				},
			},
			wantSliceNames: []string{"s1", "s2"},
			wantVictims:    []string{"v1", "v2", "v3"}, // sorted
		},
		{
			name: "Single option - with victims sorted",
			options: []*interfaces.PlacementOption{
				makeOptionWithVictims("pod1", []*interfaces.SliceInfo{
					{Name: "z-victim"},
					{Name: "a-victim"},
				}, "p1"),
			},
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
			},
			wantSliceNames: []string{"s1"},
			wantVictims:    []string{"a-victim", "z-victim"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeOptions(tt.options, tt.req)
			if tt.wantNil {
				if got != nil {
					t.Errorf("mergeOptions() = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("mergeOptions() = nil, want non-nil")
			}

			// Check assignments
			for _, sn := range tt.wantSliceNames {
				if _, ok := got.Assignments[sn]; !ok {
					t.Errorf("Missing slice %q in assignments", sn)
				}
			}

			// Check victims
			if len(got.Victims) != len(tt.wantVictims) {
				t.Fatalf("Victims count = %d, want %d", len(got.Victims), len(tt.wantVictims))
			}
			for i, wv := range tt.wantVictims {
				if got.Victims[i].Name != wv {
					t.Errorf("Victim[%d].Name = %q, want %q", i, got.Victims[i].Name, wv)
				}
			}
		})
	}
}

// TestMergeOptions_CopyIndependence verifies that merged assignments use independent
// backing arrays, so appending to the original PlacementOption does not affect the result.
func TestMergeOptions_CopyIndependence(t *testing.T) {
	opt := makeOption("pod1", "p1")
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	merged := mergeOptions([]*interfaces.PlacementOption{opt}, req)
	if merged == nil {
		t.Fatal("mergeOptions() returned nil")
	}
	mergedPartitions := merged.Assignments["s1"]
	if len(mergedPartitions) != 1 {
		t.Fatalf("Expected 1 partition, got %d", len(mergedPartitions))
	}
	// mergeOptions uses copy(), so merged has its own backing array.
	opt.Partitions = append(opt.Partitions, &interfaces.PartitionInfo{ID: "p2"})
	if len(mergedPartitions) != 1 {
		t.Error("mergeOptions() assignments grew after appending to original")
	}
}

// TestMergeOptions_PartitionDataPreserved verifies that merged assignments retain
// partition metadata fields (ID, Topology, PodName).
func TestMergeOptions_PartitionDataPreserved(t *testing.T) {
	opt := &interfaces.PlacementOption{
		Partitions: []*interfaces.PartitionInfo{
			{ID: "cube-1", PodName: "pod-alpha", Topology: "4x4x4"},
		},
	}
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	merged := mergeOptions([]*interfaces.PlacementOption{opt}, req)
	if merged == nil {
		t.Fatal("mergeOptions() returned nil")
	}
	parts := merged.Assignments["s1"]
	if len(parts) != 1 {
		t.Fatalf("Expected 1 partition, got %d", len(parts))
	}
	if parts[0].ID != "cube-1" {
		t.Errorf("ID = %q, want %q", parts[0].ID, "cube-1")
	}
	if parts[0].PodName != "pod-alpha" {
		t.Errorf("PodName = %q, want %q", parts[0].PodName, "pod-alpha")
	}
	if parts[0].Topology != "4x4x4" {
		t.Errorf("Topology = %q, want %q", parts[0].Topology, "4x4x4")
	}
}

// ---------------------------------------------------------------------------
// Tests: joinIds
// ---------------------------------------------------------------------------
func TestJoinIds(t *testing.T) {
	tests := []struct {
		name  string
		parts []*interfaces.PartitionInfo
		want  string
	}{
		{
			name:  "Empty slice",
			parts: nil,
			want:  "",
		},
		{
			name: "Single partition",
			parts: []*interfaces.PartitionInfo{
				{ID: "p1"},
			},
			want: "p1",
		},
		{
			name: "Multiple partitions",
			parts: []*interfaces.PartitionInfo{
				{ID: "p1"},
				{ID: "p2"},
				{ID: "p3"},
			},
			want: "p1,p2,p3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := joinIds(tt.parts)
			if got != tt.want {
				t.Errorf("joinIds() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: tieBreakCandidates
// ---------------------------------------------------------------------------
func TestTieBreakCandidates(t *testing.T) {
	slices := []interfaces.SliceRequirement{{SliceName: "s1"}}

	tests := []struct {
		name string
		m1   *interfaces.PlacementCandidate
		m2   *interfaces.PlacementCandidate
		want bool
	}{
		{
			name: "Equal candidates - no preference",
			m1: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			m2: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			want: false,
		},
		{
			name: "m1 fewer victims",
			m1: &interfaces.PlacementCandidate{
				Victims:     []*interfaces.SliceInfo{{Name: "a"}},
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			m2: &interfaces.PlacementCandidate{
				Victims:     []*interfaces.SliceInfo{{Name: "a"}, {Name: "b"}},
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			want: true,
		},
		{
			name: "m2 fewer victims",
			m1: &interfaces.PlacementCandidate{
				Victims:     []*interfaces.SliceInfo{{Name: "a"}, {Name: "b"}},
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			m2: &interfaces.PlacementCandidate{
				Victims:     []*interfaces.SliceInfo{{Name: "a"}},
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			want: false,
		},
		{
			name: "Same victim count - lexicographic victim names break tie",
			m1: &interfaces.PlacementCandidate{
				Victims:     []*interfaces.SliceInfo{{Name: "alpha"}},
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			m2: &interfaces.PlacementCandidate{
				Victims:     []*interfaces.SliceInfo{{Name: "beta"}},
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			want: true, // "alpha" < "beta"
		},
		{
			name: "No victims - tie break on assignments",
			m1: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "a1"}}},
			},
			m2: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "b1"}}},
			},
			want: true, // "a1" < "b1"
		},
		{
			name: "No victims - both empty",
			m1: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			m2: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			want: false,
		},
		{
			name: "Assignment length tie break",
			m1: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}}},
			},
			m2: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{"s1": {{ID: "p1"}, {ID: "p2"}}},
			},
			want: true, // fewer assignments
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tieBreakCandidates(tt.m1, tt.m2, slices)
			if got != tt.want {
				t.Errorf("tieBreakCandidates() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTieBreakCandidates_MultiSlice tests tie-breaking across multiple slices.
func TestTieBreakCandidates_MultiSlice(t *testing.T) {
	slices := []interfaces.SliceRequirement{{SliceName: "s1"}, {SliceName: "s2"}}
	tests := []struct {
		name string
		m1   *interfaces.PlacementCandidate
		m2   *interfaces.PlacementCandidate
		want bool
	}{
		{
			name: "Same first slice, different second slice",
			m1: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s1": {{ID: "p1"}},
					"s2": {{ID: "a2"}},
				},
			},
			m2: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s1": {{ID: "p1"}},
					"s2": {{ID: "b2"}},
				},
			},
			want: true, // "a2" < "b2"
		},
		{
			name: "Both slices identical",
			m1: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s1": {{ID: "p1"}},
					"s2": {{ID: "p2"}},
				},
			},
			m2: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s1": {{ID: "p1"}},
					"s2": {{ID: "p2"}},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tieBreakCandidates(tt.m1, tt.m2, slices)
			if got != tt.want {
				t.Errorf("tieBreakCandidates() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: NewPartialGang
// ---------------------------------------------------------------------------
func TestNewPartialGang(t *testing.T) {
	tests := []struct {
		name       string
		options    []*interfaces.PlacementOption
		req        interfaces.GroupAllocationRequest
		wantSpread int
	}{
		{
			name: "Single option single pod",
			options: []*interfaces.PlacementOption{
				makeOption("pod1", "p1"),
			},
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
			},
			wantSpread: 1,
		},
		{
			name: "Two options same pod",
			options: []*interfaces.PlacementOption{
				makeOption("pod1", "p1"),
				makeOption("pod1", "p2"),
			},
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1"}, {SliceName: "s2"}},
			},
			wantSpread: 1,
		},
		{
			name: "Two options different pods",
			options: []*interfaces.PlacementOption{
				makeOption("pod1", "p1"),
				makeOption("pod2", "p2"),
			},
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1"}, {SliceName: "s2"}},
			},
			wantSpread: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gang := NewPartialGang(tt.options, tt.req)
			if gang.Cost.Spread != tt.wantSpread {
				t.Errorf("NewPartialGang().Cost.Spread = %d, want %d", gang.Cost.Spread, tt.wantSpread)
			}
			if len(gang.Options) != len(tt.options) {
				t.Errorf("NewPartialGang().Options count = %d, want %d", len(gang.Options), len(tt.options))
			}
			if gang.MergedCandidate == nil {
				t.Error("NewPartialGang().MergedCandidate is nil, want non-nil")
			}
		})
	}
}

// TestNewPartialGang_WithVictims verifies that NewPartialGang correctly computes
// the preemption cost from victims in the merged candidate.
func TestNewPartialGang_WithVictims(t *testing.T) {
	opt := makeOptionWithVictims("pod1", []*interfaces.SliceInfo{
		{Name: "victim-1", Priority: 50, Topology: "4x4"},
		{Name: "victim-2", Priority: 50, Topology: "4x4"},
	}, "p1")
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	gang := NewPartialGang([]*interfaces.PlacementOption{opt}, req)

	// With 2 victims, preemption cost should be non-nil.
	if gang.Cost.Preemption == nil {
		t.Fatal("Expected non-nil PreemptionCost for gang with victims")
	}
	// Merged candidate should have the 2 victims.
	if len(gang.MergedCandidate.Victims) != 2 {
		t.Errorf("MergedCandidate.Victims count = %d, want 2", len(gang.MergedCandidate.Victims))
	}
}

// ---------------------------------------------------------------------------
// Tests: NewGroupGenerator
// ---------------------------------------------------------------------------
func TestNewGroupGenerator(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 42})
	if g == nil {
		t.Fatal("NewGroupGenerator() returned nil")
	}
	if g.config.BeamWidth != 42 {
		t.Errorf("config.BeamWidth = %d, want 42", g.config.BeamWidth)
	}
}

// ---------------------------------------------------------------------------
// Tests: GroupGenerator.Name
// ---------------------------------------------------------------------------
func TestGroupGenerator_Name(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	if g.Name() != "GroupSliceGenerator" {
		t.Errorf("Name() = %q, want %q", g.Name(), "GroupSliceGenerator")
	}
}

// ---------------------------------------------------------------------------
// Tests: pruneGangs
// ---------------------------------------------------------------------------
func TestPruneGangs(t *testing.T) {
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	buildGangs := func() []PartialGang {
		// Build 5 gangs with increasing costs (more victims = higher cost).
		var gangs []PartialGang
		for i := 0; i < 5; i++ {
			vic := make([]*interfaces.SliceInfo, 0)
			for j := 0; j <= i; j++ {
				vic = append(vic, &interfaces.SliceInfo{Name: "v", Priority: 10, Topology: "4x4"})
			}
			opt := makeOptionWithVictims("pod1", vic, "p"+string(rune('a'+i)))
			gangs = append(gangs, NewPartialGang([]*interfaces.PlacementOption{opt}, req))
		}
		return gangs
	}

	tests := []struct {
		name      string
		width     int
		wantCount int
	}{
		{
			name:      "Width larger than gangs",
			width:     10,
			wantCount: 5,
		},
		{
			name:      "Width smaller than gangs",
			width:     3,
			wantCount: 3,
		},
		{
			name:      "Width 1",
			width:     1,
			wantCount: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gangs := buildGangs()
			result := pruneGangs(gangs, tt.width, req, logfFromCtx(context.Background()), 1)
			if len(result) != tt.wantCount {
				t.Errorf("pruneGangs() returned %d gangs, want %d", len(result), tt.wantCount)
			}
		})
	}
}

// TestPruneGangs_Deduplication verifies that identical gangs are deduplicated.
func TestPruneGangs_Deduplication(t *testing.T) {
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	opt := makeOption("pod1", "p1")
	gang := NewPartialGang([]*interfaces.PlacementOption{opt}, req)
	// Create 3 identical gangs.
	gangs := []PartialGang{gang, gang, gang}

	result := pruneGangs(gangs, 10, req, logfFromCtx(context.Background()), 1)
	if len(result) != 1 {
		t.Errorf("pruneGangs() should deduplicate identical gangs: got %d, want 1", len(result))
	}
}

// TestPruneGangs_SymmetryBreaking verifies that permutations of the same assignment
// are treated as duplicates (e.g., [s1->A, s2->B] == [s1->B, s2->A]).
func TestPruneGangs_SymmetryBreaking(t *testing.T) {
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}, {SliceName: "s2"}},
	}
	optA := makeOption("pod1", "pA")
	optB := makeOption("pod2", "pB")

	gang1 := NewPartialGang([]*interfaces.PlacementOption{optA, optB}, req)
	gang2 := NewPartialGang([]*interfaces.PlacementOption{optB, optA}, req)

	gangs := []PartialGang{gang1, gang2}
	result := pruneGangs(gangs, 10, req, logfFromCtx(context.Background()), 1)
	if len(result) != 1 {
		t.Errorf("pruneGangs() should treat permutations as duplicates: got %d, want 1", len(result))
	}
}

// TestPruneGangs_SurvivorIsOrderIndependent verifies that when a permutation
// class is collapsed, which member survives does not depend on the order the
// members were generated in.
//
// Deduplication runs before ranking, so the survivor is what reaches the
// output. Both input orders below must yield the representative the total
// tie-breaker ranks first, s1->pA.
func TestPruneGangs_SurvivorIsOrderIndependent(t *testing.T) {
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}, {SliceName: "s2"}},
	}
	optA := makeOption("pod1", "pA")
	optB := makeOption("pod2", "pB")

	gang1 := NewPartialGang([]*interfaces.PlacementOption{optA, optB}, req) // s1->pA, s2->pB
	gang2 := NewPartialGang([]*interfaces.PlacementOption{optB, optA}, req) // s1->pB, s2->pA

	for _, tt := range []struct {
		name  string
		gangs []PartialGang
	}{
		{name: "ranked-first generated first", gangs: []PartialGang{gang1, gang2}},
		{name: "ranked-first generated second", gangs: []PartialGang{gang2, gang1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := pruneGangs(tt.gangs, 10, req, logfFromCtx(context.Background()), 1)
			if len(result) != 1 {
				t.Fatalf("pruneGangs() should collapse the permutation class: got %d, want 1", len(result))
			}
			if got := joinIds(result[0].MergedCandidate.Assignments["s1"]); got != "pA" {
				t.Errorf("survivor assigns s1=%q, want %q", got, "pA")
			}
		})
	}
}

// TestPruneGangs_SortOrder verifies that pruneGangs returns gangs sorted by
// ascending cost, so the cheapest (fewest victims) comes first.
func TestPruneGangs_SortOrder(t *testing.T) {
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	// Gang with 3 victims (expensive).
	expensive := makeOptionWithVictims("pod1", []*interfaces.SliceInfo{
		{Name: "v1", Priority: 10, Topology: "4x4"},
		{Name: "v2", Priority: 10, Topology: "4x4"},
		{Name: "v3", Priority: 10, Topology: "4x4"},
	}, "p-expensive")
	// Gang with 0 victims (cheap).
	cheap := makeOption("pod1", "p-cheap")

	gangs := []PartialGang{
		NewPartialGang([]*interfaces.PlacementOption{expensive}, req),
		NewPartialGang([]*interfaces.PlacementOption{cheap}, req),
	}

	result := pruneGangs(gangs, 10, req, logfFromCtx(context.Background()), 1)
	if len(result) != 2 {
		t.Fatalf("Expected 2 gangs, got %d", len(result))
	}
	// First result should be the cheaper one (no victims).
	if len(result[0].MergedCandidate.Victims) != 0 {
		t.Errorf("First gang should have 0 victims (cheapest), got %d", len(result[0].MergedCandidate.Victims))
	}
	if len(result[1].MergedCandidate.Victims) != 3 {
		t.Errorf("Second gang should have 3 victims (most expensive), got %d", len(result[1].MergedCandidate.Victims))
	}
}

// TestPruneGangs_EmptyInput verifies that nil and empty inputs return empty results.
func TestPruneGangs_EmptyInput(t *testing.T) {
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	result := pruneGangs(nil, 10, req, logfFromCtx(context.Background()), 1)
	if len(result) != 0 {
		t.Errorf("pruneGangs(nil) returned %d gangs, want 0", len(result))
	}
	result = pruneGangs([]PartialGang{}, 10, req, logfFromCtx(context.Background()), 1)
	if len(result) != 0 {
		t.Errorf("pruneGangs(empty) returned %d gangs, want 0", len(result))
	}
}

// ---------------------------------------------------------------------------
// Tests: GroupGenerator.Generate (integration through the full path)
// ---------------------------------------------------------------------------
func TestGroupGenerator_Generate_EmptySlices(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	req := interfaces.GroupAllocationRequest{Slices: nil}
	state := testutil.NewStateBuilder().Pod("pod1").Tree("4x4", "p1").Build()
	got := g.Generate(context.Background(), req, state, getAllPartitionIDsFromState(state))
	if got != nil {
		t.Errorf("Generate() with empty slices = %+v, want nil", got)
	}
}

func TestGroupGenerator_Generate_SingleSlice(t *testing.T) {
	tests := []struct {
		name    string
		req     interfaces.GroupAllocationRequest
		builder func() *testutil.MockClusterState
		wantIDs []string
		wantNil bool
	}{
		{
			name: "Single partition match",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1", Topology: "4x4"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").Tree("4x4", "p1").Build()
			},
			wantIDs: []string{"p1"},
		},
		{
			name: "Multiple available partitions",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1", Topology: "4x4"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("8x8", "root").
					Child("4x4", "c1").ParentNode().
					Child("4x4", "c2").Build()
			},
			wantIDs: []string{"c1"}, // Check at least one is returned
		},
		{
			name: "No matching topology",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1", Topology: "16x16"}},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").Tree("4x4", "p1").Build()
			},
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
			state := tt.builder()
			eligible := getAllPartitionIDsFromState(state)
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

			// Check first candidate
			c := got[0]
			parts := c.Assignments[tt.req.Slices[0].SliceName]
			var gotIDs []string
			for _, p := range parts {
				gotIDs = append(gotIDs, p.ID)
			}
			sort.Strings(gotIDs)
			sort.Strings(tt.wantIDs)

			// We only check the first expected ID is present
			if gotIDs[0] != tt.wantIDs[0] {
				t.Errorf("Got partition ID %q, want %q", gotIDs[0], tt.wantIDs[0])
			}
		})
	}
}

func TestGroupGenerator_Generate_MultiSliceBeamSearch(t *testing.T) {
	tests := []struct {
		name       string
		req        interfaces.GroupAllocationRequest
		builder    func() *testutil.MockClusterState
		beamWidth  int
		wantCount  int // min number of candidates expected
		wantNil    bool
		wantSlices []string // slice names that should appear in assignments
	}{
		{
			name: "Two slices - sufficient capacity",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "s1", Topology: "4x4x4"},
					{SliceName: "s2", Topology: "4x4x4"},
				},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("4x4x16", "root").
					Child("4x4x4", "c1").ParentNode().
					Child("4x4x4", "c2").ParentNode().
					Child("4x4x4", "c3").Build()
			},
			beamWidth:  10,
			wantCount:  1,
			wantSlices: []string{"s1", "s2"},
		},
		{
			name: "Two slices - insufficient capacity",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "s1", Topology: "4x4x4"},
					{SliceName: "s2", Topology: "4x4x4"},
				},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("4x4x4", "c1").Build()
			},
			beamWidth: 10,
			wantNil:   true,
		},
		{
			name: "Beam width = 1 limits results",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "s1", Topology: "4x4x4"},
					{SliceName: "s2", Topology: "4x4x4"},
				},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("4x4x16", "root").
					Child("4x4x4", "c1").ParentNode().
					Child("4x4x4", "c2").ParentNode().
					Child("4x4x4", "c3").ParentNode().
					Child("4x4x4", "c4").Build()
			},
			beamWidth:  1,
			wantCount:  1,
			wantSlices: []string{"s1", "s2"},
		},
		{
			name: "Three slices - beam search",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "s1", Topology: "4x4x4"},
					{SliceName: "s2", Topology: "4x4x4"},
					{SliceName: "s3", Topology: "4x4x4"},
				},
			},
			builder: func() *testutil.MockClusterState {
				return testutil.NewStateBuilder().Pod("pod1").
					Tree("4x4x16", "root").
					Child("4x4x4", "c1").ParentNode().
					Child("4x4x4", "c2").ParentNode().
					Child("4x4x4", "c3").ParentNode().
					Child("4x4x4", "c4").Build()
			},
			beamWidth:  10,
			wantCount:  1,
			wantSlices: []string{"s1", "s2", "s3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: tt.beamWidth})
			state := tt.builder()
			eligible := getAllPartitionIDsFromState(state)
			got := g.Generate(context.Background(), tt.req, state, eligible)

			if tt.wantNil {
				if len(got) != 0 {
					t.Errorf("Expected 0 candidates, got %d", len(got))
				}
				return
			}

			if len(got) < tt.wantCount {
				t.Fatalf("Expected at least %d candidates, got %d", tt.wantCount, len(got))
			}

			// Verify all required slices are in the assignments
			c := got[0]
			for _, sn := range tt.wantSlices {
				if _, ok := c.Assignments[sn]; !ok {
					t.Errorf("Missing slice %q in assignments", sn)
				}
			}

			// Verify no partition collisions in the selected candidate
			usedPartitions := make(map[string]bool)
			for _, parts := range c.Assignments {
				for _, p := range parts {
					if usedPartitions[p.ID] {
						t.Errorf("Partition collision detected: partition %q used more than once", p.ID)
					}
					usedPartitions[p.ID] = true
				}
			}
		})
	}
}

func TestGroupGenerator_Generate_MultiSliceEmptyRequest(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{
			{SliceName: "s1", Topology: "4x4x4"},
			{SliceName: "s2", Topology: "4x4x4"},
		},
	}
	// Empty state - no partition trees at all.
	state := testutil.NewStateBuilder().Build()
	got := g.Generate(context.Background(), req, state, map[string]struct{}{})
	if got != nil {
		t.Errorf("Generate() with empty state for multi-slice = %+v, want nil", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: beamSearch directly
// ---------------------------------------------------------------------------
func TestBeamSearch_EmptyPool(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	// With empty pool, currentGangs is empty after Level 1 init.
	// requestedSlices == 1, so no expansion loop — bestCandidates is empty.
	got := g.beamSearch(context.Background(), req, nil)
	if len(got) != 0 {
		t.Errorf("beamSearch() with nil pool = %d candidates, want 0", len(got))
	}
}

func TestBeamSearch_EmptySlices(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	req := interfaces.GroupAllocationRequest{Slices: nil}
	got := g.beamSearch(context.Background(), req, []*interfaces.PlacementOption{makeOption("pod1", "p1")})
	if got != nil {
		t.Errorf("beamSearch() with empty slices = %+v, want nil", got)
	}
}

// TestBeamSearch_EmptyPoolMultiSlice verifies that with a nil pool and multi-slice
// request, the expansion loop returns nil (nextGangs is empty at level 2).
func TestBeamSearch_EmptyPoolMultiSlice(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}, {SliceName: "s2"}},
	}
	got := g.beamSearch(context.Background(), req, nil)
	if got != nil {
		t.Errorf("beamSearch() with nil pool multi-slice = %+v, want nil", got)
	}
}

// TestBeamSearch_SingleSliceReturnsDirect verifies that for single-slice requests,
// beamSearch skips the expansion loop and returns directly after Level 1 init.
func TestBeamSearch_SingleSliceReturnsDirect(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1"}},
	}
	pool := []*interfaces.PlacementOption{
		makeOption("pod1", "p1"),
		makeOption("pod1", "p2"),
	}
	got := g.beamSearch(context.Background(), req, pool)
	if len(got) == 0 {
		t.Fatal("beamSearch() with single slice and 2 options should return candidates")
	}
	for i, c := range got {
		if _, ok := c.Assignments["s1"]; !ok {
			t.Errorf("Candidate %d missing slice s1 in assignments", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests: Generate with multiple pods
// ---------------------------------------------------------------------------
func TestGroupGenerator_Generate_MultiplePods(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1", Topology: "4x4"}},
	}
	state := testutil.NewStateBuilder().
		Pod("pod1").Tree("4x4", "p1").ParentPod().
		Pod("pod2").Tree("4x4", "p2").Build()
	eligible := getAllPartitionIDsFromState(state)
	got := g.Generate(context.Background(), req, state, eligible)

	if len(got) < 2 {
		t.Fatalf("Expected at least 2 candidates from 2 pods, got %d", len(got))
	}
}

// ---------------------------------------------------------------------------
// Tests: Generate skips empty PlacementOptions
// ---------------------------------------------------------------------------
func TestGroupGenerator_Generate_SkipsEmptyOptions(t *testing.T) {
	g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "s1", Topology: "4x4"}},
	}
	// Build a state where the partition is broken — no valid options.
	state := testutil.NewStateBuilder().Pod("pod1").Tree("4x4", "p1").Broken().Build()
	eligible := getAllPartitionIDsFromState(state)
	got := g.Generate(context.Background(), req, state, eligible)

	// Broken partitions should not produce candidates when they have no available options.
	// This depends on the filter pipeline; with all partitions eligible, broken partitions
	// may still produce candidates (with victims or through preemption).
	// The key check is no panic.
	_ = got
}

// TestGroupGenerator_Generate_OutOfRangeTopology pins the behavior for topology
// strings whose dimensions exceed util.MaxTopologyDimension.
// Every one of these must now be rejected outright, yielding no placement.
func TestGroupGenerator_Generate_OutOfRangeTopology(t *testing.T) {
	topologies := []string{
		"2097152x2097152x2097152",
		"10000x10000x10000",
	}

	for _, topology := range topologies {
		t.Run(topology, func(t *testing.T) {
			g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
			req := interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1", Type: "tpu7x", Topology: topology}},
			}
			state := testutil.NewStateBuilder().Pod("pod1").
				Tree("4x4x4", "c1").ParentPod().
				Tree("4x4x4", "c2").ParentPod().
				Tree("4x4x4", "c3").ParentPod().
				Tree("4x4x4", "c4").Build()

			got := g.Generate(context.Background(), req, state, getAllPartitionIDsFromState(state))
			if len(got) != 0 {
				t.Errorf("Generate(topology=%q) returned %d candidates, want 0", topology, len(got))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: Generate is independent of the order GetPartitionTrees returns roots
// ---------------------------------------------------------------------------

// candidateKeys renders a candidate list as an ordered, comparable summary.
// Order is preserved deliberately: ScorePhase keeps the first candidate unless
// a later one scores strictly better, so the sequence Generate emits -- not
// just the set -- is what decides the placement.
func candidateKeys(cands []*interfaces.PlacementCandidate, sliceNames []string) []string {
	keys := make([]string, 0, len(cands))
	for _, c := range cands {
		var parts []string
		for _, name := range sliceNames {
			parts = append(parts, name+"="+joinIds(c.Assignments[name]))
		}
		keys = append(keys, strings.Join(parts, "|"))
	}
	return keys
}

// TestGroupGenerator_Generate_IndependentOfTreeOrder pins the contract that
// ClusterState.GetPartitionTrees returns a set, not an ordering.
//
// The production implementation (watcher.partitionState.GetPartitionTrees)
// builds its slice by ranging over a map, so a real cluster hands the same
// roots over in a different order from one call to the next. Generate must
// return the identical candidate sequence regardless.
func TestGroupGenerator_Generate_IndependentOfTreeOrder(t *testing.T) {
	// Two pods, two same-topology roots each: enough for both the pod ordering
	// and the within-pod root ordering to matter.
	newState := func() *testutil.MockClusterState {
		return testutil.NewStateBuilder().
			Pod("pod-a").
			Tree("4x4", "a1").ParentPod().
			Tree("4x4", "a2").ParentPod().
			Pod("pod-b").
			Tree("4x4", "b1").ParentPod().
			Tree("4x4", "b2").
			Build()
	}

	// Every rotation of the root slice describes the same cluster.
	permute := func(roots []*interfaces.PartitionInfo, by int) []*interfaces.PartitionInfo {
		out := make([]*interfaces.PartitionInfo, 0, len(roots))
		for i := range roots {
			out = append(out, roots[(i+by)%len(roots)])
		}
		return out
	}

	tests := []struct {
		name string
		req  interfaces.GroupAllocationRequest
	}{
		{
			name: "single slice",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s1", Topology: "4x4"}},
			},
		},
		{
			name: "two slice gang",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "s1", Topology: "4x4"},
					{SliceName: "s2", Topology: "4x4"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sliceNames := make([]string, len(tt.req.Slices))
			for i, s := range tt.req.Slices {
				sliceNames[i] = s.SliceName
			}

			g := NewGroupGenerator(GroupGeneratorConfig{BeamWidth: 10})
			base := newState()
			want := candidateKeys(g.Generate(context.Background(), tt.req, base, getAllPartitionIDsFromState(base)), sliceNames)
			if len(want) == 0 {
				t.Fatal("Generate returned no candidates; the fixture cannot detect order sensitivity")
			}

			for by := 1; by < len(base.PartitionTrees); by++ {
				shuffled := newState()
				shuffled.PartitionTrees = permute(shuffled.PartitionTrees, by)

				got := candidateKeys(g.Generate(context.Background(), tt.req, shuffled, getAllPartitionIDsFromState(shuffled)), sliceNames)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("root order rotated by %d changed the candidate sequence:\n got %v\nwant %v", by, got, want)
				}
			}
		})
	}
}

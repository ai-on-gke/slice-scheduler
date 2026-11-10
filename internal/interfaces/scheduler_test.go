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

package interfaces

import (
	"testing"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
)

func TestSliceInfo_DeepCopy(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var s *SliceInfo
		got := s.DeepCopy()
		if got != nil {
			t.Errorf("DeepCopy(nil) = %v, want nil", got)
		}
	})

	t.Run("mutation of copy does not affect original", func(t *testing.T) {
		orig := &SliceInfo{
			Name:         "slice-1",
			Priority:     10,
			Topology:     "4x4",
			Type:         "v6e",
			PartitionIds: []string{"p1", "p2"},
			ManagedBy:    "slice-scheduler",
		}

		copied := orig.DeepCopy()

		// Mutate the copy
		copied.Name = "mutated"
		copied.Priority = 999
		copied.PartitionIds[0] = "mutated-p"
		copied.PartitionIds = append(copied.PartitionIds, "p3")

		// Original must be unchanged
		if orig.Name != "slice-1" {
			t.Errorf("original Name mutated: got %q", orig.Name)
		}
		if orig.Priority != 10 {
			t.Errorf("original Priority mutated: got %d", orig.Priority)
		}
		if len(orig.PartitionIds) != 2 {
			t.Errorf("original PartitionIds length mutated: got %d", len(orig.PartitionIds))
		}
		if orig.PartitionIds[0] != "p1" {
			t.Errorf("original PartitionIds[0] mutated: got %q", orig.PartitionIds[0])
		}
	})

	t.Run("nil PartitionIds preserved", func(t *testing.T) {
		orig := &SliceInfo{Name: "no-partitions"}
		copied := orig.DeepCopy()
		if copied.PartitionIds != nil {
			t.Errorf("copied PartitionIds should be nil, got %v", copied.PartitionIds)
		}
	})
}

func TestSliceInfo_SameSlice(t *testing.T) {
	tests := []struct {
		name string
		a, b *SliceInfo
		want bool
	}{
		{"same name and UID", &SliceInfo{Name: "s", UID: "u1"}, &SliceInfo{Name: "s", UID: "u1"}, true},
		{"same name, different UID", &SliceInfo{Name: "s", UID: "u1"}, &SliceInfo{Name: "s", UID: "u2"}, false},
		{"different name", &SliceInfo{Name: "s", UID: "u1"}, &SliceInfo{Name: "t", UID: "u1"}, false},
		{"UID unknown on one side", &SliceInfo{Name: "s", UID: "u1"}, &SliceInfo{Name: "s"}, true},
		{"UID unknown on both sides", &SliceInfo{Name: "s"}, &SliceInfo{Name: "s"}, true},
		{"nil", &SliceInfo{Name: "s"}, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.SameSlice(tc.b); got != tc.want {
				t.Errorf("SameSlice() = %v, want %v", got, tc.want)
			}
			if got := tc.b.SameSlice(tc.a); got != tc.want {
				t.Errorf("SameSlice() reversed = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDegradationState_DeepCopy(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var d *DegradationState
		got := d.DeepCopy()
		if got != nil {
			t.Errorf("DeepCopy(nil) = %v, want nil", got)
		}
	})

	t.Run("copy is independent", func(t *testing.T) {
		orig := &DegradationState{Mode: "DEGRADED", ToleratedOcsName: "ocs-1"}
		copied := orig.DeepCopy()
		copied.Mode = "NORMAL"
		if orig.Mode != "DEGRADED" {
			t.Errorf("original Mode mutated: got %q", orig.Mode)
		}
	})
}

func TestPartitionInfo_DeepCopy(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var p *PartitionInfo
		got := p.DeepCopy()
		if got != nil {
			t.Errorf("DeepCopy(nil) = %v, want nil", got)
		}
	})

	t.Run("full tree deep copy", func(t *testing.T) {
		orig := &PartitionInfo{
			ID:          "root",
			Topology:    "4x4x4",
			IsAvailable: true,
			PodName:     "pod-1",
			InUseBy: &SliceInfo{
				Name:         "slice-a",
				PartitionIds: []string{"root"},
			},
			DegradationState: &DegradationState{Mode: "NORMAL"},
			Children: []*PartitionInfo{
				{
					ID:       "child-1",
					Topology: "4x4x2",
					Children: []*PartitionInfo{
						{ID: "leaf-1", Topology: "4x4"},
					},
				},
			},
		}

		copied := orig.DeepCopy()

		// Mutate all levels of the copy
		copied.ID = "mutated-root"
		copied.InUseBy.Name = "mutated-slice"
		copied.InUseBy.PartitionIds[0] = "mutated"
		copied.DegradationState.Mode = "DEGRADED"
		copied.Children[0].ID = "mutated-child"
		copied.Children[0].Children[0].ID = "mutated-leaf"

		// Verify original is untouched
		if orig.ID != "root" {
			t.Errorf("original ID mutated: got %q", orig.ID)
		}
		if orig.InUseBy.Name != "slice-a" {
			t.Errorf("original InUseBy.Name mutated: got %q", orig.InUseBy.Name)
		}
		if orig.InUseBy.PartitionIds[0] != "root" {
			t.Errorf("original InUseBy.PartitionIds[0] mutated: got %q", orig.InUseBy.PartitionIds[0])
		}
		if orig.DegradationState.Mode != "NORMAL" {
			t.Errorf("original DegradationState.Mode mutated: got %q", orig.DegradationState.Mode)
		}
		if orig.Children[0].ID != "child-1" {
			t.Errorf("original Children[0].ID mutated: got %q", orig.Children[0].ID)
		}
		if orig.Children[0].Children[0].ID != "leaf-1" {
			t.Errorf("original leaf ID mutated: got %q", orig.Children[0].Children[0].ID)
		}
	})

	t.Run("nil InUseBy and DegradationState", func(t *testing.T) {
		orig := &PartitionInfo{ID: "simple"}
		copied := orig.DeepCopy()
		if copied.InUseBy != nil {
			t.Errorf("expected nil InUseBy, got %v", copied.InUseBy)
		}
		if copied.DegradationState != nil {
			t.Errorf("expected nil DegradationState, got %v", copied.DegradationState)
		}
	})
}

func TestGroupAllocationRequest_DeepCopy(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var g *GroupAllocationRequest
		got := g.DeepCopy()
		if got != nil {
			t.Errorf("DeepCopy(nil) = %v, want nil", got)
		}
	})

	t.Run("slice requirements isolated", func(t *testing.T) {
		orig := &GroupAllocationRequest{
			GroupName: "group-1",
			Priority:  100,
			Slices: []SliceRequirement{
				{SliceName: "s1", Type: "v6e", Topology: "4x4"},
				{SliceName: "s2", Type: "tpu7x", Topology: "2x2x2"},
			},
		}

		copied := orig.DeepCopy()
		copied.GroupName = "mutated"
		copied.Slices[0].SliceName = "mutated-s1"
		copied.Slices = append(copied.Slices, SliceRequirement{SliceName: "s3"})

		if orig.GroupName != "group-1" {
			t.Errorf("original GroupName mutated: got %q", orig.GroupName)
		}
		if orig.Slices[0].SliceName != "s1" {
			t.Errorf("original Slices[0].SliceName mutated: got %q", orig.Slices[0].SliceName)
		}
		if len(orig.Slices) != 2 {
			t.Errorf("original Slices length mutated: got %d", len(orig.Slices))
		}
	})
}

func TestPlacementCandidate_DeepCopy(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var p *PlacementCandidate
		got := p.DeepCopy()
		if got != nil {
			t.Errorf("DeepCopy(nil) = %v, want nil", got)
		}
	})

	t.Run("assignments and victims isolated", func(t *testing.T) {
		orig := &PlacementCandidate{
			Assignments: map[string][]*PartitionInfo{
				"slice-1": {
					{ID: "p1", PodName: "pod-a"},
					{ID: "p2", PodName: "pod-a"},
				},
			},
			Victims: []*SliceInfo{
				{Name: "victim-1", PartitionIds: []string{"p3"}},
			},
		}

		copied := orig.DeepCopy()

		// Mutate copy's assignments
		copied.Assignments["slice-1"][0].ID = "mutated"
		copied.Assignments["slice-2"] = []*PartitionInfo{{ID: "new"}}

		// Mutate copy's victims
		copied.Victims[0].Name = "mutated-victim"
		copied.Victims[0].PartitionIds[0] = "mutated-p"

		// Verify original is untouched
		if orig.Assignments["slice-1"][0].ID != "p1" {
			t.Errorf("original assignment ID mutated: got %q", orig.Assignments["slice-1"][0].ID)
		}
		if _, ok := orig.Assignments["slice-2"]; ok {
			t.Error("original assignments gained a new key from copy mutation")
		}
		if orig.Victims[0].Name != "victim-1" {
			t.Errorf("original victim Name mutated: got %q", orig.Victims[0].Name)
		}
		if orig.Victims[0].PartitionIds[0] != "p3" {
			t.Errorf("original victim PartitionIds mutated: got %q", orig.Victims[0].PartitionIds[0])
		}
	})

	t.Run("nil assignments and victims", func(t *testing.T) {
		orig := &PlacementCandidate{}
		copied := orig.DeepCopy()
		if copied.Assignments != nil {
			t.Errorf("expected nil Assignments, got %v", copied.Assignments)
		}
		if copied.Victims != nil {
			t.Errorf("expected nil Victims, got %v", copied.Victims)
		}
	})
}

func taint(key string, effect partitiontreev1.TaintEffect) *partitiontreev1.PartitionTaint {
	return &partitiontreev1.PartitionTaint{Key: key, Effect: effect}
}

func TestHasBlockedTaint(t *testing.T) {
	tests := []struct {
		description string
		taints      []*partitiontreev1.PartitionTaint
		want        bool
	}{
		{
			description: "no taints",
			want:        false,
		},
		{
			description: "nil taint",
			taints:      []*partitiontreev1.PartitionTaint{nil},
			want:        false,
		},
		{
			description: "cordoned node",
			taints:      []*partitiontreev1.PartitionTaint{taint("node.kubernetes.io/unschedulable", partitiontreev1.TaintEffectNoSchedule)},
			want:        true,
		},
		{
			description: "incomplete partition",
			taints:      []*partitiontreev1.PartitionTaint{taint(TaintKeyPartitionState, partitiontreev1.TaintEffectNoSchedule)},
			want:        true,
		},
		{
			description: "degraded partition is advisory",
			taints:      []*partitiontreev1.PartitionTaint{taint(TaintKeyPartitionState, partitiontreev1.TaintEffectPreferNoSchedule)},
			want:        false,
		},
		{
			description: "any key with NoSchedule",
			taints:      []*partitiontreev1.PartitionTaint{taint("example.com/hw-suspect", partitiontreev1.TaintEffectNoSchedule)},
			want:        true,
		},
		{
			description: "any key with NoExecute",
			taints:      []*partitiontreev1.PartitionTaint{taint("example.com/custom", partitiontreev1.TaintEffectNoExecute)},
			want:        true,
		},
		{
			description: "any key with PreferNoSchedule is advisory",
			taints:      []*partitiontreev1.PartitionTaint{taint("example.com/custom", partitiontreev1.TaintEffectPreferNoSchedule)},
			want:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			info := &PartitionInfo{Taints: tc.taints}
			if got := info.HasBlockedTaint(); got != tc.want {
				t.Errorf("HasBlockedTaint() = %v, want %v", got, tc.want)
			}
		})
	}

	var nilInfo *PartitionInfo
	if nilInfo.HasBlockedTaint() {
		t.Error("HasBlockedTaint() on nil PartitionInfo = true, want false")
	}
}

func TestIsNoExecuteBlocked(t *testing.T) {
	tests := []struct {
		description string
		taint       *partitiontreev1.PartitionTaint
		want        bool
	}{
		{
			description: "nil taint",
			taint:       nil,
			want:        false,
		},
		{
			description: "NoExecute with a recognised node key",
			taint:       taint("node.kubernetes.io/unreachable", partitiontreev1.TaintEffectNoExecute),
			want:        true,
		},
		{
			description: "unhealthy partition",
			taint:       taint(TaintKeyPartitionState, partitiontreev1.TaintEffectNoExecute),
			want:        true,
		},
		{
			description: "nodes not found",
			taint:       taint(TaintKeyNodesNotFound, partitiontreev1.TaintEffectNoExecute),
			want:        true,
		},
		{
			description: "NoExecute with an unrecognised key is not the scheduler's business",
			taint:       taint("example.com/custom", partitiontreev1.TaintEffectNoExecute),
			want:        false,
		},
		{
			description: "NoSchedule blocks placement but never evicts",
			taint:       taint("node.kubernetes.io/unschedulable", partitiontreev1.TaintEffectNoSchedule),
			want:        false,
		},
		{
			description: "PreferNoSchedule is advisory",
			taint:       taint("node.kubernetes.io/not-ready", partitiontreev1.TaintEffectPreferNoSchedule),
			want:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := IsNoExecuteBlocked(tc.taint); got != tc.want {
				t.Errorf("IsNoExecuteBlocked() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNoExecuteIsSubsetOfBlocked guards the invariant the eviction path depends on: every
// taint that makes the scheduler move a slice must also be one TaintFilter refuses to place
// it back onto. It walks the real BlockedTaints, crossed with every effect the API defines,
// so adding a key to one check and not the other fails here.
func TestNoExecuteIsSubsetOfBlocked(t *testing.T) {
	keys := append([]string{"example.com/custom"}, BlockedTaints...)
	effects := []partitiontreev1.TaintEffect{
		partitiontreev1.TaintEffectNoSchedule,
		partitiontreev1.TaintEffectNoExecute,
		partitiontreev1.TaintEffectPreferNoSchedule,
	}

	for _, key := range keys {
		for _, effect := range effects {
			tt := taint(key, effect)
			info := &PartitionInfo{Taints: []*partitiontreev1.PartitionTaint{tt}}
			if IsNoExecuteBlocked(tt) && !info.HasBlockedTaint() {
				t.Errorf("taint {key: %q, effect: %q} triggers eviction but is not blocked for placement", key, effect)
			}
		}
	}
}

// TestPartitionInfoDeepCopyTaints checks that a copy shares no taint memory with the
// original, including a nil element, which must stay a nil hole in the copy.
func TestPartitionInfoDeepCopyTaints(t *testing.T) {
	original := &PartitionInfo{
		ID: "p1",
		Taints: []*partitiontreev1.PartitionTaint{
			taint("node.kubernetes.io/unreachable", partitiontreev1.TaintEffectNoExecute),
			nil,
		},
		Children: []*PartitionInfo{{
			ID: "p1-c0",
			Taints: []*partitiontreev1.PartitionTaint{
				taint("node.kubernetes.io/not-ready", partitiontreev1.TaintEffectNoExecute),
			},
		}},
	}

	copied := original.DeepCopy()

	if len(copied.Taints) != len(original.Taints) {
		t.Fatalf("copied %d taints, want %d", len(copied.Taints), len(original.Taints))
	}
	if copied.Taints[0] == original.Taints[0] {
		t.Error("copy shares a taint pointer with the original")
	}
	if *copied.Taints[0] != *original.Taints[0] {
		t.Errorf("copied taint %+v, want %+v", *copied.Taints[0], *original.Taints[0])
	}
	if copied.Taints[1] != nil {
		t.Errorf("nil taint copied as %+v, want nil", copied.Taints[1])
	}
	if copied.Children[0].Taints[0] == original.Children[0].Taints[0] {
		t.Error("copied child shares a taint pointer with the original child")
	}

	// The backing array must be distinct too.
	original.Taints[0] = taint("node.kubernetes.io/memory-pressure", partitiontreev1.TaintEffectNoSchedule)
	original.Taints[1] = taint("node.kubernetes.io/disk-pressure", partitiontreev1.TaintEffectNoSchedule)
	if copied.Taints[0].Key != "node.kubernetes.io/unreachable" {
		t.Errorf("copy observed a later write to the original: got %q", copied.Taints[0].Key)
	}
	if copied.Taints[1] != nil {
		t.Errorf("copy observed a later write to the original's nil slot: got %+v", copied.Taints[1])
	}
	if !copied.HasBlockedTaint() {
		t.Error("copy lost its blocked taint")
	}
}

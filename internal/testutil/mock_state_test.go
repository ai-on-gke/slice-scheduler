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

package testutil

import (
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

// findInTree locates a partition by ID by walking the trees, which is the only
// route that does not go through the index under test.
func findInTree(roots []*interfaces.PartitionInfo, id string) *interfaces.PartitionInfo {
	for _, root := range roots {
		if root == nil {
			continue
		}
		if root.ID == id {
			return root
		}
		if found := findInTree(root.Children, id); found != nil {
			return found
		}
	}
	return nil
}

func TestMockClusterState_GetPartitionInfo_ReturnsTreeNodes(t *testing.T) {
	state := NewStateBuilder().
		Pod("pod1").
		Tree("4x4x4", "root1").
		Child("2x4x4", "half1").
		Child("2x2x4", "quarter1").
		End().
		Build()

	for _, id := range []string{"root1", "half1", "quarter1"} {
		got := state.GetPartitionInfo([]string{id})
		if len(got) != 1 {
			t.Fatalf("GetPartitionInfo(%q) returned %d infos, want 1", id, len(got))
		}
		want := findInTree(state.GetPartitionTrees(), id)
		if want == nil {
			t.Fatalf("%q is not reachable from GetPartitionTrees, test fixture is wrong", id)
		}
		// Pointer identity, not just equality: the two accessors must agree on
		// one object, or a caller can observe two different views of a
		// partition within a single scheduling cycle.
		if got[0] != want {
			t.Errorf("GetPartitionInfo(%q) returned %p, GetPartitionTrees has %p; want the same node", id, got[0], want)
		}
	}
}

func TestMockClusterState_GetPartitionInfo_InheritsAncestorInUseBy(t *testing.T) {
	state := NewStateBuilder().
		Pod("pod1").
		Tree("4x4x4", "root1").
		UsedBy("slice-a", 100, "test").
		Child("2x4x4", "half1").
		End().
		Build()

	got := state.GetPartitionInfo([]string{"half1"})
	if len(got) != 1 {
		t.Fatalf("GetPartitionInfo returned %d infos, want 1", len(got))
	}
	// half1 holds no reservation of its own, but its ancestor does. Reporting
	// InUseBy == nil here would contradict the tree the same state just
	// returned.
	if got[0].InUseBy == nil {
		t.Fatal("descendant of an in-use partition reports InUseBy == nil")
	}
	if got[0].InUseBy.Name != "slice-a" {
		t.Errorf("InUseBy.Name = %q, want %q", got[0].InUseBy.Name, "slice-a")
	}
}

func TestMockClusterState_GetPartitionInfo_SkipsUnknownIDs(t *testing.T) {
	state := NewStateBuilder().
		Pod("pod1").
		Tree("4x4x4", "root1").
		End().
		Build()

	got := state.GetPartitionInfo([]string{"root1", "does-not-exist"})
	if len(got) != 1 {
		t.Fatalf("GetPartitionInfo returned %d infos, want 1", len(got))
	}
	if got[0].ID != "root1" {
		t.Errorf("ID = %q, want %q", got[0].ID, "root1")
	}

	if got := state.GetPartitionInfo(nil); len(got) != 0 {
		t.Errorf("GetPartitionInfo(nil) returned %d infos, want 0", len(got))
	}
}

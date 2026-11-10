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

package watcher

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPartitionState_TestingNotReady(t *testing.T) {
	cache := NewPartitionCache(false)
	ctx := context.Background()
	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "4x4",
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: "leaf-1",
						Topology:    "2x4",
						NodeNames:   []string{"node-1"},
					},
					{
						PartitionID: "leaf-2",
						Topology:    "2x4",
						NodeNames:   []string{"node-2"},
					},
				},
			},
		},
	}
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				GkeSchedulerTestingLabel: SchedulerTestingNotReady,
			},
		},
	}
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-2",
		},
	}
	cache.UpdateTree(ctx, tree, []corev1.Node{*node1, *node2})

	watcher := NewPartitionWatcher(cache)
	state := watcher.GetState()
	trees := state.GetPartitionTrees()
	if len(trees) != 1 {
		t.Fatalf("expected 1 tree, got %d", len(trees))
	}
	root := trees[0]
	if !root.TestingNotReady {
		t.Errorf("expected root.TestingNotReady = true because leaf-1 has testing label")
	}
	if !root.Children[0].TestingNotReady {
		t.Errorf("expected leaf-1.TestingNotReady = true")
	}
	if root.Children[1].TestingNotReady {
		t.Errorf("expected leaf-2.TestingNotReady = false")
	}
}

func TestPartitionWatcher(t *testing.T) {
	pc := NewPartitionCache(false)
	watcher := NewPartitionWatcher(pc)

	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tree"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root-1",
				Topology:    "TRILLIUM_4_4",
				State:       partitiontreev1.PartitionStateHealthy,
			},
		},
	}
	pc.UpdateTree(context.Background(), tree, nil)

	state := watcher.GetState()
	trees := state.GetPartitionTrees()
	if len(trees) != 1 {
		t.Errorf("Expected 1 tree, got %d", len(trees))
	}
	if trees[0].ID != "root-1" {
		t.Errorf("Expected tree ID 'root-1', got %s", trees[0].ID)
	}

	slicesA := []*interfaces.SliceInfo{
		{Name: "slice-a", PartitionIds: []string{"root-1"}, Topology: "4x4"},
	}
	err := watcher.Reserve(context.Background(), slicesA)
	if err != nil {
		t.Errorf("Unexpected error reserving: %v", err)
	}

	// Try reserving again, should fail
	slicesB := []*interfaces.SliceInfo{
		{Name: "slice-b", PartitionIds: []string{"root-1"}, Topology: "4x4"},
	}
	err = watcher.Reserve(context.Background(), slicesB)
	if err == nil {
		t.Error("Expected error reserving already used partition")
	}

	watcher.Unreserve(context.Background(), slicesA)
}

func TestPartitionState_toInterfacePartitionInfo_DegradationState(t *testing.T) {
	tests := []struct {
		name         string
		state        partitiontreev1.PartitionStateType
		wantDegState bool
		wantMode     string
		wantOcsName  string
	}{
		{
			name:         "Degraded state produces DegradationState",
			state:        partitiontreev1.PartitionStateDegraded,
			wantDegState: true,
			wantMode:     interfaces.DegradationModeDegraded,
			wantOcsName:  "DEFAULT_DEGRADATION_DOMAIN",
		},
		{
			name:         "Healthy state produces nil DegradationState",
			state:        partitiontreev1.PartitionStateHealthy,
			wantDegState: false,
		},
		{
			name:         "Unhealthy state produces nil DegradationState",
			state:        partitiontreev1.PartitionStateUnhealthy,
			wantDegState: false,
		},
		{
			name:         "Unknown state produces nil DegradationState",
			state:        partitiontreev1.PartitionStateUnknown,
			wantDegState: false,
		},
		{
			name:         "Incomplete state produces nil DegradationState",
			state:        partitiontreev1.PartitionStateIncomplete,
			wantDegState: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := NewPartitionCache(false)
			watcher := NewPartitionWatcher(pc)

			tree := &partitiontreev1.PartitionTree{
				ObjectMeta: metav1.ObjectMeta{Name: "test-tree"},
				Status: partitiontreev1.PartitionTreeStatus{
					Root: &partitiontreev1.PartitionInfo{
						PartitionID: "root-1",
						Topology:    "4x4x4",
						State:       tt.state,
					},
				},
			}
			pc.UpdateTree(context.Background(), tree, nil)

			state := watcher.GetState()
			trees := state.GetPartitionTrees()
			if len(trees) != 1 {
				t.Fatalf("Expected 1 tree, got %d", len(trees))
			}

			info := trees[0]
			if tt.wantDegState {
				if info.DegradationState == nil {
					t.Fatal("Expected non-nil DegradationState")
				}
				if info.DegradationState.Mode != tt.wantMode {
					t.Errorf("DegradationState.Mode = %q, want %q", info.DegradationState.Mode, tt.wantMode)
				}
				if info.DegradationState.ToleratedOcsName != tt.wantOcsName {
					t.Errorf("DegradationState.ToleratedOcsName = %q, want %q", info.DegradationState.ToleratedOcsName, tt.wantOcsName)
				}
			} else {
				if info.DegradationState != nil {
					t.Errorf("Expected nil DegradationState, got %+v", info.DegradationState)
				}
			}
		})
	}
}

func TestPartitionState_toInterfacePartitionInfo_DegradedState(t *testing.T) {
	pc := NewPartitionCache(false)

	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tree"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root-1",
				Topology:    "4x4x4",
				State:       partitiontreev1.PartitionStateDegraded,
			},
		},
	}
	pc.UpdateTree(context.Background(), tree, nil)

	watcher := NewPartitionWatcher(pc)
	state := watcher.GetState()
	trees := state.GetPartitionTrees()
	if len(trees) != 1 {
		t.Fatalf("Expected 1 tree, got %d", len(trees))
	}

	info := trees[0]
	if info.DegradationState == nil {
		t.Fatal("Expected DegradationState to be non-nil for Degraded partition")
	}
	if info.DegradationState.Mode != interfaces.DegradationModeDegraded {
		t.Errorf("DegradationState.Mode = %q, want %q", info.DegradationState.Mode, interfaces.DegradationModeDegraded)
	}
	if info.DegradationState.ToleratedOcsName != "DEFAULT_DEGRADATION_DOMAIN" {
		t.Errorf("DegradationState.ToleratedOcsName = %q, want %q", info.DegradationState.ToleratedOcsName, "DEFAULT_DEGRADATION_DOMAIN")
	}
}

func TestPartitionState_toInterfacePartitionInfo_HealthyState_NilDegradation(t *testing.T) {
	pc := NewPartitionCache(false)

	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tree"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root-1",
				Topology:    "4x4x4",
				State:       partitiontreev1.PartitionStateHealthy,
			},
		},
	}
	pc.UpdateTree(context.Background(), tree, nil)

	watcher := NewPartitionWatcher(pc)
	state := watcher.GetState()
	trees := state.GetPartitionTrees()
	if len(trees) != 1 {
		t.Fatalf("Expected 1 tree, got %d", len(trees))
	}

	info := trees[0]
	if info.DegradationState != nil {
		t.Errorf("Expected DegradationState to be nil for Healthy partition, got %+v", info.DegradationState)
	}
}

// twoLevelTree builds a root with a single child, both resolvable by ID.
func twoLevelTree(name, rootID, childID string) *partitiontreev1.PartitionTree {
	return &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: rootID,
				Topology:    "4x4x4",
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: childID,
						Topology:    "2x4x4",
						NodeNames:   []string{"node-1"},
					},
				},
			},
		},
	}
}

// findByID walks the trees, which is the only route to a partition that does
// not go through the id index under test.
func findByID(roots []*interfaces.PartitionInfo, id string) *interfaces.PartitionInfo {
	for _, root := range roots {
		if root == nil {
			continue
		}
		if root.ID == id {
			return root
		}
		if found := findByID(root.Children, id); found != nil {
			return found
		}
	}
	return nil
}

// A partition whose ancestor holds a reservation is itself in use. Resolving
// it by ID used to re-convert from partitionNodeCache with parentInUse = nil,
// which reported InUseBy == nil and flatly contradicted the tree returned by
// GetPartitionTrees in the same cycle.
func TestPartitionState_GetPartitionInfo_InheritsAncestorInUseBy(t *testing.T) {
	ctx := context.Background()
	cache := NewPartitionCache(false)
	cache.UpdateTree(ctx, twoLevelTree("tree-1", "root", "child"), nil)

	sInfo := &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"root"}}
	if ok, existing := cache.ReservePartition(ctx, sInfo); !ok {
		t.Fatalf("failed to reserve root, held by %q", existing)
	}

	state := NewPartitionWatcher(cache).GetState()

	infos := state.GetPartitionInfo([]string{"child"})
	if len(infos) != 1 {
		t.Fatalf("GetPartitionInfo(child) returned %d infos, want 1", len(infos))
	}
	if infos[0].InUseBy == nil {
		t.Fatal("child reports InUseBy == nil while its ancestor is reserved")
	}
	if got := infos[0].InUseBy.Name; got != "slice-a" {
		t.Errorf("child InUseBy.Name = %q, want %q", got, "slice-a")
	}

	// The tree must agree, or the two accessors describe different clusters.
	fromTree := findByID(state.GetPartitionTrees(), "child")
	if fromTree == nil {
		t.Fatal("child is not reachable from GetPartitionTrees")
	}
	if fromTree != infos[0] {
		t.Errorf("GetPartitionInfo returned %p, GetPartitionTrees has %p; want one node", infos[0], fromTree)
	}
}

// A ClusterState is a snapshot. Once taken, a later cache mutation must not be
// visible through it -- otherwise one scheduling cycle reasons about two
// different versions of the cluster.
func TestPartitionState_SnapshotIsStableAcrossCacheMutation(t *testing.T) {
	ctx := context.Background()
	cache := NewPartitionCache(false)
	cache.UpdateTree(ctx, twoLevelTree("tree-1", "root", "child"), nil)

	state := NewPartitionWatcher(cache).GetState()

	before := state.GetPartitionInfo([]string{"root"})
	if len(before) != 1 || before[0].InUseBy != nil {
		t.Fatalf("root should start unreserved, got %+v", before)
	}

	// Mutate the cache after the snapshot was taken.
	sInfo := &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"root"}}
	if ok, existing := cache.ReservePartition(ctx, sInfo); !ok {
		t.Fatalf("failed to reserve root, held by %q", existing)
	}

	after := state.GetPartitionInfo([]string{"root"})
	if len(after) != 1 {
		t.Fatalf("GetPartitionInfo(root) returned %d infos, want 1", len(after))
	}
	if after[0].InUseBy != nil {
		t.Errorf("snapshot observed a reservation made after it was taken: InUseBy = %+v", after[0].InUseBy)
	}

	// A fresh snapshot must see it.
	fresh := NewPartitionWatcher(cache).GetState().GetPartitionInfo([]string{"root"})
	if len(fresh) != 1 || fresh[0].InUseBy == nil {
		t.Errorf("a snapshot taken after the reservation does not see it: %+v", fresh)
	}
}

func TestPartitionState_GetPartitionInfo_SkipsUnknownIDs(t *testing.T) {
	ctx := context.Background()
	cache := NewPartitionCache(false)
	cache.UpdateTree(ctx, twoLevelTree("tree-1", "root", "child"), nil)

	state := NewPartitionWatcher(cache).GetState()

	infos := state.GetPartitionInfo([]string{"root", "no-such-partition", "child"})
	if len(infos) != 2 {
		t.Fatalf("GetPartitionInfo returned %d infos, want 2", len(infos))
	}
	if infos[0].ID != "root" || infos[1].ID != "child" {
		t.Errorf("got ids [%q %q], want [root child]", infos[0].ID, infos[1].ID)
	}
}

// taintedTree is a cube with two leaves, where only leaf-1 carries taints.
func taintedTree(leafTaints ...partitiontreev1.PartitionTaint) *partitiontreev1.PartitionTree {
	return &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "4x4",
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{PartitionID: "leaf-1", Topology: "2x4", NodeNames: []string{"node-1"}, Taints: leafTaints},
					{PartitionID: "leaf-2", Topology: "2x4", NodeNames: []string{"node-2"}},
				},
			},
		},
	}
}

var (
	unreachable = partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnreachable, Effect: partitiontreev1.TaintEffectNoExecute}
	cordoned    = partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnschedulable, Effect: partitiontreev1.TaintEffectNoSchedule}
)

func taintKeys(info *interfaces.PartitionInfo) []string {
	var keys []string
	for _, t := range info.Taints {
		keys = append(keys, t.Key)
	}
	return keys
}

func TestPartitionState_Taints(t *testing.T) {
	cache := NewPartitionCache(false, WithTaintFiltering(true))
	cache.UpdateTree(context.Background(), taintedTree(unreachable), nil)

	state := NewPartitionWatcher(cache).GetState()
	leaf1 := findByID(state.GetPartitionTrees(), "leaf-1")
	leaf2 := findByID(state.GetPartitionTrees(), "leaf-2")
	root := findByID(state.GetPartitionTrees(), "root")

	if got := taintKeys(leaf1); len(got) != 1 || got[0] != corev1.TaintNodeUnreachable {
		t.Errorf("leaf-1 taints = %v, want [%s]", got, corev1.TaintNodeUnreachable)
	}
	if !root.HasBlockedTaint() {
		t.Error("root does not carry its tainted child's taint")
	}
	if len(leaf2.Taints) != 0 {
		t.Errorf("leaf-2 taints = %v, want none: taints must not spread to siblings", taintKeys(leaf2))
	}
}

func TestPartitionState_TaintsDedupedInOrder(t *testing.T) {
	tree := taintedTree(unreachable, cordoned)
	tree.Status.Root.Taints = []partitiontreev1.PartitionTaint{cordoned}
	tree.Status.Root.ChildPartitions[1].Taints = []partitiontreev1.PartitionTaint{unreachable}

	cache := NewPartitionCache(false, WithTaintFiltering(true))
	cache.UpdateTree(context.Background(), tree, nil)

	root := findByID(NewPartitionWatcher(cache).GetState().GetPartitionTrees(), "root")
	got := taintKeys(root)
	want := []string{corev1.TaintNodeUnschedulable, corev1.TaintNodeUnreachable}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("root taints = %v, want %v: own first, then children's, each once", got, want)
	}
}

func TestPartitionState_TaintsDedupedWithEarliestTimeAdded(t *testing.T) {
	later := metav1.NewTime(time.Date(2026, 11, 3, 12, 5, 0, 0, time.UTC))
	earlier := metav1.NewTime(time.Date(2026, 11, 3, 12, 1, 0, 0, time.UTC))
	wantEarliest := earlier

	tree := taintedTree(partitiontreev1.PartitionTaint{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitiontreev1.TaintEffectNoExecute,
		TimeAdded: &later,
	})
	tree.Status.Root.ChildPartitions[1].Taints = []partitiontreev1.PartitionTaint{{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitiontreev1.TaintEffectNoExecute,
		TimeAdded: &earlier,
	}}

	cache := NewPartitionCache(false, WithTaintFiltering(true))
	cache.UpdateTree(context.Background(), tree, nil)

	state := NewPartitionWatcher(cache).GetState()
	root := findByID(state.GetPartitionTrees(), "root")
	if len(root.Taints) != 1 {
		t.Fatalf("root taints len = %d (%+v), want 1 deduplicated taint", len(root.Taints), root.Taints)
	}
	if root.Taints[0].TimeAdded == nil || !root.Taints[0].TimeAdded.Equal(&earlier) {
		t.Errorf("root taint TimeAdded = %v, want earliest %v", root.Taints[0].TimeAdded, earlier)
	}

	// Mutating the input PartitionTree's TimeAdded pointer must not mutate the cached taint,
	// and mutating a DeepCopy of PartitionInfo must not mutate the snapshot.
	laterMut := metav1.NewTime(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
	*tree.Status.Root.ChildPartitions[1].Taints[0].TimeAdded = laterMut
	freshRoot := findByID(NewPartitionWatcher(cache).GetState().GetPartitionTrees(), "root")
	if !freshRoot.Taints[0].TimeAdded.Equal(&wantEarliest) {
		t.Errorf("cached TimeAdded mutated through input tree pointer: got %v, want %v", freshRoot.Taints[0].TimeAdded, wantEarliest)
	}

	cloned := freshRoot.DeepCopy()
	*cloned.Taints[0].TimeAdded = laterMut
	if !freshRoot.Taints[0].TimeAdded.Equal(&wantEarliest) {
		t.Errorf("snapshot TimeAdded mutated through PartitionInfo.DeepCopy: got %v, want %v", freshRoot.Taints[0].TimeAdded, wantEarliest)
	}
}

func TestPartitionState_TaintsIgnoredWhenDisabled(t *testing.T) {
	cache := NewPartitionCache(false)
	cache.UpdateTree(context.Background(), taintedTree(unreachable), nil)

	for _, id := range []string{"root", "leaf-1"} {
		info := findByID(NewPartitionWatcher(cache).GetState().GetPartitionTrees(), id)
		if len(info.Taints) != 0 {
			t.Errorf("%s taints = %v, want none with taint filtering disabled", id, taintKeys(info))
		}
	}
}

func TestPartitionState_TaintsClearedOnUpdate(t *testing.T) {
	cache := NewPartitionCache(false, WithTaintFiltering(true))
	ctx := context.Background()
	cache.UpdateTree(ctx, taintedTree(unreachable), nil)
	cache.UpdateTree(ctx, taintedTree(), nil)

	root := findByID(NewPartitionWatcher(cache).GetState().GetPartitionTrees(), "root")
	if root.HasBlockedTaint() {
		t.Errorf("root taints = %v after the leaf recovered, want none", taintKeys(root))
	}
}

// TestPartitionCache_TaintsDoNotAliasTree checks that the cache's taints are its own: the
// tree passed to UpdateTree is the informer's object, which the cache must neither write
// through nor observe later writes to.
func TestPartitionCache_TaintsDoNotAliasTree(t *testing.T) {
	tree := taintedTree(unreachable)
	cache := NewPartitionCache(false, WithTaintFiltering(true))
	cache.UpdateTree(context.Background(), tree, nil)

	tree.Status.Root.ChildPartitions[0].Taints[0] = cordoned

	leaf1 := findByID(NewPartitionWatcher(cache).GetState().GetPartitionTrees(), "leaf-1")
	if got := taintKeys(leaf1); len(got) != 1 || got[0] != corev1.TaintNodeUnreachable {
		t.Errorf("leaf-1 taints = %v after the tree was rewritten, want [%s]", got, corev1.TaintNodeUnreachable)
	}
}

func TestPartitionCache_NoExecute(t *testing.T) {
	ctx := context.Background()

	t.Run("NoExecute on a leaf marks the leaf and its ancestors", func(t *testing.T) {
		cache := NewPartitionCache(false, WithTaintFiltering(true))
		cache.UpdateTree(ctx, taintedTree(unreachable), nil)

		for id, want := range map[string]bool{"root": true, "leaf-1": true, "leaf-2": false, "missing": false} {
			got, wait := cache.NoExecuteWait(id)
			if got != want || wait != 0 {
				t.Errorf("NoExecuteWait(%q) = (%v, %v), want (%v, 0)", id, got, wait, want)
			}
		}
		wantDeadlines := map[string]time.Time{"root": {}, "leaf-1": {}}
		if got := cache.NoExecuteDeadlines("tree-1"); !reflect.DeepEqual(got, wantDeadlines) {
			t.Errorf("NoExecuteDeadlines() = %v, want %v", got, wantDeadlines)
		}
	})

	t.Run("NoSchedule is not NoExecute", func(t *testing.T) {
		cache := NewPartitionCache(false, WithTaintFiltering(true))
		cache.UpdateTree(ctx, taintedTree(cordoned), nil)

		if gotLeaf, _ := cache.NoExecuteWait("leaf-1"); gotLeaf {
			t.Error("NoExecuteWait(leaf-1) = true for a NoSchedule taint")
		}
		if gotRoot, _ := cache.NoExecuteWait("root"); gotRoot {
			t.Error("NoExecuteWait(root) = true for a NoSchedule taint")
		}
		if got := cache.NoExecuteDeadlines("tree-1"); len(got) != 0 {
			t.Errorf("NoExecuteDeadlines() = %v, want empty", got)
		}
	})

	t.Run("disabled filtering reports nothing", func(t *testing.T) {
		cache := NewPartitionCache(false)
		cache.UpdateTree(ctx, taintedTree(unreachable), nil)
		cache.ReservePartition(ctx, &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"leaf-1"}})

		if got, _ := cache.NoExecuteWait("leaf-1"); got {
			t.Error("NoExecuteWait() = true with filtering disabled")
		}
		if got := cache.NoExecuteDeadlines("tree-1"); len(got) != 0 {
			t.Errorf("NoExecuteDeadlines() = %v, want empty", got)
		}
		if got := cache.SlicesOnNoExecutePartitions("tree-1"); len(got) != 0 {
			t.Errorf("SlicesOnNoExecutePartitions() = %v, want empty", got)
		}
	})

	t.Run("occupants of tainted partitions only", func(t *testing.T) {
		cache := NewPartitionCache(false, WithTaintFiltering(true))
		cache.UpdateTree(ctx, taintedTree(unreachable), nil)
		cache.ReservePartition(ctx, &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"leaf-1"}})
		cache.ReservePartition(ctx, &interfaces.SliceInfo{Name: "slice-b", PartitionIds: []string{"leaf-2"}})

		got := cache.SlicesOnNoExecutePartitions("tree-1")
		if len(got) != 1 || got[0].Name != "slice-a" {
			t.Errorf("SlicesOnNoExecutePartitions() = %v, want [slice-a]", got)
		}
		if got := cache.SlicesOnNoExecutePartitions("no-such-tree"); got != nil {
			t.Errorf("SlicesOnNoExecutePartitions(unknown tree) = %v, want nil", got)
		}
	})

	t.Run("slice on the parent of a tainted leaf is an occupant", func(t *testing.T) {
		cache := NewPartitionCache(false, WithTaintFiltering(true))
		cache.UpdateTree(ctx, taintedTree(unreachable), nil)
		cache.ReservePartition(ctx, &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"root"}})

		got := cache.SlicesOnNoExecutePartitions("tree-1")
		if len(got) != 1 || got[0].Name != "slice-a" {
			t.Errorf("SlicesOnNoExecutePartitions() = %v, want [slice-a]", got)
		}
	})

	t.Run("reservations survive a tree update that adds the taint", func(t *testing.T) {
		cache := NewPartitionCache(false, WithTaintFiltering(true))
		cache.UpdateTree(ctx, taintedTree(), nil)
		cache.ReservePartition(ctx, &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"leaf-1"}})
		if got := cache.SlicesOnNoExecutePartitions("tree-1"); len(got) != 0 {
			t.Fatalf("SlicesOnNoExecutePartitions() before taint = %v, want empty", got)
		}

		cache.UpdateTree(ctx, taintedTree(unreachable), nil)
		got := cache.SlicesOnNoExecutePartitions("tree-1")
		if len(got) != 1 || got[0].Name != "slice-a" {
			t.Errorf("SlicesOnNoExecutePartitions() after taint = %v, want [slice-a]", got)
		}
	})
}

func TestPartitionCache_NoExecuteTolerationOverflowAndEdgeCases(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	added := metav1.NewTime(t0)
	notReady := partitiontreev1.PartitionTaint{
		Key:       corev1.TaintNodeNotReady,
		Effect:    partitiontreev1.TaintEffectNoExecute,
		TimeAdded: &added,
	}

	t.Run("extremely large toleration seconds clamps instead of overflowing int64 duration", func(t *testing.T) {
		now := t0.Add(10 * time.Second)
		for _, hugeTol := range []int64{math.MaxInt64/int64(time.Second) + 1, math.MaxInt64} {
			cache := NewPartitionCache(false,
				WithTaintFiltering(true),
				WithDefaultNotReadyTolerationSeconds(hugeTol),
				WithClock(func() time.Time { return now }),
			)
			cache.UpdateTree(ctx, taintedTree(notReady), nil)
			tainted, wait := cache.NoExecuteWait("leaf-1")
			if !tainted || wait <= 100*365*24*time.Hour {
				t.Errorf("NoExecuteWait(tol=%d) = (%v, %v), want tainted=true with multi-century clamped wait (not overflow to 0)", hugeTol, tainted, wait)
			}
		}
	})

	t.Run("future-skewed TimeAdded with huge toleration clamps cleanly", func(t *testing.T) {
		futureAdded := metav1.NewTime(t0.Add(30 * time.Second))
		futureTaint := partitiontreev1.PartitionTaint{
			Key:       corev1.TaintNodeNotReady,
			Effect:    partitiontreev1.TaintEffectNoExecute,
			TimeAdded: &futureAdded,
		}
		cache300 := NewPartitionCache(false,
			WithTaintFiltering(true),
			WithDefaultNotReadyTolerationSeconds(300),
			WithClock(func() time.Time { return t0 }),
		)
		cache300.UpdateTree(ctx, taintedTree(futureTaint), nil)
		if tainted, wait := cache300.NoExecuteWait("leaf-1"); !tainted || wait != 330*time.Second {
			t.Errorf("NoExecuteWait(future TimeAdded, tol=300) = (%v, %v), want (true, 330s)", tainted, wait)
		}

		cacheMax := NewPartitionCache(false,
			WithTaintFiltering(true),
			WithDefaultNotReadyTolerationSeconds(math.MaxInt64),
			WithClock(func() time.Time { return t0 }),
		)
		cacheMax.UpdateTree(ctx, taintedTree(futureTaint), nil)
		if tainted, wait := cacheMax.NoExecuteWait("leaf-1"); !tainted || wait != time.Duration(math.MaxInt64) {
			t.Errorf("NoExecuteWait(future TimeAdded, tol=MaxInt64) = (%v, %v), want (true, MaxInt64)", tainted, wait)
		}
	})

	t.Run("negative toleration is excluded from NoExecuteWait, NoExecuteDeadlines, and SlicesOnNoExecutePartitions", func(t *testing.T) {
		cache := NewPartitionCache(false,
			WithTaintFiltering(true),
			WithDefaultNotReadyTolerationSeconds(-1),
			WithClock(func() time.Time { return t0.Add(24 * time.Hour) }),
		)
		cache.UpdateTree(ctx, taintedTree(notReady), nil)
		cache.ReservePartition(ctx, &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"leaf-1"}})

		if tainted, wait := cache.NoExecuteWait("leaf-1"); tainted || wait != 0 {
			t.Errorf("NoExecuteWait(tol=-1) = (%v, %v), want (false, 0)", tainted, wait)
		}
		if got := cache.NoExecuteDeadlines("tree-1"); len(got) != 0 {
			t.Errorf("NoExecuteDeadlines(tol=-1) = %v, want empty", got)
		}
		if got := cache.SlicesOnNoExecutePartitions("tree-1"); len(got) != 0 {
			t.Errorf("SlicesOnNoExecutePartitions(tol=-1) = %v, want empty", got)
		}
	})

	t.Run("unionTaints across sibling leaves retains earliest TimeAdded on parent", func(t *testing.T) {
		later := metav1.NewTime(t0.Add(60 * time.Second))
		earlier := metav1.NewTime(t0.Add(10 * time.Second))
		tree := taintedTree(partitiontreev1.PartitionTaint{
			Key:       corev1.TaintNodeNotReady,
			Effect:    partitiontreev1.TaintEffectNoExecute,
			TimeAdded: &later,
		})
		tree.Status.Root.ChildPartitions[1].Taints = []partitiontreev1.PartitionTaint{{
			Key:       corev1.TaintNodeNotReady,
			Effect:    partitiontreev1.TaintEffectNoExecute,
			TimeAdded: &earlier,
		}}

		now := t0.Add(100 * time.Second)
		cache := NewPartitionCache(false,
			WithTaintFiltering(true),
			WithDefaultNotReadyTolerationSeconds(300),
			WithClock(func() time.Time { return now }),
		)
		cache.UpdateTree(ctx, tree, nil)

		// root unions leaf-1 (t0+60s -> 260s left) and leaf-2 (t0+10s -> 210s left), retaining earliest (210s left).
		if tainted, wait := cache.NoExecuteWait("root"); !tainted || wait != 210*time.Second {
			t.Errorf("NoExecuteWait(root) = (%v, %v), want (true, 210s)", tainted, wait)
		}
	})
}

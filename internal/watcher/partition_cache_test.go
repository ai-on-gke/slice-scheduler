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
	"sync"
	"testing"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Helper function to create a corev1.Node for testing.
func newTestNode(name string, ready corev1.ConditionStatus) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: ready,
				},
			},
		},
	}
}

// Helper function to create a simple partition info for testing.
func newTestPartitionInfo(id string, topology string, nodeNames []string) *partitiontreev1.PartitionInfo {
	return &partitiontreev1.PartitionInfo{
		PartitionID: id,
		Topology:    topology,
		State:       partitiontreev1.PartitionStateHealthy,
		NodeNames:   nodeNames,
	}
}

// Helper function to create a test partition node.
func newTestPartitionNode(id string, topology string) *PartitionNode {
	return &PartitionNode{
		Info: newTestPartitionInfo(id, topology, nil),
	}
}

func TestNewPartitionCache(t *testing.T) {
	pc := NewPartitionCache(true)
	if pc.partitionTreeCache == nil {
		t.Error("partitionStateCache should be initialized")
	}
	if pc.partitionNodeCache == nil {
		t.Error("partitionIdToNodeMap should be initialized")
	}
	if !pc.enableNodeReadiness {
		t.Error("enableNodeReadiness should be set")
	}
}

func TestPartitionCache_RootNodeOperations(t *testing.T) {
	node1 := &PartitionNode{
		Info: newTestPartitionInfo("root-1", "TRILLIUM_16_16", nil),
		Children: []*PartitionNode{
			{
				Info:     newTestPartitionInfo("child-1", "TRILLIUM_2_4", []string{"h1"}),
				KubeNode: &KubeNode{Name: "h1", Ready: true},
			},
			{
				Info:     newTestPartitionInfo("child-2", "TRILLIUM_2_4", []string{"h2"}),
				KubeNode: &KubeNode{Name: "h2", Ready: false},
			},
		},
	}
	testCases := []struct {
		name              string
		initialRootNodes  map[string]*PartitionNode
		op                string // "set", "get", "delete"
		opTreeName        string
		opNode            *PartitionNode
		wantOk            bool
		wantNode          *PartitionNode
		wantFinalNumRoots int
	}{
		{
			name:              "Get Existing Root",
			initialRootNodes:  map[string]*PartitionNode{"tree-1": node1},
			op:                "get",
			opTreeName:        "tree-1",
			wantOk:            true,
			wantNode:          node1,
			wantFinalNumRoots: 1,
		},
		{
			name:              "Get Non-existent Root",
			initialRootNodes:  map[string]*PartitionNode{},
			op:                "get",
			opTreeName:        "tree-1",
			wantOk:            false,
			wantNode:          nil,
			wantFinalNumRoots: 0,
		},
		{
			name:              "Delete Existing Root",
			initialRootNodes:  map[string]*PartitionNode{"tree-1": node1},
			op:                "delete",
			opTreeName:        "tree-1",
			wantFinalNumRoots: 0,
		},
		{
			name:              "Set New Root",
			initialRootNodes:  map[string]*PartitionNode{},
			op:                "set",
			opTreeName:        "tree-1",
			opNode:            node1,
			wantFinalNumRoots: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pc := NewPartitionCache(false)
			pc.partitionTreeCache = tc.initialRootNodes
			switch tc.op {
			case "get":
				gotNode, ok := pc.GetRootNode(tc.opTreeName)
				if ok != tc.wantOk {
					t.Errorf("GetRootNode() ok = %v, want %v", ok, tc.wantOk)
				}
				if gotNode != tc.wantNode {
					t.Errorf("GetRootNode() node = %p, want %p", gotNode, tc.wantNode)
				}
			case "set":
				pc.SetRootNode(tc.opTreeName, tc.opNode)
			case "delete":
				pc.DeletePartitionTree(context.Background(), tc.opTreeName)
			}
			if len(pc.partitionTreeCache) != tc.wantFinalNumRoots {
				t.Errorf("Final number of roots = %d, want %d", len(pc.partitionTreeCache), tc.wantFinalNumRoots)
			}
		})
	}
}

func TestPartitionCache_GetNode(t *testing.T) {
	node1 := newTestPartitionNode("node-1", "TRILLIUM_8_8")
	testCases := []struct {
		name             string
		initialNodes     map[string]*PartitionNode
		partitionIDToGet string
		wantOk           bool
		wantNode         *PartitionNode
	}{
		{
			name:             "Get Existing Node",
			initialNodes:     map[string]*PartitionNode{"node-1": node1},
			partitionIDToGet: "node-1",
			wantOk:           true,
			wantNode:         node1,
		},
		{
			name:             "Get Non-existent Node",
			initialNodes:     map[string]*PartitionNode{"node-1": node1},
			partitionIDToGet: "non-existent",
			wantOk:           false,
			wantNode:         nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pc := NewPartitionCache(false)
			pc.partitionNodeCache = tc.initialNodes
			gotNode, ok := pc.GetNode(tc.partitionIDToGet)
			if ok != tc.wantOk {
				t.Errorf("GetNode() ok = %v, want %v", ok, tc.wantOk)
			}
			if gotNode != tc.wantNode {
				t.Errorf("GetNode() node = %p, want %p", gotNode, tc.wantNode)
			}
		})
	}
}

func TestPartitionCache_ReserveReleasePartition(t *testing.T) {
	ctx := context.Background()
	baseNode := func(id string) *PartitionNode {
		return newTestPartitionNode(id, "TRILLIUM_4_4")
	}
	testCases := []struct {
		name                string
		initialNodes        map[string]*PartitionNode
		initialReservations map[string]string
		op                  string // "reserve" or "release"
		wantSuccess         bool
		wantExistingSlice   string
		wantFinalInUseBy    map[string]string // partitionID -> sliceName
		sliceInfos          []*interfaces.SliceInfo
	}{
		{
			name:             "Successful Reservation of single partition",
			initialNodes:     map[string]*PartitionNode{"p1": baseNode("p1")},
			op:               "reserve",
			wantSuccess:      true,
			wantFinalInUseBy: map[string]string{"p1": "slice-a"},
			sliceInfos:       []*interfaces.SliceInfo{{Name: "slice-a", PartitionIds: []string{"p1"}, ManagedBy: interfaces.SliceSchedulerName}},
		},
		{
			name:             "Successful Reservation of multiple partitions",
			initialNodes:     map[string]*PartitionNode{"p1": baseNode("p1"), "p2": baseNode("p2")},
			op:               "reserve",
			wantSuccess:      true,
			wantFinalInUseBy: map[string]string{"p1": "slice-a", "p2": "slice-a"},
			sliceInfos:       []*interfaces.SliceInfo{{Name: "slice-a", PartitionIds: []string{"p1", "p2"}, ManagedBy: interfaces.SliceSchedulerName}},
		},
		{
			name:                "Failed Reservation - one already in Use",
			initialNodes:        map[string]*PartitionNode{"p1": baseNode("p1"), "p2": baseNode("p2")},
			initialReservations: map[string]string{"p1": "slice-a"},
			op:                  "reserve",
			wantSuccess:         false,
			wantExistingSlice:   "slice-a",
			wantFinalInUseBy:    map[string]string{"p1": "slice-a", "p2": ""},
			sliceInfos:          []*interfaces.SliceInfo{{Name: "slice-b", PartitionIds: []string{"p2", "p1"}, ManagedBy: interfaces.SliceSchedulerName}},
		},
		{
			name:                "Release with wrong slice name",
			initialNodes:        map[string]*PartitionNode{"p1": baseNode("p1")},
			initialReservations: map[string]string{"p1": "slice-a"},
			op:                  "release",
			wantFinalInUseBy:    map[string]string{"p1": "slice-a"},
			sliceInfos:          []*interfaces.SliceInfo{{Name: "slice-c", PartitionIds: []string{"p1"}, ManagedBy: interfaces.SliceSchedulerName}},
		},
		{
			name:                "Successful Release of multiple partitions",
			initialNodes:        map[string]*PartitionNode{"p1": baseNode("p1"), "p2": baseNode("p2")},
			initialReservations: map[string]string{"p1": "slice-a", "p2": "slice-a"},
			op:                  "release",
			wantFinalInUseBy:    map[string]string{"p1": "", "p2": ""},
			sliceInfos:          []*interfaces.SliceInfo{{Name: "slice-a", PartitionIds: []string{"p1", "p2"}, ManagedBy: interfaces.SliceSchedulerName}},
		},
		{
			name:                "Successful Release of multiple slices",
			initialNodes:        map[string]*PartitionNode{"p1": baseNode("p1"), "p2": baseNode("p2")},
			initialReservations: map[string]string{"p1": "slice-a", "p2": "slice-b"},
			op:                  "release",
			wantFinalInUseBy:    map[string]string{"p1": "", "p2": ""},
			sliceInfos: []*interfaces.SliceInfo{
				{Name: "slice-a", PartitionIds: []string{"p1"}, ManagedBy: interfaces.SliceSchedulerName},
				{Name: "slice-b", PartitionIds: []string{"p2"}, ManagedBy: interfaces.SliceSchedulerName},
			},
		},
		{
			name:             "Reserve non-existent partition",
			initialNodes:     map[string]*PartitionNode{},
			op:               "reserve",
			wantSuccess:      false,
			wantFinalInUseBy: map[string]string{},
			sliceInfos:       []*interfaces.SliceInfo{{Name: "slice-a", PartitionIds: []string{"non-existent"}, ManagedBy: interfaces.SliceSchedulerName}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pc := NewPartitionCache(false)
			pc.partitionNodeCache = tc.initialNodes
			for pid, sliceName := range tc.initialReservations {
				if node, ok := pc.partitionNodeCache[pid]; ok {
					node.InUseBy = &interfaces.SliceInfo{Name: sliceName, ManagedBy: interfaces.SliceSchedulerName}
				}
			}
			switch tc.op {
			case "reserve":
				success, existingSlice := pc.ReservePartition(ctx, tc.sliceInfos[0])
				if success != tc.wantSuccess {
					t.Errorf("ReservePartition() success = %v, want %v", success, tc.wantSuccess)
				}
				if existingSlice != tc.wantExistingSlice {
					t.Errorf("ReservePartition() existingSlice = %q, want %q", existingSlice, tc.wantExistingSlice)
				}
			case "release":
				pc.ReleasePartitions(ctx, tc.sliceInfos)
			}

			for pID, sliceName := range tc.wantFinalInUseBy {
				node, ok := pc.GetNode(pID)
				if !ok {
					t.Errorf("node %q not found", pID)
					continue
				}
				if sliceName == "" {
					if node.IsUsed() {
						t.Errorf("node.inUseBy.name = %q, want %q for partition %q", node.InUseBy.Name, sliceName, pID)
					}
				} else {
					if !node.IsUsed() || node.InUseBy.Name != sliceName {
						t.Errorf("node.inUseBy.name = %q, want %q for partition %q", node.InUseBy.Name, sliceName, pID)
					}
				}
			}
		})
	}
}

func TestIsNodeReady(t *testing.T) {
	testCases := []struct {
		name string
		node *corev1.Node
		want bool
	}{
		{
			name: "Node Ready True",
			node: newTestNode("node-1", corev1.ConditionTrue),
			want: true,
		},
		{
			name: "Node Ready False",
			node: newTestNode("node-2", corev1.ConditionFalse),
			want: false,
		},
		{
			name: "Node Ready Unknown",
			node: newTestNode("node-3", corev1.ConditionUnknown),
			want: false,
		},
		{
			name: "Node No Ready Condition",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-4"},
				Status:     corev1.NodeStatus{},
			},
			want: false,
		},
		{
			name: "Node Multiple Conditions",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-5"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse},
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isNodeReady(tc.node)
			if got != tc.want {
				t.Errorf("isNodeReady() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPartitionCache_UpdateTreeOperations(t *testing.T) {
	ctx := context.Background()
	tree1 := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "TRILLIUM_4_4",
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: "child-1",
						Topology:    "TRILLIUM_2_4",
						NodeNames:   []string{"h1"},
					},
					{
						PartitionID: "child-2",
						Topology:    "TRILLIUM_2_4",
						NodeNames:   []string{"h2"},
					},
				},
			},
		},
	}
	newTree1 := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "new-root",
				Topology:    "TRILLIUM_4_4",
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: "child-1", // Same ID
						Topology:    "TRILLIUM_2_4",
					},
				},
			},
		},
	}
	testCases := []struct {
		name                string
		initialTrees        []*partitiontreev1.PartitionTree
		initialReservations map[string]string
		treeToUpdate        *partitiontreev1.PartitionTree
		wantNodes           map[string]bool   // partitionId -> exists
		wantReservations    map[string]string // partitionId -> sliceName
		wantLeafCache       map[string]string // nodeName -> partitionId
		wantKubeNodeStatus  map[string]bool   // partitionId -> ready
	}{
		{
			name:         "Add New Tree",
			treeToUpdate: tree1,
			wantNodes: map[string]bool{
				"root":    true,
				"child-1": true,
			},
		},
		{
			name:                "Update with State Preservation",
			initialTrees:        []*partitiontreev1.PartitionTree{tree1},
			initialReservations: map[string]string{"child-1": "slice-x"},
			treeToUpdate:        newTree1,
			wantNodes: map[string]bool{
				"root":     false,
				"new-root": true,
				"child-1":  true,
			},
			wantReservations: map[string]string{
				"child-1": "slice-x",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pc := NewPartitionCache(false)
			for _, tree := range tc.initialTrees {
				pc.UpdateTree(context.Background(), tree, nil)
			}
			for pid, sliceName := range tc.initialReservations {
				pc.ReservePartition(ctx, &interfaces.SliceInfo{Name: sliceName, PartitionIds: []string{pid}, ManagedBy: interfaces.SliceSchedulerName})
			}
			pc.UpdateTree(context.Background(), tc.treeToUpdate, nil)
			for pid, exists := range tc.wantNodes {
				_, ok := pc.GetNode(pid)
				if ok != exists {
					t.Errorf("GetNode(%q) ok = %v, want %v", pid, ok, exists)
				}
			}
			for pid, sliceName := range tc.wantReservations {
				node, ok := pc.GetNode(pid)
				if !ok {
					t.Errorf("Expected reserved node %q not found", pid)
					continue
				}
				if node.InUseBy.Name != sliceName {
					t.Errorf("node %q inUseBy.name = %q, want %q", pid, node.InUseBy.Name, sliceName)
				}
			}
		})
	}
}

func TestPartitionCache_Concurrency(t *testing.T) {
	ctx := context.Background()
	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "concurrent-tree"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "TRILLIUM_16_16",
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{PartitionID: "p1"}, {PartitionID: "p2"}, {PartitionID: "p3"}, {PartitionID: "p4"},
				},
			},
		},
	}
	testCases := []struct {
		name          string
		numGoroutines int
	}{
		{
			name:          "Interleaved Reads and Writes",
			numGoroutines: 100,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pc := NewPartitionCache(false)
			pc.UpdateTree(context.Background(), tree, nil)
			var wg sync.WaitGroup
			wg.Add(tc.numGoroutines)
			for i := 0; i < tc.numGoroutines; i++ {
				go func() {
					defer wg.Done()
					pc.ReservePartition(ctx, &interfaces.SliceInfo{Name: "slice-1", PartitionIds: []string{"p1"}, ManagedBy: interfaces.SliceSchedulerName})
					pc.GetNode("p2")
					pc.ReleasePartitions(ctx, []*interfaces.SliceInfo{{Name: "slice-1", PartitionIds: []string{"p1"}, ManagedBy: interfaces.SliceSchedulerName}})
					pc.UpdateTree(context.Background(), tree, nil)
					pc.ReservePartition(ctx, &interfaces.SliceInfo{Name: "slice-3", PartitionIds: []string{"p3"}, ManagedBy: interfaces.SliceSchedulerName})
					pc.ReleasePartitions(ctx, []*interfaces.SliceInfo{{Name: "slice-3", PartitionIds: []string{"p3"}, ManagedBy: interfaces.SliceSchedulerName}})
				}()
			}
			wg.Wait()
			node, _ := pc.GetNode("p1")
			if node.IsUsed() {
				t.Error("p1 should be released at the end of the test")
			}
		})
	}
}

func TestPartitionCache_DeletePartitionTree(t *testing.T) {
	ctx := context.Background()
	pc := NewPartitionCache(false)
	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "TRILLIUM_4_4",
				NodeNames:   []string{"h1"},
			},
		},
	}
	pc.UpdateTree(ctx, tree, nil)

	if _, ok := pc.GetNode("root"); !ok {
		t.Fatal("Expected node 'root' to exist")
	}

	pc.DeletePartitionTree(ctx, "tree-1")

	if _, ok := pc.GetRootNode("tree-1"); ok {
		t.Error("Expected root node 'tree-1' to be deleted")
	}
	if _, ok := pc.GetNode("root"); ok {
		t.Error("Expected node 'root' to be deleted from partitionNodeCache")
	}
}

func TestPartitionCache_UpdateLeafNodeCache(t *testing.T) {
	ctx := context.Background()
	pc := NewPartitionCache(true)
	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "TRILLIUM_4_4",
				NodeNames:   []string{"h1"},
			},
		},
	}
	node1 := newTestNode("h1", corev1.ConditionTrue)
	pc.UpdateTree(ctx, tree, []corev1.Node{*node1})

	ln, ok := pc.GetLeafNode("h1")
	if !ok || ln.KubeNode == nil || !ln.KubeNode.Ready {
		t.Fatal("Expected leaf node 'h1' to be ready")
	}

	node1Unready := newTestNode("h1", corev1.ConditionFalse)
	pc.UpdateLeafNodeCache(ctx, node1Unready)

	if ln.KubeNode.Ready {
		t.Error("Expected leaf node 'h1' to be unready after update")
	}
}

// TestPartitionCache_PodNamePropagatesToDescendants pins the pod name onto every
// node of the tree, not just the root.
//
// plugins.TightestFitScorer resolves a candidate's tree by matching the assigned
// partition's PodName against each root's PodName. Sub-cube slices are assigned
// descendants, so a blank descendant PodName matches no root at all: the scorer
// falls through to FitScore=math.MaxInt and DensityScore=0 for every sub-cube
// candidate, which ties them all and hands the placement decision to enumeration
// order. generators.countUniquePods and the pod label on the scheduling-outcome
// metrics read the same field.
func TestPartitionCache_PodNamePropagatesToDescendants(t *testing.T) {
	ctx := context.Background()
	pc := NewPartitionCache(true)

	pc.UpdateTree(ctx, &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "tree-pod-a",
			Labels: map[string]string{TpuPodIDLabel: "pod-a"},
		},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "IRONWOOD_4x4x4",
				State:       partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: "mid",
						Topology:    "IRONWOOD_2x2x2",
						State:       partitiontreev1.PartitionStateHealthy,
						ChildPartitions: []partitiontreev1.PartitionInfo{
							{
								PartitionID: "leaf",
								Topology:    "IRONWOOD_2x2x1",
								State:       partitiontreev1.PartitionStateHealthy,
								NodeNames:   []string{"h1"},
							},
						},
					},
				},
			},
		},
	}, nil)

	for _, id := range []string{"root", "mid", "leaf"} {
		node, ok := pc.GetNode(id)
		if !ok {
			t.Fatalf("partition %q missing from node cache", id)
		}
		if node.PodName != "pod-a" {
			t.Errorf("partition %q PodName = %q, want %q", id, node.PodName, "pod-a")
		}
	}
}

// threeLevelTree builds root -> mid -> leaf so that every pair of nodes in it
// physically overlaps.
// A Slice recreated under the same name can be placed on its predecessor's partitions
// before the predecessor's delete is seen. Releasing for the predecessor must leave the
// successor's hold alone.
func TestPartitionCache_ReleasePartitions_KeepsSuccessorHold(t *testing.T) {
	tests := []struct {
		name        string
		releaseUID  types.UID
		wantRelease bool
	}{
		{name: "different UID keeps the hold", releaseUID: "old-uid", wantRelease: false},
		{name: "same UID releases", releaseUID: "new-uid", wantRelease: true},
		{name: "unknown UID releases by name", releaseUID: "", wantRelease: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cache := NewPartitionCache(false)
			cache.UpdateTree(ctx, threeLevelTree("tree-1"), nil)

			successor := &interfaces.SliceInfo{Name: "slice-a", UID: "new-uid", PartitionIds: []string{"mid"}}
			if ok, existing := cache.ReservePartition(ctx, successor); !ok {
				t.Fatalf("setup: reserve failed, held by %q", existing)
			}

			freed := cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{{Name: "slice-a", UID: tc.releaseUID, PartitionIds: []string{"mid"}}})

			node, _ := cache.GetNode("mid")
			if released := node.InUseBy == nil; released != tc.wantRelease {
				t.Errorf("released = %v, want %v", released, tc.wantRelease)
			}
			if gotFreed := len(freed) > 0; gotFreed != tc.wantRelease {
				t.Errorf("ReleasePartitions() returned %v, want freed = %v", freed, tc.wantRelease)
			}
		})
	}
}

func threeLevelTree(name string) *partitiontreev1.PartitionTree {
	return &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "4x4x4",
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: "mid",
						Topology:    "2x4x4",
						ChildPartitions: []partitiontreev1.PartitionInfo{
							{
								PartitionID: "leaf",
								Topology:    "2x2x4",
								NodeNames:   []string{"node-1"},
							},
						},
					},
					{
						PartitionID: "sibling",
						Topology:    "2x4x4",
						NodeNames:   []string{"node-2"},
					},
				},
			},
		},
	}
}

// Two slices must never hold physically overlapping partitions. A partition
// contains its descendants and is contained by its ancestors, so holding any
// of them is the same conflict as holding the node itself.
func TestPartitionCache_ReservePartition_RejectsOverlap(t *testing.T) {
	tests := []struct {
		name       string
		heldBy     string
		requested  string
		wantOK     bool
		wantHolder string
	}{
		{
			name:       "descendant of a held ancestor",
			heldBy:     "root",
			requested:  "leaf",
			wantOK:     false,
			wantHolder: "slice-a",
		},
		{
			name:       "ancestor of a held descendant",
			heldBy:     "leaf",
			requested:  "root",
			wantOK:     false,
			wantHolder: "slice-a",
		},
		{
			name:       "parent of a held child",
			heldBy:     "leaf",
			requested:  "mid",
			wantOK:     false,
			wantHolder: "slice-a",
		},
		{
			// Siblings occupy disjoint hardware, so this must still succeed.
			name:      "disjoint sibling",
			heldBy:    "mid",
			requested: "sibling",
			wantOK:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cache := NewPartitionCache(false)
			cache.UpdateTree(ctx, threeLevelTree("tree-1"), nil)

			held := &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{tc.heldBy}}
			if ok, existing := cache.ReservePartition(ctx, held); !ok {
				t.Fatalf("setup: reserving %q failed, held by %q", tc.heldBy, existing)
			}

			req := &interfaces.SliceInfo{Name: "slice-b", PartitionIds: []string{tc.requested}}
			ok, existing := cache.ReservePartition(ctx, req)
			if ok != tc.wantOK {
				t.Fatalf("ReservePartition(%q) = %v, want %v", tc.requested, ok, tc.wantOK)
			}
			if !tc.wantOK && existing != tc.wantHolder {
				t.Errorf("existingSlice = %q, want %q", existing, tc.wantHolder)
			}

			if !tc.wantOK {
				// A refused reservation must leave no trace.
				node, found := cache.GetNode(tc.requested)
				if !found {
					t.Fatalf("%q vanished from the cache", tc.requested)
				}
				if node.InUseBy != nil {
					t.Errorf("%q was marked in use by %q despite the reservation failing", tc.requested, node.InUseBy.Name)
				}
			}
		})
	}
}

// The overlap check keys on the slice name, so a slice re-reserving what it
// already holds -- which the watch-driven paths do on every resync -- must
// still succeed.
func TestPartitionCache_ReservePartition_SameSliceIsIdempotent(t *testing.T) {
	ctx := context.Background()
	cache := NewPartitionCache(false)
	cache.UpdateTree(ctx, threeLevelTree("tree-1"), nil)

	sInfo := &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"mid"}}
	if ok, existing := cache.ReservePartition(ctx, sInfo); !ok {
		t.Fatalf("first reservation failed, held by %q", existing)
	}
	if ok, existing := cache.ReservePartition(ctx, sInfo); !ok {
		t.Errorf("re-reserving the same partition for the same slice failed, held by %q", existing)
	}
}

// A multi-partition reservation that fails partway must roll back completely,
// including when the failure comes from the overlap check rather than from a
// direct conflict.
func TestPartitionCache_ReservePartition_RollsBackOnOverlap(t *testing.T) {
	ctx := context.Background()
	cache := NewPartitionCache(false)
	cache.UpdateTree(ctx, threeLevelTree("tree-1"), nil)

	held := &interfaces.SliceInfo{Name: "slice-a", PartitionIds: []string{"leaf"}}
	if ok, existing := cache.ReservePartition(ctx, held); !ok {
		t.Fatalf("setup: reserving leaf failed, held by %q", existing)
	}

	// sibling is free and would be taken first; mid overlaps the held leaf.
	req := &interfaces.SliceInfo{Name: "slice-b", PartitionIds: []string{"sibling", "mid"}}
	if ok, _ := cache.ReservePartition(ctx, req); ok {
		t.Fatal("reservation containing an overlapping partition succeeded")
	}

	node, found := cache.GetNode("sibling")
	if !found {
		t.Fatal("sibling vanished from the cache")
	}
	if node.InUseBy != nil {
		t.Errorf("sibling stayed reserved by %q after the batch was refused", node.InUseBy.Name)
	}
}

// A leaf is a partition with no children, so it may list any number of node
// names -- including none. Deleting such a tree must not index NodeNames[0],
// and must still evict the entries the insert side actually created.
func TestPartitionCache_DeletePartitionTree_ChildlessPartitionNodeNameCounts(t *testing.T) {
	tests := []struct {
		name      string
		nodeNames []string
		// wantEvicted is the leafPartitionNodeCache key the insert side created
		// for this partition, or "" if it created none.
		wantEvicted string
	}{
		{"no node names", nil, ""},
		{"empty node names", []string{}, ""},
		{"single node name", []string{"h1"}, "h1"},
		{"multiple node names", []string{"h1", "h2"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pc := NewPartitionCache(false)
			tree := &partitiontreev1.PartitionTree{
				ObjectMeta: metav1.ObjectMeta{Name: "tree-1"},
				Status: partitiontreev1.PartitionTreeStatus{
					Root: &partitiontreev1.PartitionInfo{
						PartitionID: "root",
						Topology:    "TRILLIUM_4_4",
						NodeNames:   tt.nodeNames,
					},
				},
			}
			pc.UpdateTree(ctx, tree, nil)

			if tt.wantEvicted != "" {
				if _, ok := pc.leafPartitionNodeCache[tt.wantEvicted]; !ok {
					t.Fatalf("insert did not populate leafPartitionNodeCache[%q], test fixture is wrong", tt.wantEvicted)
				}
			}

			// Panics here on NodeNames[0] if the delete path indexes instead of
			// ranging.
			pc.DeletePartitionTree(ctx, "tree-1")

			if _, ok := pc.GetNode("root"); ok {
				t.Error("root stayed in partitionNodeCache after its tree was deleted")
			}
			if len(pc.leafPartitionNodeCache) != 0 {
				t.Errorf("leafPartitionNodeCache = %v, want empty after its tree was deleted", pc.leafPartitionNodeCache)
			}
		})
	}
}

func TestPartitionCache_OverlapsAndHolds(t *testing.T) {
	ctx := context.Background()
	pc := NewPartitionCache(false)
	cube := newTestPartitionNode("cube", "4x4")
	half0 := newTestPartitionNode("half-0", "2x4")
	half1 := newTestPartitionNode("half-1", "2x4")
	other := newTestPartitionNode("other", "4x4")
	half0.Parent, half1.Parent = cube, cube
	cube.Children = []*PartitionNode{half0, half1}
	for _, n := range []*PartitionNode{cube, half0, half1, other} {
		pc.partitionNodeCache[n.Info.PartitionID] = n
	}

	overlaps := []struct {
		a, b string
		want bool
	}{
		{"cube", "cube", true},
		{"cube", "half-0", true},
		{"half-1", "cube", true},
		{"half-0", "half-1", false},
		{"cube", "other", false},
		{"cube", "unknown", false},
	}
	for _, tt := range overlaps {
		if got := pc.Overlaps(tt.a, tt.b); got != tt.want {
			t.Errorf("Overlaps(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}

	held := &interfaces.SliceInfo{Name: "slice-a", UID: "uid-a", PartitionIds: []string{"half-0", "half-1"}}
	if pc.Holds(held) {
		t.Errorf("Holds() = true before reserving")
	}
	if ok, _ := pc.ReservePartition(ctx, held); !ok {
		t.Fatalf("ReservePartition() failed")
	}
	if !pc.Holds(held) {
		t.Errorf("Holds() = false after reserving")
	}
	successor := &interfaces.SliceInfo{Name: "slice-a", UID: "uid-b", PartitionIds: []string{"half-0", "half-1"}}
	if pc.Holds(successor) {
		t.Errorf("Holds() = true for a same-name Slice with a different UID")
	}
	partly := &interfaces.SliceInfo{Name: "slice-a", UID: "uid-a", PartitionIds: []string{"half-0", "other"}}
	if pc.Holds(partly) {
		t.Errorf("Holds() = true when only some partitions are held")
	}
}

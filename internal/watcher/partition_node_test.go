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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

// ---------------------------------------------------------------------------
// Helpers: build a PartitionNode with common defaults for tests.
// ---------------------------------------------------------------------------
func newPN(id, topology string, state partitiontreev1.PartitionStateType) *PartitionNode {
	return &PartitionNode{
		Info: &partitiontreev1.PartitionInfo{
			PartitionID: id,
			Topology:    topology,
			State:       state,
		},
	}
}

func newHealthyPN(id, topology string) *PartitionNode {
	return newPN(id, topology, partitiontreev1.PartitionStateHealthy)
}

func newLeafPN(id, topology, nodeName string) *PartitionNode {
	return &PartitionNode{
		Info: &partitiontreev1.PartitionInfo{
			PartitionID: id,
			Topology:    topology,
			State:       partitiontreev1.PartitionStateHealthy,
			NodeNames:   []string{nodeName},
			Conditions:  []metav1.Condition{{Type: partitiontreev1.ConditionTypeNodeReady, Status: metav1.ConditionTrue}},
		},
		KubeNode: &KubeNode{Name: nodeName, Ready: true},
	}
}

// ---------------------------------------------------------------------------
// Tests: KubeNode.IsReady
// ---------------------------------------------------------------------------
func TestKubeNode_IsReady(t *testing.T) {
	tests := []struct {
		name  string
		ready bool
		want  bool
	}{
		{"Ready node", true, true},
		{"Not ready node", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kn := &KubeNode{Name: "n1", Ready: tt.ready}
			if got := kn.IsReady(); got != tt.want {
				t.Errorf("KubeNode.IsReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: PartitionNode.IsUsed
// ---------------------------------------------------------------------------
func TestPartitionNode_IsUsed(t *testing.T) {
	tests := []struct {
		name    string
		inUseBy *interfaces.SliceInfo
		want    bool
	}{
		{"Not in use", nil, false},
		{"In use", &interfaces.SliceInfo{Name: "slice-1"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := newHealthyPN("p1", "4x4")
			pn.InUseBy = tt.inUseBy
			if got := pn.IsUsed(); got != tt.want {
				t.Errorf("IsUsed() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: isLeafPartitionNode
// ---------------------------------------------------------------------------
// Leafness is a property of the tree, so every case here varies the children
// and the node names independently: a childless partition is a leaf whatever
// its CRD happens to list, and a partition with children is not a leaf even if
// it lists exactly one node name.
func TestIsLeafPartitionNode(t *testing.T) {
	tests := []struct {
		name      string
		nodeNames []string
		children  []*PartitionNode
		want      bool
	}{
		{"Leaf - single node name", []string{"h1"}, nil, true},
		{"Leaf - no node names", nil, nil, true},
		{"Leaf - empty node names", []string{}, nil, true},
		{"Leaf - multiple node names", []string{"h1", "h2"}, nil, true},
		{
			name:      "Non-leaf - has a child",
			nodeNames: nil,
			children:  []*PartitionNode{newHealthyPN("c1", "2x2")},
			want:      false,
		},
		{
			name:      "Non-leaf - has a child and one node name",
			nodeNames: []string{"h1"},
			children:  []*PartitionNode{newHealthyPN("c1", "2x2")},
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := newHealthyPN("p1", "4x4")
			pn.Info.NodeNames = tt.nodeNames
			pn.Children = tt.children
			if got := pn.isLeafPartitionNode(); got != tt.want {
				t.Errorf("isLeafPartitionNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: isKubeNodeSchedulable
// ---------------------------------------------------------------------------
func TestIsKubeNodeSchedulable(t *testing.T) {
	tests := []struct {
		name                string
		pn                  *PartitionNode
		enableNodeReadiness bool
		want                bool
	}{
		{
			name:                "NodeReadiness disabled - always schedulable",
			pn:                  newHealthyPN("p1", "4x4"),
			enableNodeReadiness: false,
			want:                true,
		},
		{
			name: "NodeReadiness enabled, not leaf - always schedulable",
			pn: func() *PartitionNode {
				pn := newHealthyPN("p1", "4x4")
				pn.Children = []*PartitionNode{newLeafPN("c1", "2x2", "h1")}
				return pn
			}(),
			enableNodeReadiness: true,
			want:                true,
		},
		{
			// A childless partition is a leaf however many node names it lists,
			// so it must answer the readiness question rather than skip it. With
			// no backing KubeNode there is nothing to prove it ready.
			name:                "NodeReadiness enabled, childless with no node names",
			pn:                  newHealthyPN("p1", "4x4"),
			enableNodeReadiness: true,
			want:                false,
		},
		{
			name: "NodeReadiness enabled, childless with multiple node names",
			pn: func() *PartitionNode {
				pn := newHealthyPN("p1", "4x4")
				pn.Info.NodeNames = []string{"h1", "h2"}
				return pn
			}(),
			enableNodeReadiness: true,
			want:                false,
		},
		{
			name:                "NodeReadiness enabled, leaf with NodeReady True",
			pn:                  newLeafPN("p1", "4x4", "h1"),
			enableNodeReadiness: true,
			want:                true,
		},
		{
			name: "NodeReadiness enabled, leaf with NodeReady False",
			pn: func() *PartitionNode {
				pn := newLeafPN("p1", "4x4", "h1")
				pn.Info.Conditions[0].Status = metav1.ConditionFalse
				return pn
			}(),
			enableNodeReadiness: true,
			want:                false,
		},
		{
			// The KubeNode cache is not consulted: a ready KubeNode doesn't make up for a
			// missing condition.
			name: "NodeReadiness enabled, leaf without NodeReady but with ready KubeNode",
			pn: func() *PartitionNode {
				pn := newLeafPN("p1", "4x4", "h1")
				pn.Info.Conditions = nil
				return pn
			}(),
			enableNodeReadiness: true,
			want:                false,
		},
		{
			// Nor does a missing or unready KubeNode override the condition, as when the
			// scheduler never saw the node's events.
			name: "NodeReadiness enabled, leaf with NodeReady True and nil KubeNode",
			pn: func() *PartitionNode {
				pn := newLeafPN("p1", "4x4", "h1")
				pn.KubeNode = nil
				return pn
			}(),
			enableNodeReadiness: true,
			want:                true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.pn.isKubeNodeSchedulable(tt.enableNodeReadiness); got != tt.want {
				t.Errorf("isKubeNodeSchedulable(%v) = %v, want %v", tt.enableNodeReadiness, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: isBroken
// ---------------------------------------------------------------------------
func TestIsBroken(t *testing.T) {
	tests := []struct {
		name                string
		state               partitiontreev1.PartitionStateType
		enableNodeReadiness bool
		leafNotReady        bool
		want                bool
	}{
		{"Healthy - not broken", partitiontreev1.PartitionStateHealthy, false, false, false},
		{"Unhealthy - broken", partitiontreev1.PartitionStateUnhealthy, false, false, true},
		{"Unknown - broken", partitiontreev1.PartitionStateUnknown, false, false, true},
		{"Degraded - not broken", partitiontreev1.PartitionStateDegraded, false, false, false},
		{"Incomplete - broken", partitiontreev1.PartitionStateIncomplete, false, false, true},
		{"Healthy but node not ready - broken", partitiontreev1.PartitionStateHealthy, true, true, true},
		{"Healthy, node readiness disabled, node not ready - not broken", partitiontreev1.PartitionStateHealthy, false, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := newPN("p1", "4x4", tt.state)
			if tt.leafNotReady {
				pn.Info.NodeNames = []string{"h1"}
				pn.Info.Conditions = []metav1.Condition{{Type: partitiontreev1.ConditionTypeNodeReady, Status: metav1.ConditionFalse}}
			}
			if got := pn.isBroken(tt.enableNodeReadiness); got != tt.want {
				t.Errorf("isBroken(%v) = %v, want %v", tt.enableNodeReadiness, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: SliceInfo.DeepCopy
// ---------------------------------------------------------------------------
func TestSliceInfoDeepCopy(t *testing.T) {
	tests := []struct {
		name  string
		input *interfaces.SliceInfo
	}{
		{"Nil input", nil},
		{
			"Non-nil input",
			&interfaces.SliceInfo{
				Name:         "s1",
				Priority:     42,
				Topology:     "4x4",
				Type:         "tpu",
				PartitionIds: []string{"p1", "p2"},
				ManagedBy:    "scheduler",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.DeepCopy()
			if tt.input == nil {
				if got != nil {
					t.Errorf("DeepCopy(nil) = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("DeepCopy() = nil, want non-nil")
			}
			if got == tt.input {
				t.Error("DeepCopy() returned same pointer, want a copy")
			}
			if got.Name != tt.input.Name {
				t.Errorf("Name = %q, want %q", got.Name, tt.input.Name)
			}
			if got.Priority != tt.input.Priority {
				t.Errorf("Priority = %d, want %d", got.Priority, tt.input.Priority)
			}
			if got.Topology != tt.input.Topology {
				t.Errorf("Topology = %q, want %q", got.Topology, tt.input.Topology)
			}
			if got.Type != tt.input.Type {
				t.Errorf("Type = %q, want %q", got.Type, tt.input.Type)
			}
			if got.ManagedBy != tt.input.ManagedBy {
				t.Errorf("ManagedBy = %q, want %q", got.ManagedBy, tt.input.ManagedBy)
			}
			// Verify PartitionIds are the same content
			if len(got.PartitionIds) != len(tt.input.PartitionIds) {
				t.Fatalf("PartitionIds length = %d, want %d", len(got.PartitionIds), len(tt.input.PartitionIds))
			}
			for i := range tt.input.PartitionIds {
				if got.PartitionIds[i] != tt.input.PartitionIds[i] {
					t.Errorf("PartitionIds[%d] = %q, want %q", i, got.PartitionIds[i], tt.input.PartitionIds[i])
				}
			}
		})
	}
}

// TestSliceInfoDeepCopy_Independence verifies that modifying the original does not
// affect the copy for scalar fields.
func TestSliceInfoDeepCopy_Independence(t *testing.T) {
	original := &interfaces.SliceInfo{
		Name:         "s1",
		Priority:     10,
		PartitionIds: []string{"p1", "p2"},
	}
	copied := original.DeepCopy()

	// Modify original scalar fields
	original.Name = "modified"
	original.Priority = 999

	// Verify copy is independent for scalars
	if copied.Name != "s1" {
		t.Errorf("Copy Name changed to %q after original modification", copied.Name)
	}
	if copied.Priority != 10 {
		t.Errorf("Copy Priority changed to %d after original modification", copied.Priority)
	}
}

// TestSliceInfoDeepCopy_PartitionIdsIndependence verifies that DeepCopy performs
// a deep copy of PartitionIds (independent slice).
func TestSliceInfoDeepCopy_PartitionIdsIndependence(t *testing.T) {
	original := &interfaces.SliceInfo{
		Name:         "s1",
		PartitionIds: []string{"p1", "p2"},
	}
	copied := original.DeepCopy()
	if copied == nil {
		t.Fatal("DeepCopy() = nil, want non-nil")
	}
	if len(copied.PartitionIds) != 2 {
		t.Fatalf("PartitionIds length = %d, want 2", len(copied.PartitionIds))
	}
	if copied.PartitionIds[0] != "p1" || copied.PartitionIds[1] != "p2" {
		t.Errorf("PartitionIds = %v, want [p1 p2]", copied.PartitionIds)
	}
	// Verify independence: modifying original does not affect copy
	original.PartitionIds[0] = "modified"
	if copied.PartitionIds[0] != "p1" {
		t.Errorf("Copy PartitionIds[0] changed to %q after original modification", copied.PartitionIds[0])
	}
}

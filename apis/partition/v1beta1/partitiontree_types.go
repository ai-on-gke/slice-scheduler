/*
Copyright 2026 Google LLC.

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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PartitionStateType string

const (
	PartitionStateUnknown    PartitionStateType = "Unknown"
	PartitionStateUnhealthy  PartitionStateType = "Unhealthy"
	PartitionStateHealthy    PartitionStateType = "Healthy"
	PartitionStateDegraded   PartitionStateType = "Degraded"
	PartitionStateIncomplete PartitionStateType = "Incomplete"
)

type OccupancyState string

const (
	OccupancyUnspecified OccupancyState = "Unspecified"
	OccupancyEmpty       OccupancyState = "Empty"
	OccupancyOccupied    OccupancyState = "Occupied"
)

const (
	ConditionTypeNodeReady   = "NodeReady"
	ConditionTypeSynced      = "Synced"
	ConditionTypeTaintsValid = "TaintsValid"
)

// +kubebuilder:validation:Enum=NoSchedule;PreferNoSchedule;NoExecute
type TaintEffect string

const (
	TaintEffectNoSchedule       TaintEffect = "NoSchedule"
	TaintEffectPreferNoSchedule TaintEffect = "PreferNoSchedule"
	TaintEffectNoExecute        TaintEffect = "NoExecute"
)

type PartitionTaint struct {
	Key    string      `json:"key"`
	Value  string      `json:"value,omitempty"`
	Effect TaintEffect `json:"effect"`
	// TimeAdded is the timestamp when this taint (or, for node-health NoExecute
	// taints on a leaf, the continuous node-health NoExecute episode) was first
	// observed on the partition.
	// +optional
	TimeAdded *metav1.Time `json:"timeAdded,omitempty"`
}

// PartitionInfo is a node in the partition tree. Leaves carry NodeNames;
// intermediate and root partitions carry ChildPartitions.
type PartitionInfo struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="PartitionID is immutable"
	PartitionID string `json:"partitionId"`
	// +optional
	Topology string             `json:"topology,omitempty"`
	State    PartitionStateType `json:"state"`
	// +optional
	Occupancy OccupancyState `json:"occupancy,omitempty"`
	// NodeNames is populated only on leaf partitions.
	// +optional
	NodeNames []string `json:"nodeNames,omitempty"`
	// ChildPartitions holds the recursive sub-tree. The field is marked
	// schemaless because controller-gen cannot emit OpenAPI for recursive
	// Go types; a validating webhook can tighten this later.
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	ChildPartitions []PartitionInfo `json:"childPartitions,omitempty"`
	// +optional
	SliceId string `json:"sliceId,omitempty"`
	// Conditions describes per-partition state. NodeReady is only set on
	// leaves and never bubbles up. The Synced condition lives at the
	// top-level PartitionTreeStatus, not here.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Taints represents the taints applied to this partition via the external
	// partitiontree.gke.io/taints annotation.
	// +optional
	Taints []PartitionTaint `json:"taints,omitempty"`
}

// PartitionTreeStatus is the status block for the PartitionTree CR. It
// wraps the recursive partition tree (Root) and carries top-level
// conditions that apply to the CR as a whole, not to individual
// partitions. The Synced condition lives here because GCE API freshness
// is a property of the entire tree, not a per-partition attribute.
type PartitionTreeStatus struct {
	// AcceleratorType is the TPU generation family (e.g. "v6e", "v7x")
	// for this MIG. It is uniform across the entire tree because a MIG
	// is homogeneous by definition. Derived from the node label
	// cloud.google.com/gke-tpu-accelerator; empty if unknown.
	// +optional
	AcceleratorType string `json:"acceleratorType,omitempty"`
	// Root is the root of the partition tree.
	// +optional
	Root *PartitionInfo `json:"root,omitempty"`
	// Conditions are top-level conditions for the PartitionTree CR.
	// Currently only Synced is set here.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status

// PartitionTree represents one GCE MIG as a hierarchical tree of TPU
// partitions. The tree itself lives under .status so it is written via the
// /status subresource.
type PartitionTree struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Status holds the partition tree and top-level conditions. Updates
	// flow through the /status subresource and cannot race with metadata
	// edits.
	// +optional
	Status PartitionTreeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PartitionTreeList contains a list of PartitionTree
type PartitionTreeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PartitionTree `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PartitionTree{}, &PartitionTreeList{})
}

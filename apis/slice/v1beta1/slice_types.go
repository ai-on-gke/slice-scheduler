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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
type Type string

const (
	TypeV6e   Type = "v6e"
	TypeTpu7x Type = "tpu7x"
)

func (t Type) String() string {
	return string(t)
}

// SliceSpec defines the desired state of Slice.
type SliceSpec struct {
	// Type specifies the type of accelerator used in this slice, e.g., "v6e", "tpu7x".
	// +kubebuilder:validation:Immutable
	// +kubebuilder:validation:Enum=v6e;tpu7x
	Type Type `json:"type"`

	// Topology represents the network topology of the slice.
	// It defines the physical arrangement of TPU chips in a 2D or 3D mesh.
	// The topology must be specified in `<X>x<Y>` or `<X>x<Y>x<Z>` format.
	// Each dimension is limited to four digits so that the scheduler can multiply
	// the dimensions together without overflowing.
	// +kubebuilder:validation:Immutable
	// +kubebuilder:validation:MaxLength=14
	// +kubebuilder:validation:Pattern=`^\d{1,4}x\d{1,4}(x\d{1,4})?$`
	Topology string `json:"topology"`

	// PartitionIds denotes the set of partitions to use to form a slice
	// For slices that span multiple partitions, it will be a list of 4x4x4 IDs
	// For sub-partition topology, it will be a single entry corresponding to the ID of the partition
	PartitionIds []string `json:"partitionIds"`
}

// SliceStatus defines the observed state of Slice.
type SliceStatus struct {
	// Conditions store the status conditions of the Slice
	// +operator-sdk:csv:customresourcedefinitions:type=status
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the most recent generation observed by the slice-controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Topology",type=string,JSONPath=`.spec.topology`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// Slice is the Schema for the slices API.
type Slice struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SliceSpec   `json:"spec,omitempty"`
	Status SliceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SliceList contains a list of Slice.
type SliceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Slice `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Slice{}, &SliceList{})
}

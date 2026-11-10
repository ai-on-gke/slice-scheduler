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

package scheduler

import (
	"context"
	"testing"

	partitiontree "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetSlicePriority(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        int
	}{
		{
			name:        "no annotations",
			annotations: nil,
			want:        0,
		},
		{
			name:        "no priority annotation",
			annotations: map[string]string{"other": "value"},
			want:        0,
		},
		{
			name:        "valid priority",
			annotations: map[string]string{SlicePriorityAnnotation: "500"},
			want:        500,
		},
		{
			name:        "zero priority",
			annotations: map[string]string{SlicePriorityAnnotation: "0"},
			want:        0,
		},
		{
			name:        "negative priority",
			annotations: map[string]string{SlicePriorityAnnotation: "-10"},
			want:        -10,
		},
		{
			name:        "invalid priority (non-numeric)",
			annotations: map[string]string{SlicePriorityAnnotation: "abc"},
			want:        0,
		},
		{
			name:        "invalid priority (float)",
			annotations: map[string]string{SlicePriorityAnnotation: "3.14"},
			want:        0,
		},
		{
			name:        "invalid priority (empty)",
			annotations: map[string]string{SlicePriorityAnnotation: ""},
			want:        0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-slice",
					Annotations: tt.annotations,
				},
			}
			got := getSlicePriority(context.Background(), slice)
			if got != tt.want {
				t.Errorf("getSlicePriority() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGetSliceGroupName(t *testing.T) {
	tests := []struct {
		name        string
		sliceName   string
		annotations map[string]string
		want        string
	}{
		{
			name:      "no annotation, returns slice name",
			sliceName: "my-slice",
			want:      "my-slice",
		},
		{
			name:        "has slice-group-name annotation",
			sliceName:   "my-slice",
			annotations: map[string]string{SliceGroupNameAnnotation: "my-workload"},
			want:        "my-workload",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        tt.sliceName,
					Annotations: tt.annotations,
				},
			}
			got := getSliceGroupName(slice)
			if got != tt.want {
				t.Errorf("getSliceGroupName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetNumSlices(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        int
	}{
		{
			name: "no annotation, default 1",
			want: 1,
		},
		{
			name:        "valid num-slices",
			annotations: map[string]string{NumSlicesAnnotation: "3"},
			want:        3,
		},
		{
			name:        "invalid num-slices",
			annotations: map[string]string{NumSlicesAnnotation: "abc"},
			want:        1,
		},
		{
			name:        "empty num-slices",
			annotations: map[string]string{NumSlicesAnnotation: ""},
			want:        1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-slice",
					Annotations: tt.annotations,
				},
			}
			got := getNumSlices(context.Background(), slice)
			if got != tt.want {
				t.Errorf("getNumSlices() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPartitionTreeType(t *testing.T) {
	tests := []struct {
		name string
		tree *partitiontree.PartitionTree
		want string
	}{
		{
			name: "accelerator type v7x",
			tree: &partitiontree.PartitionTree{
				Status: partitiontree.PartitionTreeStatus{
					AcceleratorType: "v7x",
				},
			},
			want: TPU7x,
		},
		{
			name: "accelerator type v6e",
			tree: &partitiontree.PartitionTree{
				Status: partitiontree.PartitionTreeStatus{
					AcceleratorType: "v6e",
				},
			},
			want: "v6e",
		},
		{
			name: "no accelerator type, ironwood topology",
			tree: &partitiontree.PartitionTree{
				Status: partitiontree.PartitionTreeStatus{
					Root: &partitiontree.PartitionInfo{
						Topology: "IRONWOOD_4x4x4",
					},
				},
			},
			want: TPU7x,
		},
		{
			name: "no accelerator type, trillium topology",
			tree: &partitiontree.PartitionTree{
				Status: partitiontree.PartitionTreeStatus{
					Root: &partitiontree.PartitionInfo{
						Topology: "TRILLIUM_4x4",
					},
				},
			},
			want: TPU6e,
		},
		{
			name: "no accelerator type, nil root",
			tree: &partitiontree.PartitionTree{
				Status: partitiontree.PartitionTreeStatus{},
			},
			want: "",
		},
		{
			name: "no accelerator type, unknown topology",
			tree: &partitiontree.PartitionTree{
				Status: partitiontree.PartitionTreeStatus{
					Root: &partitiontree.PartitionInfo{
						Topology: "UNKNOWN_8x8",
					},
				},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := partitionTreeType(tt.tree)
			if got != tt.want {
				t.Errorf("partitionTreeType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSliceInfo(t *testing.T) {
	slice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-slice",
			UID:  "test-slice-uid",
			Annotations: map[string]string{
				SlicePriorityAnnotation:                       "42",
				"cloud.google.com/slice-scheduler-managed-by": "slice-scheduler",
			},
		},
		Spec: slicev1beta1.SliceSpec{
			Topology:     "4x4x4",
			Type:         "tpu7x",
			PartitionIds: []string{"p1", "p2"},
		},
	}

	info := sliceInfo(context.Background(), slice)

	if info.Name != "test-slice" {
		t.Errorf("Name = %q, want %q", info.Name, "test-slice")
	}
	if info.UID != "test-slice-uid" {
		t.Errorf("UID = %q, want %q", info.UID, "test-slice-uid")
	}
	if info.Priority != 42 {
		t.Errorf("Priority = %d, want %d", info.Priority, 42)
	}
	if info.Topology != "4x4x4" {
		t.Errorf("Topology = %q, want %q", info.Topology, "4x4x4")
	}
	if info.Type != "tpu7x" {
		t.Errorf("Type = %q, want %q", info.Type, "tpu7x")
	}
	if len(info.PartitionIds) != 2 {
		t.Errorf("PartitionIds len = %d, want 2", len(info.PartitionIds))
	}
}

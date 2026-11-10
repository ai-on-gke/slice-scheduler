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

package taint

import (
	"context"
	"testing"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	corev1 "k8s.io/api/core/v1"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

func TestTaintFilter(t *testing.T) {
	filter := &TaintFilter{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{}

	if filter.Name() != "TaintFilter" {
		t.Errorf("TaintFilter.Name() = %v, want TaintFilter", filter.Name())
	}

	taints := func(ts ...*partitiontreev1.PartitionTaint) *interfaces.PartitionInfo {
		return &interfaces.PartitionInfo{Taints: ts}
	}

	tests := []struct {
		name     string
		node     *interfaces.PartitionInfo
		expected bool
	}{
		{
			name:     "Nil node",
			node:     nil,
			expected: false,
		},
		{
			name:     "Nil taints",
			node:     taints(),
			expected: true,
		},
		{
			name:     "Nil taint entry is ignored",
			node:     taints(nil),
			expected: true,
		},
		{
			name: "Any key with NoSchedule",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: "custom.taint.io/unregistered", Effect: partitiontreev1.TaintEffectNoSchedule,
			}),
			expected: false,
		},
		{
			name: "Any key with NoExecute",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: "custom.taint.io/unregistered", Effect: partitiontreev1.TaintEffectNoExecute,
			}),
			expected: false,
		},
		{
			name: "Any key with PreferNoSchedule",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: "custom.taint.io/unregistered", Effect: partitiontreev1.TaintEffectPreferNoSchedule,
			}),
			expected: true,
		},
		{
			name: "Blocked key with unblocked PreferNoSchedule effect",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: corev1.TaintNodeNotReady, Effect: partitiontreev1.TaintEffectPreferNoSchedule,
			}),
			expected: true,
		},
		{
			name: "Blocked key not-ready with NoSchedule",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: corev1.TaintNodeNotReady, Effect: partitiontreev1.TaintEffectNoSchedule,
			}),
			expected: false,
		},
		{
			name: "Blocked key unreachable with NoExecute",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: corev1.TaintNodeUnreachable, Effect: partitiontreev1.TaintEffectNoExecute,
			}),
			expected: false,
		},
		{
			name: "Blocked key unschedulable with NoSchedule",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: corev1.TaintNodeUnschedulable, Effect: partitiontreev1.TaintEffectNoSchedule,
			}),
			expected: false,
		},
		{
			name: "Incomplete partition",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: interfaces.TaintKeyPartitionState, Value: "Incomplete", Effect: partitiontreev1.TaintEffectNoSchedule,
			}),
			expected: false,
		},
		{
			name: "Degraded partition is advisory",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: interfaces.TaintKeyPartitionState, Value: "Degraded", Effect: partitiontreev1.TaintEffectPreferNoSchedule,
			}),
			expected: true,
		},
		{
			name: "Nodes not found",
			node: taints(&partitiontreev1.PartitionTaint{
				Key: interfaces.TaintKeyNodesNotFound, Effect: partitiontreev1.TaintEffectNoExecute,
			}),
			expected: false,
		},
		{
			name: "PreferNoSchedule taint alongside a blocked one",
			node: taints(
				&partitiontreev1.PartitionTaint{Key: "custom.taint.io/x", Effect: partitiontreev1.TaintEffectPreferNoSchedule},
				&partitiontreev1.PartitionTaint{Key: corev1.TaintNodeNotReady, Effect: partitiontreev1.TaintEffectNoExecute},
			),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filter.Filter(ctx, req, tt.node); got != tt.expected {
				t.Errorf("TaintFilter.Filter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

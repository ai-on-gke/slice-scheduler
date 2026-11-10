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
	"sync"
	"time"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

type KubeNode struct {
	Name string
	// Ready is no longer read when scheduling; isKubeNodeSchedulable uses the leaf's
	// NodeReady condition instead.
	// TODO: remove Ready along with the Node watch.
	Ready           bool
	TestingNotReady bool
}

// PartitionNode is an in-memory representation of a partition, tracking its state and usage.
type PartitionNode struct {
	Info     *partitiontreev1.PartitionInfo
	InUseBy  *interfaces.SliceInfo // The slice using this partition, if any. This field does not consider the descendant in use case. The partitionInfo.STATE_IN_USE will be set if any of the descendant is in use.
	PodName  string                // The name of the TPU pod the partition belongs to.
	Parent   *PartitionNode
	Children []*PartitionNode
	KubeNode *KubeNode // KubeNode encapsulates the information from k8s node. This field is only set on the leaf node.
	// Taints is the union of this partition's own taints and those of every descendant:
	// a partition contains its descendants' hardware, so their taints apply to it too.
	// The partition-tree controller records a taint only where it originates, so the cache
	// does the union. Only set when taint filtering is enabled.
	Taints []partitiontreev1.PartitionTaint
	// fallbackNodeHealthFirstSeen records the earliest continuous episode start across
	// node-health NoExecute taints (not-ready or unreachable) on this partition: seeded from
	// explicit PartitionTaint.TimeAdded when present, or cache observation time when TimeAdded
	// is nil, preserving the toleration clock across controller upgrades and downgrades.
	fallbackNodeHealthFirstSeen time.Time
}

// PartitionCache holds the state of all partitions of a specific type (e.g., Trillium or Ironwood).
type PartitionCache struct {
	// partitionTreeCache stores the in-memory representation of all partition trees. The key is the name of the PartitionTree CR.
	partitionTreeCache map[string]*PartitionNode
	// partitionNodeCache provides fast lookups from a partition ID to its node in the tree.
	partitionNodeCache map[string]*PartitionNode
	// leafPartitionNodeCache provides fast lookups from a kube node name to a partition node.
	leafPartitionNodeCache              map[string]*PartitionNode
	mu                                  sync.RWMutex
	enableNodeReadiness                 bool // If true, the scheduler will consider the node presence and readiness for scheduling.
	enableTaintFiltering                bool // If true, the scheduler will consider PartitionTree taints for scheduling.
	defaultNotReadyTolerationSeconds    int64
	defaultUnreachableTolerationSeconds int64
	now                                 func() time.Time
}

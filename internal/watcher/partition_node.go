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
	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
)

const (
	ManagedByAnnotation = "cloud.google.com/managed-by"
)

func (kn *KubeNode) IsReady() bool {
	return kn.Ready
}

func (pn *PartitionNode) isTestingNotReady() bool {
	if pn.isLeafPartitionNode() {
		return pn.KubeNode != nil && pn.KubeNode.TestingNotReady
	}
	for _, child := range pn.Children {
		if child.isTestingNotReady() {
			return true
		}
	}
	return false
}

// isKubeNodeSchedulable reports whether a leaf's k8s node is ready, read from the leaf's
// NodeReady condition, which the partition-tree controller sets from its own Node watch.
// A leaf without the condition is not ready. The scheduler's KubeNode cache is not used
// here: it is filled only by Node events and node lists that miss a tree created after
// its nodes, or nodes that gain their partition label after they register, and that left
// every leaf of such a tree unschedulable until a restart.
func (pn *PartitionNode) isKubeNodeSchedulable(enableNodeReadiness bool) bool {
	if enableNodeReadiness && pn.isLeafPartitionNode() {
		return meta.IsStatusConditionTrue(pn.Info.Conditions, partitiontreev1.ConditionTypeNodeReady)
	}
	return true
}

// isLeafPartitionNode reports whether this node is a leaf of the partition
// tree. Leafness is a property of the tree, not of how many kube nodes the CRD
// happens to list: defining it as len(NodeNames) == 1 answers false for a
// childless partition carrying zero or several node names, which then silently
// escapes the readiness checks in isKubeNodeSchedulable and isBroken.
func (p *PartitionNode) isLeafPartitionNode() bool {
	return len(p.Children) == 0
}

func (pn *PartitionNode) isBroken(enableNodeReadiness bool) bool {
	broken := pn.Info.State == partitiontreev1.PartitionStateUnhealthy ||
		pn.Info.State == partitiontreev1.PartitionStateUnknown ||
		pn.Info.State == partitiontreev1.PartitionStateIncomplete
	return broken || !pn.isKubeNodeSchedulable(enableNodeReadiness)
}

func (pn *PartitionNode) IsUsed() bool {
	return pn.InUseBy != nil
}

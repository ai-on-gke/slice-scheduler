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

package generators

import (
	"slices"
	"sort"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/util"
)

// findSinglePartitionOptions searches through the provided partition trees to find
// valid placements for a request of the specified topology. It dynamically
// performs a full subtree validation for each matching candidate by verifying that
// every PartitionInfo node exists within the eligiblePartitions map. This ensures
// candidates strictly conform to upstream filters (e.g. Managed, Priority).
// It discovers both zero-preemption and preemption candidates in a single unified traversal.
//
// Example Input:
//
//	trees: []interfaces.PartitionInfo{
//	    {
//	        ID: "cube-1", Topology: "4x4x4",
//	        Children: []interfaces.PartitionInfo{
//	            {ID: "cube-1-sub-1", Topology: "2x2x2", InUseBy: &interfaces.SliceInfo{Name: "victim-A"}},
//	            {ID: "cube-1-sub-2", Topology: "2x2x2"},
//	        },
//	    },
//	}
//	topology: "2x2x2"
//	eligiblePartitions: map[string]struct{}{"cube-1": {}, "cube-1-sub-1": {}, "cube-1-sub-2": {}}
//
// Example Output:
//
//	[]*interfaces.PlacementOption{
//	    {Partitions: []interfaces.PartitionInfo{{ID: "cube-1-sub-1", Topology: "2x2x2"}}, Victims: []*interfaces.SliceInfo{{Name: "victim-A"}}},
//	    {Partitions: []interfaces.PartitionInfo{{ID: "cube-1-sub-2", Topology: "2x2x2"}}, Victims: nil},
//	}
func findSinglePartitionOptions(trees []*interfaces.PartitionInfo, sliceNames []string, topology string, eligiblePartitions map[string]struct{}) []*interfaces.PlacementOption {
	var candidates []*interfaces.PlacementOption

	for _, root := range trees {
		nodeCandidates := find(root, topology, sliceNames, nil, eligiblePartitions)
		candidates = append(candidates, nodeCandidates...)
	}

	return candidates
}

// find returns all valid candidates in the subtree.
// pathVictim tracks a slice encountered on the path from root to this node.
// It uses a single pointer, as a node can only have at most one ancestor slice.
func find(node *interfaces.PartitionInfo, topology string, sliceNames []string, pathVictim *interfaces.SliceInfo, eligiblePartitions map[string]struct{}) []*interfaces.PlacementOption {
	// Pruning: A subtree can never satisfy a request larger than its root node.
	if util.ChipCount(topology) > util.ChipCount(node.Topology) {
		return nil
	}

	// Check if this specific node is eligible
	_, isEligible := eligiblePartitions[node.ID]

	// Update victim (if any) seen on the path to this node. Usually at most one slice occupies an ancestor.
	currentPathVictim := pathVictim
	if node.InUseBy != nil {
		if !isEligible {
			// If a node is in-use, and it fails filters (like Priority or Managed),
			// we CANNOT preempt it. Therefore, its entire subtree is physically locked.
			return nil
		}
		if !slices.Contains(sliceNames, node.InUseBy.Name) {
			currentPathVictim = node.InUseBy
		}
	}

	// Found exact match
	if node.Topology == topology {
		if !isEligible {
			return nil
		}

		// Collect victims and validate subtree.
		subtreeVictims := make(map[string]*interfaces.SliceInfo)
		if !validateAndCollectVictims(node, sliceNames, subtreeVictims, eligiblePartitions) {
			return nil
		}

		// Victims = (Slice on path to root) + (Slices in this node's subtree)
		allVictims := []*interfaces.SliceInfo{}
		if currentPathVictim != nil {
			allVictims = append(allVictims, currentPathVictim)
		}
		for _, v := range subtreeVictims {
			if currentPathVictim == nil || v.Name != currentPathVictim.Name {
				allVictims = append(allVictims, v)
			}
		}

		sort.Slice(allVictims, func(i, j int) bool {
			return allVictims[i].Name < allVictims[j].Name
		})

		return []*interfaces.PlacementOption{{
			Partitions: []*interfaces.PartitionInfo{node},
			Victims:    allVictims,
		}}
	}

	// Recurse down children... (even if parent failed filters, children might be healthy/valid)
	var candidates []*interfaces.PlacementOption
	for _, child := range node.Children {
		childCandidates := find(child, topology, sliceNames, currentPathVictim, eligiblePartitions)
		candidates = append(candidates, childCandidates...)
	}
	return candidates
}

// validateAndCollectVictims performs subtree validation and gathers victims.
func validateAndCollectVictims(node *interfaces.PartitionInfo, sliceNames []string, victims map[string]*interfaces.SliceInfo, eligiblePartitions map[string]struct{}) bool {
	if _, ok := eligiblePartitions[node.ID]; !ok {
		return false
	}

	if node.InUseBy != nil && !slices.Contains(sliceNames, node.InUseBy.Name) {
		victims[node.InUseBy.Name] = node.InUseBy
	}

	for _, child := range node.Children {
		if !validateAndCollectVictims(child, sliceNames, victims, eligiblePartitions) {
			return false
		}
	}
	return true
}

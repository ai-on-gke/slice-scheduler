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

package plugins

import (
	"context"
	"math"
	"sync"

	"github.com/ai-on-gke/slice-scheduler/internal/cost"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/util"
)

const (
	// densityWeightFreeRoots is the weight multiplier for free root partitions
	// in the composite density score. This ensures free-root count dominates
	// over total free chip count when comparing placement candidates.
	densityWeightFreeRoots = 10000
)

// PreemptionScorer prefers candidates that require fewer/cheaper preemptions.
type PreemptionScorer struct{}

func (s *PreemptionScorer) Name() string { return "PreemptionScorer" }

func (s *PreemptionScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	return cost.NewPreemptionCost(candidate.Victims)
}

type podDensity struct {
	totalFree int
	freeRoots int
}

// TightestFitScorer prioritizes placements in already fragmented trees to maximize bin-packing.
type TightestFitScorer struct {
	mu    sync.RWMutex
	state interfaces.ClusterState
	cache map[string]podDensity
}

func (s *TightestFitScorer) Name() string { return "TightestFitScorer" }

func (s *TightestFitScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	if len(req.Slices) == 0 {
		return cost.TightestFitCost{}
	}
	sliceReq := req.Slices[0]
	assignedPartitions := candidate.Assignments[sliceReq.SliceName]

	if len(assignedPartitions) == 0 {
		return cost.TightestFitCost{}
	}

	podName := assignedPartitions[0].PodName

	// 1. Calculate Primary Fit Score (Local constraint)
	minFitScore := math.MaxInt
	pathBuf := make([]*interfaces.PartitionInfo, 0, 16)
	for _, targetPart := range assignedPartitions {
		targetID := targetPart.ID
		pathBuf = pathBuf[:0]
		found := false
		for _, pt := range state.GetPartitionTrees() {
			if pt.PodName != podName {
				continue
			}
			if findPath(pt, targetID, &pathBuf) {
				found = true
				break
			}
		}
		if !found {
			continue
		}

		largestFreeAncestor := pathBuf[len(pathBuf)-1]
		for i := len(pathBuf) - 2; i >= 0; i-- {
			if isCompletelyFree(pathBuf[i]) {
				largestFreeAncestor = pathBuf[i]
			} else {
				break
			}
		}
		fit := util.ChipCount(largestFreeAncestor.Topology)
		if fit < minFitScore {
			minFitScore = fit
		}
	}
	// 2. Calculate Pod-level Density Score (Tie-breaker)
	s.mu.RLock()
	stateChanged := s.state != state
	var density podDensity
	var cached bool
	if !stateChanged {
		density, cached = s.cache[podName]
	}
	s.mu.RUnlock()

	if stateChanged || !cached {
		s.mu.Lock()
		if s.state != state {
			s.state = state
			s.cache = make(map[string]podDensity)
		}
		density, cached = s.cache[podName]
		if !cached {
			totalFreeInPod := 0
			freeRootsInPod := 0
			for _, pt := range state.GetPartitionTrees() {
				if pt.PodName == podName {
					chips := countFreeChips(pt)
					totalFreeInPod += chips
					if chips == util.ChipCount(pt.Topology) {
						freeRootsInPod++
					}
				}
			}
			density = podDensity{
				totalFree: totalFreeInPod,
				freeRoots: freeRootsInPod,
			}
			s.cache[podName] = density
		}
		s.mu.Unlock()
	}

	totalFreeInPod := density.totalFree
	freeRootsInPod := density.freeRoots

	return cost.TightestFitCost{
		FitScore:     minFitScore,
		DensityScore: freeRootsInPod*densityWeightFreeRoots + totalFreeInPod,
	}
}

// countFreeChips sums the chips of every completely-free partition in the
// subtree rooted at node.
//
// A partition held by a slice contributes nothing and neither can anything
// below it, so the walk stops there instead of descending. On a full cluster
// the density loop runs this over every tree in the pod, so the in-use subtrees
// it used to walk were the bulk of the traversal on exactly the busy clusters
// where scheduling latency matters.
//
// The early return is also what makes the "in use means no free chips" rule
// hold locally rather than by cooperation: PartitionWatcher.GetState propagates
// InUseBy from a node to its descendants (watcher.go:59-63), so today the
// recursion reaches the same answer only because every descendant repeats the
// parent's flag. Checking it here does not depend on that.
func countFreeChips(node *interfaces.PartitionInfo) int {
	if node == nil || node.InUseBy != nil {
		return 0
	}
	if isCompletelyFree(node) {
		return util.ChipCount(node.Topology)
	}
	count := 0
	for _, child := range node.Children {
		count += countFreeChips(child)
	}
	return count
}

// isCompletelyFree reports whether the partition and every descendant is
// healthy, untainted and unoccupied. A partition with a blocked taint can take
// no placement, so counting it as free would overstate the density of its pod.
//
// IsOccupied is checked at every level, not only on leaves. It is derived from
// the CRD's Occupancy field, which the partition controller sets on interior
// partitions too, and gating it on len(Children) == 0 let a mid-level occupied
// partition count as completely free. That both overstated the free-chip count
// of the pod holding it -- countFreeChips returns the whole subtree's chip
// count on a "free" node without descending -- and let the fit walk widen
// largestFreeAncestor past it, so the scorer was pulled toward exactly the
// trees it should have avoided.
func isCompletelyFree(node *interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	if node.IsBroken || node.HasBlockedTaint() || node.InUseBy != nil || node.IsOccupied {
		return false
	}

	for _, child := range node.Children {
		if !isCompletelyFree(child) {
			return false
		}
	}
	return true
}

func findPath(node *interfaces.PartitionInfo, targetID string, path *[]*interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	*path = append(*path, node)
	if node.ID == targetID {
		return true
	}
	for _, child := range node.Children {
		if findPath(child, targetID, path) {
			return true
		}
	}
	*path = (*path)[:len(*path)-1]
	return false
}

// buildRootDegradationIndex maps every partition ID in the snapshot to the
// degradation state of the root of the tree containing it.
//
// Degradation is a property of the OCS domain, which is the root cube, so a
// descendant is scored by its root and never by its own State field. The index
// records the root's value against every ID beneath it to preserve that.
//
// Trees are visited in GetPartitionTrees order and the first tree to claim an
// ID keeps it, reproducing the first-match behaviour of the per-lookup scan
// this replaces. IDs are unique cluster-wide, so the rule only decides
// malformed inputs. Only the map's keys are ever read; nothing iterates it, so
// it cannot leak map ordering into a placement decision.
func buildRootDegradationIndex(state interfaces.ClusterState) map[string]*interfaces.DegradationState {
	index := make(map[string]*interfaces.DegradationState)
	var pending []*interfaces.PartitionInfo
	for _, root := range state.GetPartitionTrees() {
		if root == nil {
			continue
		}
		rootDegradation := root.DegradationState
		pending = append(pending[:0], root)
		for len(pending) > 0 {
			node := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if _, claimed := index[node.ID]; !claimed {
				index[node.ID] = rootDegradation
			}
			for _, child := range node.Children {
				if child != nil {
					pending = append(pending, child)
				}
			}
		}
	}
	return index
}

// DegradationSinkScorer prefers candidates that use degraded cubes for workloads that tolerate it.
//
// Resolving a partition to its root's degradation state walked every tree in
// the cluster, per assigned partition, per candidate -- an O(candidates x
// partitions x trees) scan of the whole snapshot to answer a question with one
// answer per snapshot. The lookup is now served from an index built once per
// ClusterState, the same cache shape TightestFitScorer uses for its density
// figures.
type DegradationSinkScorer struct {
	mu    sync.RWMutex
	state interfaces.ClusterState
	index map[string]*interfaces.DegradationState
}

// rootDegradationState returns the degradation state of the root of the tree
// containing targetID, or nil if no tree contains it.
//
// A nil return is indistinguishable from a healthy root by design: the watcher
// populates PartitionInfo.DegradationState only for State == Degraded
// (watcher.go:79-84), so in this repo the value is either nil or Degraded, and
// both a miss and a healthy root must score as normal.
//
// ClusterState is an immutable snapshot that PartitionWatcher.GetState rebuilds
// for every scheduling cycle, so identity comparison is a sound invalidation
// signal: a new cycle is a new value, and nothing mutates a snapshot in place.
func (s *DegradationSinkScorer) rootDegradationState(targetID string, state interfaces.ClusterState) *interfaces.DegradationState {
	s.mu.RLock()
	if s.state == state {
		index := s.index
		s.mu.RUnlock()
		return index[targetID]
	}
	s.mu.RUnlock()

	s.mu.Lock()
	if s.state != state {
		s.state = state
		s.index = buildRootDegradationIndex(state)
	}
	// The map is published once and never mutated afterwards, so it is safe to
	// read outside the lock.
	index := s.index
	s.mu.Unlock()
	return index[targetID]
}

func (s *DegradationSinkScorer) Name() string { return "DegradationSinkScorer" }

func (s *DegradationSinkScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	if len(req.Slices) == 0 {
		return cost.DegradationCost(0)
	}

	// Determine the most restrictive tolerance and collect assigned partitions
	// only from slices that qualify for degradation scoring.
	// The scorer activates for sub-cube Ironwood OR tolerant workloads.
	var allAssignedPartitions []*interfaces.PartitionInfo
	for _, sliceReq := range req.Slices {
		// OCS degradation only applies to Ironwood (tpu7x).
		if sliceReq.Type != "tpu7x" {
			continue
		}

		isSubCube := util.ChipCount(sliceReq.Topology) < util.ChipsPerCube
		toleratesDegradation := sliceReq.DegradationTolerance == interfaces.DegradationToleranceAll

		if isSubCube || toleratesDegradation {
			allAssignedPartitions = append(allAssignedPartitions, candidate.Assignments[sliceReq.SliceName]...)
		}
	}

	if len(allAssignedPartitions) == 0 {
		return cost.DegradationCost(0)
	}

	// Degradation cost is the number of NORMAL partitions that are used.
	normalCount := 0
	for _, part := range allAssignedPartitions {
		degState := s.rootDegradationState(part.ID, state)
		if degState == nil || degState.Mode != interfaces.DegradationModeDegraded {
			normalCount++
		}
	}

	return cost.DegradationCost(normalCount * 1)
}

// MinimizeSwappedCubesScorer prefers candidates that swap/relocate fewer existing partitions.
type MinimizeSwappedCubesScorer struct{}

func (s *MinimizeSwappedCubesScorer) Name() string { return "MinimizeSwappedCubesScorer" }

// Score counts, per slice, the assigned partitions that the slice does not
// already hold.
//
// The membership test is deliberately scoped to the slice being scored rather
// than to the union of every CurrentPartitionIDs in the request. Under the
// union, handing slice A's existing partition to sibling slice B costs nothing
// -- the partition is in the union either way -- so an intra-group shuffle
// scores zero and becomes the *preferred* candidate. It is also a candidate
// Cloud cannot commit: assignPartitionsWithRetry refuses an N->M transition, so
// any slice that still holds partitions fails to bind and unwinds the whole
// group. Scoring per slice prices that shuffle at what it actually costs (A
// gives a partition up, B takes it on: two swaps) and steers the search away
// from it, while still costing zero for the intended reschedule case where a
// slice is handed back its own partitions.
func (s *MinimizeSwappedCubesScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	totalSwaps := 0
	for _, sliceReq := range req.Slices {
		if len(sliceReq.CurrentPartitionIDs) == 0 {
			continue
		}
		currentIDSet := make(map[string]bool, len(sliceReq.CurrentPartitionIDs))
		for _, id := range sliceReq.CurrentPartitionIDs {
			currentIDSet[id] = true
		}
		assigned := candidate.Assignments[sliceReq.SliceName]
		for _, part := range assigned {
			if !currentIDSet[part.ID] {
				totalSwaps++
			}
		}
	}
	return cost.SwapCost(totalSwaps)
}

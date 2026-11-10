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
	"context"
	"sort"

	"github.com/ai-on-gke/slice-scheduler/internal/cost"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/solver"
	"github.com/ai-on-gke/slice-scheduler/internal/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const defaultDegradationDomain = "DEFAULT_DEGRADATION_DOMAIN"

// getOcsDomain extracts the OCS domain name from a partition's degradation state.
// Returns empty string if the partition is not degraded.
//
// In Cloud (GKE), the watcher adapter (watcher.go) translates the CRD
// State="Degraded" into DegradationState{Mode: "DEGRADED", ToleratedOcsName:
// "DEFAULT_DEGRADATION_DOMAIN"} before the scheduler runs. This means:
//   - The ToleratedOcsName branch (below) always matches in Cloud.
//   - The empty-ToleratedOcsName fallback covers environments where real
//     per-OCS domain names exist and could theoretically be empty.
//   - The part.State fallback is defense-in-depth, redundant in Cloud since
//     the watcher already converts State → DegradationState.
func getOcsDomain(part *interfaces.PartitionInfo) string {
	if part.DegradationState != nil &&
		part.DegradationState.Mode == interfaces.DegradationModeDegraded {
		if part.DegradationState.ToleratedOcsName != "" {
			return part.DegradationState.ToleratedOcsName
		}
		return defaultDegradationDomain
	}

	// Handle environments that indicate degradation directly on the partition state.
	if part.State == interfaces.PartitionStateDegraded {
		return defaultDegradationDomain
	}

	return ""
}

// isNormal returns true if the partition is not degraded (i.e., has no OCS domain).
func isNormal(part *interfaces.PartitionInfo) bool {
	return getOcsDomain(part) == ""
}

func removeUsedCubes(remaining []*interfaces.PlacementOption, usedOpt *interfaces.PlacementOption) []*interfaces.PlacementOption {
	used := make(map[string]bool)
	for _, p := range usedOpt.Partitions {
		used[p.ID] = true
	}
	var nextRemaining []*interfaces.PlacementOption
	for _, c := range remaining {
		if len(c.Partitions) > 0 && !used[c.Partitions[0].ID] {
			nextRemaining = append(nextRemaining, c)
		}
	}
	return nextRemaining
}

// getOptionCost prices a whole multi-cube placement option so that options built
// in different OCS degradation domains can be compared against each other.
//
// The three components must mirror the per-item costs assembled by
// prepareSolverItems, otherwise generation and ranking disagree: the solver picks
// cubes using one cost definition while this cross-domain comparison ranks the
// resulting options using another. SwapCost is therefore the number of partitions
// the option would relocate -- those not already held by the requesting slice
// group -- which is the same quantity MinimizeSwappedCubesScorer computes over
// the final assignment.
//
// currentIDSet is the set built by findDisjointPlacementOptions. When it is empty
// every option of a given size scores the same swap count, so relative ordering
// is unaffected.
func getOptionCost(opt *interfaces.PlacementOption, currentIDSet map[string]bool) cost.DegradationPreemptionCost {
	preemption := cost.NewPreemptionCost(opt.Victims)
	normalCount := 0
	swapCount := 0
	for _, part := range opt.Partitions {
		if isNormal(part) {
			normalCount++
		}
		if !currentIDSet[part.ID] {
			swapCount++
		}
	}
	return cost.DegradationPreemptionCost{
		PreemptionCost:  preemption,
		SwapCost:        cost.SwapCost(swapCount),
		DegradationCost: cost.DegradationCost(normalCount),
	}
}

// newSolverCost constructs the appropriate solver.Cost.
func newSolverCost(preemption cost.PreemptionCost, swap cost.SwapCost, normalCount int) solver.Cost {
	return cost.DegradationPreemptionCost{
		PreemptionCost:  preemption,
		SwapCost:        swap,
		DegradationCost: cost.DegradationCost(normalCount),
	}
}

func findDisjointPlacementOptions(ctx context.Context, singleCubeOptions []*interfaces.PlacementOption, numCubes int, numPlacementOptions int, currentPartitionIDs []string) []*interfaces.PlacementOption {
	var result []*interfaces.PlacementOption
	logf := log.FromContext(ctx).WithName("findDisjointPlacementOptions")
	logf.V(4).Info("Selecting disjoint candidates", "availableCubesCount", len(singleCubeOptions), "numPlacementOptions", numPlacementOptions)
	remainingCubes := make([]*interfaces.PlacementOption, len(singleCubeOptions))
	copy(remainingCubes, singleCubeOptions)
	preemptedVictims := make(map[string]bool)
	currentIDSet := make(map[string]bool, len(currentPartitionIDs))
	for _, id := range currentPartitionIDs {
		currentIDSet[id] = true
	}

	for len(result) < numPlacementOptions {
		iteration := len(result) + 1
		logf.V(4).Info("Iteration start", "iteration", iteration, "remainingCubesCount", len(remainingCubes), "preemptedVictimsCount", len(preemptedVictims))

		// --- DEGRADATION: Group by Domain ---
		domainToOptions := make(map[string][]*interfaces.PlacementOption)
		for _, opt := range remainingCubes {
			if len(opt.Partitions) > 0 {
				part := opt.Partitions[0]
				domain := getOcsDomain(part)
				domainToOptions[domain] = append(domainToOptions[domain], opt)
			}
		}

		var evalDomains []string
		for d := range domainToOptions {
			if d != "" {
				evalDomains = append(evalDomains, d)
			}
		}
		sort.Strings(evalDomains)

		if len(evalDomains) == 0 {
			evalDomains = []string{""}
		}

		var bestOption *interfaces.PlacementOption
		var bestCost cost.DegradationPreemptionCost
		bestCostInitialized := false

		for _, domain := range evalDomains {
			var filteredPool []*interfaces.PlacementOption
			if domain != "" {
				filteredPool = append(filteredPool, domainToOptions[domain]...)
			}
			filteredPool = append(filteredPool, domainToOptions[""]...)

			if len(filteredPool) < numCubes {
				continue
			}

			option := findBestSingleCandidateOptimal(filteredPool, numCubes, preemptedVictims, currentIDSet)
			if option == nil {
				continue
			}

			optCost := getOptionCost(option, currentIDSet)
			if !bestCostInitialized || optCost.Less(bestCost) {
				bestOption = option
				bestCost = optCost
				bestCostInitialized = true
			}
		}

		if bestOption == nil {
			logf.V(4).Info("No more candidates found", "iteration", iteration)
			break
		}
		logf.V(4).Info("Selected candidate", "iteration", iteration, "cubes", len(bestOption.Partitions), "victimsCount", len(bestOption.Victims), "cost", bestCost)

		result = append(result, bestOption)
		for _, v := range bestOption.Victims {
			preemptedVictims[v.Name] = true
		}
		remainingCubes = removeUsedCubes(remainingCubes, bestOption)
	}

	return result
}

// solverMetadata carries the concrete domain models and properties associated with
// a generic solver.Item. This allows us to backtrack from the selected items
// to the physical cubes and victims they represent.
type solverMetadata struct {
	partitions []*interfaces.PartitionInfo
}

func prepareSolverItems(availableCubes []*interfaces.PlacementOption, preemptedVictims map[string]bool, currentIDSet map[string]bool) []solver.Item {
	largeVictims := make(map[string]*interfaces.SliceInfo)
	smallVictims := make(map[string]*interfaces.SliceInfo)
	victimPartitions := make(map[string][]*interfaces.PartitionInfo)
	for _, c := range availableCubes {
		if len(c.Partitions) == 0 {
			continue
		}
		part := c.Partitions[0]
		for _, v := range c.Victims {
			victimPartitions[v.Name] = append(victimPartitions[v.Name], part)
			if util.ChipCount(v.Topology) >= util.ChipsPerCube {
				largeVictims[v.Name] = v
			} else {
				smallVictims[v.Name] = v
			}
		}
	}
	items := make([]solver.Item, 0, len(availableCubes))
	processedPartitions := make(map[string]bool)
	victimNames := make([]string, 0, len(largeVictims))
	for name := range largeVictims {
		victimNames = append(victimNames, name)
	}
	sort.Strings(victimNames)
	for _, name := range victimNames {
		v := largeVictims[name]
		parts := victimPartitions[name]
		if len(parts) == 0 {
			continue
		}
		// Count normal partitions in this spanning item
		normalCount := 0
		swapCount := 0
		for _, part := range parts {
			if isNormal(part) {
				normalCount++
			}
			if !currentIDSet[part.ID] {
				swapCount++
			}
		}

		var preemptionCost = cost.NewPreemptionCost([]*interfaces.SliceInfo{v})
		if preemptedVictims[name] {
			preemptionCost = cost.PreemptionCost(nil)
		}

		var itemCost = newSolverCost(preemptionCost, cost.SwapCost(swapCount), normalCount)

		items = append(items, solver.Item{
			Name:   name,
			Weight: len(parts),
			Cost:   itemCost,
			Metadata: solverMetadata{
				partitions: parts,
			},
		})
		for _, part := range parts {
			processedPartitions[part.ID] = true
		}
	}
	for _, c := range availableCubes {
		if len(c.Partitions) == 0 {
			continue
		}
		part := c.Partitions[0]
		if processedPartitions[part.ID] {
			continue
		}
		var smallVictimsInCube []*interfaces.SliceInfo
		for _, v := range c.Victims {
			if _, isSmall := smallVictims[v.Name]; isSmall {
				if !preemptedVictims[v.Name] {
					smallVictimsInCube = append(smallVictimsInCube, v)
				}
			}
		}

		normalCount := 0
		if isNormal(part) {
			normalCount = 1
		}
		swapCount := 0
		if !currentIDSet[part.ID] {
			swapCount = 1
		}

		items = append(items, solver.Item{
			Name:   part.ID,
			Weight: 1,
			Cost:   newSolverCost(cost.NewPreemptionCost(smallVictimsInCube), cost.SwapCost(swapCount), normalCount),
			Metadata: solverMetadata{
				partitions: []*interfaces.PartitionInfo{part},
			},
		})
		processedPartitions[part.ID] = true
	}
	return items
}

// findBestSingleCandidateOptimal finds the absolute best single option of size numCubes
// using a mathematically optimal cost-minimizing generic solver with a degradation-aware cost vector.
//
// Example:
//
//	Available cubes: [c1(free), c2(victim: v1, pri=10), c3(victim: v2, pri=5)]
//	numCubes: 2
//
//	The solver finds the cheapest combination of 2 cubes considering their preemption
//	costs, preferring free cubes or those with lower-priority victims.
func findBestSingleCandidateOptimal(availableCubes []*interfaces.PlacementOption, numCubes int, preemptedVictims map[string]bool, currentIDSet map[string]bool) *interfaces.PlacementOption {
	if len(availableCubes) < numCubes {
		return nil
	}
	items := prepareSolverItems(availableCubes, preemptedVictims, currentIDSet)
	selectedItems, _ := solver.SolveCover(items, numCubes)
	if selectedItems == nil {
		return nil
	}
	var allPartitions []*interfaces.PartitionInfo
	for _, item := range selectedItems {
		meta := item.Metadata.(solverMetadata)
		allPartitions = append(allPartitions, meta.partitions...)
	}
	sort.Slice(allPartitions, func(i, j int) bool {
		return allPartitions[i].ID < allPartitions[j].ID
	})
	if len(allPartitions) > numCubes {
		allPartitions = allPartitions[:numCubes]
	}
	cubeVictims := make(map[string][]*interfaces.SliceInfo)
	for _, c := range availableCubes {
		if len(c.Partitions) > 0 {
			cubeVictims[c.Partitions[0].ID] = c.Victims
		}
	}
	finalVictimsMap := make(map[string]*interfaces.SliceInfo)
	for _, part := range allPartitions {
		for _, v := range cubeVictims[part.ID] {
			finalVictimsMap[v.Name] = v
		}
	}
	finalVictims := []*interfaces.SliceInfo{}
	for _, v := range finalVictimsMap {
		finalVictims = append(finalVictims, v)
	}
	sort.Slice(finalVictims, func(i, j int) bool {
		return finalVictims[i].Name < finalVictims[j].Name
	})
	return &interfaces.PlacementOption{
		Partitions: allPartitions,
		Victims:    finalVictims,
	}
}

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
	"math"
	"sort"
	"strings"

	"github.com/ai-on-gke/slice-scheduler/internal/cost"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/util"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GroupGeneratorConfig holds parameters for the beam search scheduling logic.
type GroupGeneratorConfig struct {
	BeamWidth int
}

// GroupGenerator acts as the entry point for scheduling slices.
type GroupGenerator struct {
	config GroupGeneratorConfig
}

// PartialGang represents an intermediate state during Beam Search expansion.
// In Tier 2 (Global Assembly), we schedule slices one by one. A PartialGang
// holds the candidate placements selected so far (stored in Candidates).
// To rank partial assemblies, it aggregates their preemption cost and superpod spread
// into Cost. MergedCandidate is a cached, merged representation of all the
// individual slice candidates, used to check for physical collision (overlapping partitions)
// during expansion and to serve as the final output when the gang is fully assembled.
type PartialGang struct {
	Options         []*interfaces.PlacementOption
	Cost            cost.PlacementCost
	MergedCandidate *interfaces.PlacementCandidate // Cached merged placement representing this gang
}

// NewPartialGang constructs a new PartialGang from a list of single-slice placement candidates.
// It pre-computes the aggregated metrics required for ranking, including the merged preemption cost
// (based on the union of all victims, deduplicated) and the superpod spread (unique pods spanned).
// It also caches the merged PlacementCandidate representing the combined allocation of all slices
// in the gang.
//
// Parameters:
//   - options: The list of placement options for each slice currently assembled.
//   - req: The original GroupAllocationRequest containing the slice configurations.
func NewPartialGang(
	options []*interfaces.PlacementOption,
	req interfaces.GroupAllocationRequest,
) PartialGang {
	merged := mergeOptions(options, req)

	return PartialGang{
		Options:         options,
		MergedCandidate: merged,
		Cost: cost.PlacementCost{
			Preemption: cost.NewPreemptionCost(merged.Victims),
			Spread:     countUniquePods(options),
		},
	}
}

// NewGroupGenerator creates a GroupGenerator with the given configuration.
func NewGroupGenerator(config GroupGeneratorConfig) *GroupGenerator {
	return &GroupGenerator{
		config: config,
	}
}

func (g *GroupGenerator) Name() string {
	return "GroupSliceGenerator"
}

// Generate acts as the entry point for scheduling slices. It generates interfaces.PlacementCandidate
// objects which are then evaluated by Scorers. It is aware of the overall ClusterState, using it
// to extract partition trees. Under the hood, it uses the unified interfaces.PlacementOption struct
// for building disjoint configurations. For single-slice requests, it maps the options
// directly into PlacementCandidates. For multi-slice requests, it delegates to
// beamSearch to assemble a valid gang placement.
//
// Example Input:
//
//	req: interfaces.GroupAllocationRequest{Slices: []interfaces.SliceRequirement{{SliceName: "slice-1", Topology: "4x4x4"}}}
//	state: (ClusterState containing partition trees)
//	eligiblePartitions: map[string]struct{}{"p1": {}}
//
// Example Output:
//
//	[]*interfaces.PlacementCandidate{
//	    {
//	        Assignments: map[string][]*interfaces.PartitionInfo{"slice-1": {"p1"}},
//	        Victims:     nil,
//	    },
//	}
func (g *GroupGenerator) Generate(ctx context.Context, req interfaces.GroupAllocationRequest, state interfaces.ClusterState, eligiblePartitions map[string]struct{}) []*interfaces.PlacementCandidate {
	if len(req.Slices) == 0 {
		return nil
	}

	requestedSlices := len(req.Slices)

	var options []*interfaces.PlacementOption
	numCubes := util.NumOfCubes(req.Slices[0].Topology)
	requestedTopology := req.Slices[0].Topology

	var podNames []string
	podTrees := make(map[string][]*interfaces.PartitionInfo)
	for _, root := range state.GetPartitionTrees() {
		if _, exists := podTrees[root.PodName]; !exists {
			podNames = append(podNames, root.PodName)
		}
		podTrees[root.PodName] = append(podTrees[root.PodName], root)
	}
	sort.Strings(podNames)

	// Canonicalize the enumeration order of the roots within each pod.
	//
	// interfaces.ClusterState.GetPartitionTrees() promises a set of roots, not an
	// order. The production implementation (watcher.partitionState.GetPartitionTrees)
	// builds its slice by ranging over a map, so the same cluster is handed over
	// in a different order from one call to the next.
	//
	// That order is not cosmetic. It flows straight through
	// findSinglePartitionOptions into the candidate list, and
	// schedulerlib.DefaultScheduler.ScorePhase keeps the first candidate unless a
	// later one scores strictly better. Equal-cost candidates are therefore
	// resolved positionally, so an unordered input makes the placement decision
	// non-deterministic: the identical cluster can place a slice on one cube on
	// one run and its neighbour on the next.
	//
	// Sorting by partition ID makes the generator's output a pure function of the
	// tree *set*. Partition IDs are unique cluster-wide, so this is a total order,
	// and ascending lexicographic ID is already the tie-break convention used by
	// tieBreakCandidates. The pod names above are sorted for the same reason.
	for _, trees := range podTrees {
		sort.Slice(trees, func(i, j int) bool { return trees[i].ID < trees[j].ID })
	}

	sliceNames := make([]string, len(req.Slices))
	// Gather all currently assigned partitions across the request group so multi-cube generation
	// can prioritize existing healthy partitions and minimize unnecessary partition swaps.
	var allCurrentIDs []string
	for i, s := range req.Slices {
		sliceNames[i] = s.SliceName
		allCurrentIDs = append(allCurrentIDs, s.CurrentPartitionIDs...)
	}

	for _, podName := range podNames {
		trees := podTrees[podName]
		if numCubes <= 1 {
			opts := findSinglePartitionOptions(trees, sliceNames, requestedTopology, eligiblePartitions)
			options = append(options, opts...)
		} else {
			baseOptions := findSinglePartitionOptions(trees, sliceNames, "4x4x4", eligiblePartitions)
			podCtx := log.IntoContext(ctx, log.FromContext(ctx).WithValues("pod", podName))
			opts := findDisjointPlacementOptions(podCtx, baseOptions, numCubes, requestedSlices, allCurrentIDs)
			options = append(options, opts...)
		}
	}

	if requestedSlices == 1 {
		var candidates []*interfaces.PlacementCandidate
		for _, opt := range options {
			if len(opt.Partitions) == 0 {
				continue
			}
			partIDs := make([]*interfaces.PartitionInfo, len(opt.Partitions))
			copy(partIDs, opt.Partitions)
			candidates = append(candidates, &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{req.Slices[0].SliceName: partIDs},
				Victims:     opt.Victims,
			})
		}
		return candidates
	}

	return g.beamSearch(ctx, req, options)
}

// beamSearch implements the Tier 2 (Global Assembly) heuristic beam search algorithm.
// It iterates slice-by-slice to build a gang of N slices, restricted by a Beam Width (W).
// It relies on Tier 1 (findDisjointPlacementOptions and findSinglePartitionOptions)
// for mutually exclusive single-slice candidates directly via interfaces.PlacementOption objects.
//
// Example Input:
//
//	req: interfaces.GroupAllocationRequest{Slices: []interfaces.SliceRequirement{{SliceName: "slice-1"}, {SliceName: "slice-2"}}}
//	tier1Pool: []*interfaces.PlacementOption{{Partitions: []interfaces.PartitionInfo{{ID: "p1"}}}, {Partitions: []interfaces.PartitionInfo{{ID: "p2"}}}}
//
// Example Output:
//
//	[]*interfaces.PlacementCandidate{
//	    {
//	        Assignments: map[string][]*interfaces.PartitionInfo{"slice-1": {"p1"}, "slice-2": {"p2"}},
//	    },
//	}
func (g *GroupGenerator) beamSearch(ctx context.Context, req interfaces.GroupAllocationRequest, tier1Pool []*interfaces.PlacementOption) []*interfaces.PlacementCandidate {
	requestedSlices := len(req.Slices)
	if requestedSlices == 0 {
		return nil
	}

	logf := log.FromContext(ctx, "generator", "BeamSearch")

	currentGangs := make([]PartialGang, 0, len(tier1Pool))

	// Level 1 Initialization: Score all single-slice candidates from Tier 1
	for _, c := range tier1Pool {
		currentGangs = append(currentGangs, NewPartialGang(
			[]*interfaces.PlacementOption{c},
			req,
		))
	}

	// Soft Prune Level 1 to keep Top W candidates
	currentGangs = pruneGangs(currentGangs, g.config.BeamWidth, req, logf, 1)

	// Expansion loop (Level 2 to N): For each of the W partial gangs, attempt to add one additional slice
	for level := 2; level <= requestedSlices; level++ {
		var nextGangs []PartialGang

		for _, gang := range currentGangs {
			for _, c := range tier1Pool {
				// Hard Prune: Physical Collision
				// Instantly discard any expanded combination that attempts to place two candidates in the same superpod
				if hasCollision(gang.Options, c) {
					continue
				}

				newOptions := make([]*interfaces.PlacementOption, len(gang.Options), len(gang.Options)+1)
				copy(newOptions, gang.Options)
				newOptions = append(newOptions, c)

				nextGangs = append(nextGangs, NewPartialGang(
					newOptions,
					req,
				))
			}
		}

		if len(nextGangs) == 0 {
			logf.Info("Beam search exhausted pool before reaching requested slices", "level", level-1)
			return nil
		}

		// Soft Prune: Score the valid expanded combinations using the Hierarchical Cost Function and keep the Top W
		currentGangs = pruneGangs(nextGangs, g.config.BeamWidth, req, logf, level)
	}

	bestCandidates := make([]*interfaces.PlacementCandidate, 0, len(currentGangs))
	for _, gang := range currentGangs {
		logf.Info("Selected gang candidate", "cost", gang.Cost)
		bestCandidates = append(bestCandidates, gang.MergedCandidate)
	}

	return bestCandidates
}

func hasCollision(existing []*interfaces.PlacementOption, newCand *interfaces.PlacementOption) bool {
	usedPartitions := make(map[string]bool)
	for _, c := range existing {
		for _, p := range c.Partitions {
			usedPartitions[p.ID] = true
		}
	}
	for _, p := range newCand.Partitions {
		if usedPartitions[p.ID] {
			return true
		}
	}
	return false
}

func countUniquePods(options []*interfaces.PlacementOption) int {
	pods := make(map[string]bool)
	for _, c := range options {
		if len(c.Partitions) > 0 && c.Partitions[0].PodName != "" {
			pods[c.Partitions[0].PodName] = true
		}
	}
	return len(pods)
}

// mergeCandidates combines multiple single-slice placement candidates into one cohesive gang placement.
func mergeOptions(options []*interfaces.PlacementOption, req interfaces.GroupAllocationRequest) *interfaces.PlacementCandidate {
	if len(options) == 0 {
		return nil
	}
	merged := &interfaces.PlacementCandidate{
		Assignments: make(map[string][]*interfaces.PartitionInfo),
		Victims:     nil,
	}

	seenVictims := make(map[string]bool)

	for i, c := range options {
		sliceName := req.Slices[i].SliceName
		merged.Assignments[sliceName] = make([]*interfaces.PartitionInfo, len(c.Partitions))
		copy(merged.Assignments[sliceName], c.Partitions)

		for _, v := range c.Victims {
			if !seenVictims[v.Name] {
				seenVictims[v.Name] = true
				merged.Victims = append(merged.Victims, v)
			}
		}
	}

	// The deduplicated list of Victims must be sorted lexicographically.
	// The scheduler's deterministic tie-breaker (tieBreakCandidates) compares victim lists
	// element-by-element to resolve ties when two gang options have the exact same cost.
	// Without this sort, two mathematically equivalent gang placements might accumulate the
	// exact same set of victims but in a different order (due to permutations during beam search),
	// causing the element-by-element tie-breaker to evaluate identical victim sets as unequal.
	sort.Slice(merged.Victims, func(i, j int) bool {
		return merged.Victims[i].Name < merged.Victims[j].Name
	})

	return merged
}

// tieBreakCandidates orders two equal-cost placement candidates.
//
// It is a strict weak ordering whose equivalence classes contain exactly one
// candidate each: two candidates compare equal in both directions if and only if
// they name the same victims and assign the same partitions to the same slices.
// For any two *distinct* candidates it returns a strict order. That totality is
// what makes the ranking in pruneGangs reproducible; a comparator that left
// distinct candidates unordered would hand the decision back to whatever order
// they happened to be generated in.
//
// The tiers, in order:
//
//  1. Victim names, element-wise, then victim count. Victim lists are pre-sorted
//     by name (see mergeOptions), so this compares sets, not orderings.
//  2. Assigned partition IDs per requested slice, element-wise, then count.
//     This is what separates permutations of an interchangeable gang.
//  3. A canonical fingerprint over *every* assignment in the candidate. Tiers 1
//     and 2 only see the slices in `slices`; this tier closes the gap when a
//     candidate carries assignments the caller did not list, and is what makes
//     totality hold by construction rather than by the caller's good behaviour.
//
// Tier 3 is unreachable for candidates produced by pruneGangs today, because
// there the merged candidate's assignments are exactly the listed slices. It
// exists so that a future caller cannot silently reintroduce ambiguity.
func tieBreakCandidates(m1, m2 *interfaces.PlacementCandidate, slices []interfaces.SliceRequirement) bool {
	vlen1, vlen2 := len(m1.Victims), len(m2.Victims)
	for k := 0; k < vlen1 && k < vlen2; k++ {
		if m1.Victims[k].Name != m2.Victims[k].Name {
			return m1.Victims[k].Name < m2.Victims[k].Name
		}
	}
	if vlen1 != vlen2 {
		return vlen1 < vlen2
	}

	for _, s := range slices {
		a1 := m1.Assignments[s.SliceName]
		a2 := m2.Assignments[s.SliceName]
		for k := 0; k < len(a1) && k < len(a2); k++ {
			if a1[k].ID != a2[k].ID {
				return a1[k].ID < a2[k].ID
			}
		}
		if len(a1) != len(a2) {
			return len(a1) < len(a2)
		}
	}

	if fp1, fp2 := candidateFingerprint(m1), candidateFingerprint(m2); fp1 != fp2 {
		return fp1 < fp2
	}
	return false
}

// candidateFingerprint renders every assignment a candidate holds, ordered by
// slice name, so two candidates remain comparable even when the caller's slice
// list does not cover all of them.
func candidateFingerprint(m *interfaces.PlacementCandidate) string {
	if len(m.Assignments) == 0 {
		return ""
	}
	names := make([]string, 0, len(m.Assignments))
	for name := range m.Assignments {
		names = append(names, name)
	}
	sort.Strings(names)

	var sb strings.Builder
	for _, name := range names {
		sb.WriteString(name)
		sb.WriteByte('=')
		sb.WriteString(joinIds(m.Assignments[name]))
		sb.WriteByte(';')
	}
	return sb.String()
}

// pruneGangs filters out duplicate partial gang combinations and retains the top 'width' candidates based on the heuristic score.
func pruneGangs(gangs []PartialGang, width int, req interfaces.GroupAllocationRequest, logf logr.Logger, level int) []PartialGang {
	// Filter out identical partial gangs first
	uniqueGangs := make([]PartialGang, 0)
	seenAt := make(map[string]int)

	for _, g := range gangs {
		m := g.MergedCandidate // Read directly from cache
		// Build a deterministic string key representing all assignments
		var keys []string
		for _, s := range req.Slices[:len(g.Options)] {
			keys = append(keys, joinIds(m.Assignments[s.SliceName]))
		}
		// This sort provides an important algorithmic optimization for symmetry breaking.
		// In a multi-slice gang request, all slices share identical topologies and are interchangeable.
		// A physical assignment of [slice-1 -> Cube A, slice-2 -> Cube B] is mathematically
		// identical to the permutation [slice-1 -> Cube B, slice-2 -> Cube A].
		// By sorting the assignment keys before joining them, the algorithm ensures these
		// permutations map to the exact same deduplication key, drastically pruning redundant paths
		// and preventing the beam search from wasting bandwidth on duplicate physical placements.
		sort.Strings(keys)
		key := strings.Join(keys, "|")

		idx, ok := seenAt[key]
		if !ok {
			seenAt[key] = len(uniqueGangs)
			uniqueGangs = append(uniqueGangs, g)
			continue
		}
		// Deduplication runs BEFORE ranking, so the member of a permutation class
		// that survives it is the one that reaches the output -- keeping whichever
		// arrived first would make the surviving representative a function of the
		// generation order, which is the same defect the ranking below is careful
		// to avoid. Keep the representative the total tie-breaker ranks first
		// instead. Still exactly one survivor per key: the symmetry-breaking
		// optimisation above is untouched, only the choice of survivor is pinned.
		if tieBreakCandidates(g.MergedCandidate, uniqueGangs[idx].MergedCandidate, req.Slices[:len(g.Options)]) {
			uniqueGangs[idx] = g
		}
	}

	// This sorting is the fundamental ranking step of the heuristic Beam Search algorithm.
	// It sorts the fully deduplicated generated paths by their calculated Cost (preemption impact and superpod spread).
	// If this sort is removed, uniqueGangs[:width] will simply return the first W candidates
	// in whatever arbitrary order they were generated in memory, completely bypassing the heuristic
	// cost function and degrading the beam search into an uninformed search.
	//
	// The sort is stable, and the tie-breaker below is a total order over distinct
	// candidates. Either alone would very nearly do; both together mean the
	// ranking is reproducible even if a future change makes the comparator
	// indecisive for some pair, instead of silently handing that pair's order
	// over to the sort's internal pivot choices.
	//
	// Zero-allocation sorting using cached fields
	sort.SliceStable(uniqueGangs, func(i, j int) bool {
		if !uniqueGangs[i].Cost.Less(uniqueGangs[j].Cost) && !uniqueGangs[j].Cost.Less(uniqueGangs[i].Cost) {
			// Costs are equal, use tie-breaker
			return tieBreakCandidates(uniqueGangs[i].MergedCandidate, uniqueGangs[j].MergedCandidate, req.Slices[:len(uniqueGangs[i].Options)])
		}
		return uniqueGangs[i].Cost.Less(uniqueGangs[j].Cost)
	})

	logf.Info("Soft Prune", "level", level, "candidates_evaluated", len(gangs), "unique_candidates", len(uniqueGangs), "retained", math.Min(float64(len(uniqueGangs)), float64(width)))

	if len(uniqueGangs) > width {
		return uniqueGangs[:width]
	}
	return uniqueGangs
}

func joinIds(parts []*interfaces.PartitionInfo) string {
	ids := make([]string, len(parts))
	for i, p := range parts {
		ids[i] = p.ID
	}
	return strings.Join(ids, ",")
}

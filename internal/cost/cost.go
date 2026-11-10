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

package cost

import (
	"fmt"
	"sort"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/solver"
	"github.com/ai-on-gke/slice-scheduler/internal/util"
)

// LevelCost represents the aggregated preemption cost metrics incurred at a specific
// scheduling priority level.
type LevelCost struct {
	Priority int // The scheduling priority level.
	Slices   int // Number of active victim slices preempted at this priority level.
	Capacity int // Total TPU capacity footprint of all victims at this priority level.
}

// PreemptionCost is a generic multi-priority preemption cost vector that implements solver.Cost.
// It is represented as a slice of LevelCost elements, kept sorted descending by priority.
type PreemptionCost []LevelCost

// NewPreemptionCost constructs a PreemptionCost vector from a slice of victim info.
// The resulting PreemptionCost slice is kept sorted descending by priority level.
func NewPreemptionCost(slices []*interfaces.SliceInfo) PreemptionCost {
	if len(slices) == 0 {
		return nil
	}
	// 1. Aggregate metrics in a map first
	agg := make(map[int]LevelCost)
	for _, s := range slices {
		pri := s.Priority
		cur := agg[pri]
		agg[pri] = LevelCost{
			Priority: pri,
			Slices:   cur.Slices + 1,
			Capacity: cur.Capacity + util.ChipCount(s.Topology),
		}
	}

	// 2. Convert to a slice of LevelCost
	pc := make(PreemptionCost, 0, len(agg))
	for _, lc := range agg {
		pc = append(pc, lc)
	}

	// 3. Sort descending by priority level
	sort.Slice(pc, func(i, j int) bool {
		return pc[i].Priority > pc[j].Priority
	})

	return pc
}

// Add implements solver.Cost. It combines level costs component-wise using
// a two-pointer merge algorithm, preserving descending order in linear time.
func (p PreemptionCost) Add(other solver.Cost) solver.Cost {
	if _, ok := other.(solver.MaxCost); ok {
		return other
	}
	b, ok := other.(PreemptionCost)
	if !ok {
		return p
	}

	var res PreemptionCost
	i, j := 0, 0
	for i < len(p) && j < len(b) {
		if p[i].Priority > b[j].Priority {
			res = append(res, p[i])
			i++
		} else if p[i].Priority < b[j].Priority {
			res = append(res, b[j])
			j++
		} else {
			// Priorities are equal, sum the costs
			res = append(res, LevelCost{
				Priority: p[i].Priority,
				Slices:   p[i].Slices + b[j].Slices,
				Capacity: p[i].Capacity + b[j].Capacity,
			})
			i++
			j++
		}
	}

	// Append remaining elements
	for i < len(p) {
		res = append(res, p[i])
		i++
	}
	for j < len(b) {
		res = append(res, b[j])
		j++
	}

	return res
}

// Less implements solver.Cost. It performs a descending lexicographical comparison
// element-by-element across all priority levels.
func (p PreemptionCost) Less(other interfaces.Score) bool {
	if _, ok := other.(solver.MaxCost); ok {
		return true
	}
	b, ok := other.(PreemptionCost)
	if !ok {
		return false
	}

	if len(p) == 0 && len(b) == 0 {
		return false
	}
	if len(p) == 0 {
		return true
	}
	if len(b) == 0 {
		return false
	}

	// Global tie-breaker: if max priority impacted is identical,
	// minimize the absolute global number of victims.
	if p[0].Priority == b[0].Priority {
		pTotal := 0
		for _, l := range p {
			pTotal += l.Slices
		}
		bTotal := 0
		for _, l := range b {
			bTotal += l.Slices
		}
		if pTotal != bTotal {
			return pTotal < bTotal
		}
	}

	i, j := 0, 0
	for i < len(p) && j < len(b) {
		costA := p[i]
		costB := b[j]

		if costA.Priority != costB.Priority {
			// A higher priority level represents a strictly more expensive preemption cost.
			// The one with the smaller maximum priority level (or lower level where they differ) is cheaper (better).
			return costA.Priority < costB.Priority
		}
		if costA.Slices != costB.Slices {
			return costA.Slices < costB.Slices
		}
		if costA.Capacity != costB.Capacity {
			return costA.Capacity < costB.Capacity
		}
		i++
		j++
	}

	// If we've reached here, one cost vector is a prefix of another.
	// E.g., A = [10: {1, 64}], B = [10: {1, 64}, 5: {1, 64}]
	// A has fewer levels of preemptions, so A is cheaper.
	return len(p) < len(b)
}

// String implements fmt.Stringer for nice logging output.
func (p PreemptionCost) String() string {
	return fmt.Sprintf("%v", []LevelCost(p))
}

// SwapCost represents the cost of swapping/relocating partitions.
type SwapCost int

// Add implements solver.Cost.
func (s SwapCost) Add(other solver.Cost) solver.Cost {
	if _, ok := other.(solver.MaxCost); ok {
		return other
	}
	b, ok := other.(SwapCost)
	if !ok {
		return s
	}
	return s + b
}

// Less implements solver.Cost. Less means "better" (fewer swapped partitions).
func (s SwapCost) Less(other interfaces.Score) bool {
	if _, ok := other.(solver.MaxCost); ok {
		return true
	}
	b, ok := other.(SwapCost)
	if !ok {
		return false
	}
	return s < b
}

// String implements fmt.Stringer.
func (s SwapCost) String() string {
	return fmt.Sprintf("Swaps: %d", int(s))
}

// PlacementCost combines preemption cost and superpod spread into a single cost.
type PlacementCost struct {
	Preemption PreemptionCost
	Spread     int // Number of superpods spanned
}

// Add implements solver.Cost.
func (p PlacementCost) Add(other solver.Cost) solver.Cost {
	if _, ok := other.(solver.MaxCost); ok {
		return other
	}
	b, ok := other.(PlacementCost)
	if !ok {
		return p
	}
	inner := p.Preemption.Add(b.Preemption)
	pc, ok := inner.(PreemptionCost)
	if !ok {
		return p // Defensive: inner Add returned unexpected type
	}
	return PlacementCost{
		Preemption: pc,
		Spread:     p.Spread + b.Spread,
	}
}

// Less implements solver.Cost. Less means "better" (cheaper cost / less spread).
func (p PlacementCost) Less(other interfaces.Score) bool {
	if _, ok := other.(solver.MaxCost); ok {
		return true
	}
	b, ok := other.(PlacementCost)
	if !ok {
		return false
	}
	// 1. Compare preemption first
	if p.Preemption.Less(b.Preemption) {
		return true
	}
	if b.Preemption.Less(p.Preemption) {
		return false
	}
	// 2. If preemption is identical, prefer smaller spread
	return p.Spread < b.Spread
}

// String implements fmt.Stringer.
func (p PlacementCost) String() string {
	return fmt.Sprintf("Preemption: %s, Spread: %d", p.Preemption, p.Spread)
}

// TightestFitCost represents the cost of placement bin-packing fit.
type TightestFitCost struct {
	FitScore     int // Smallest unoccupied ancestor chip count (lower is tighter/better)
	DensityScore int // Density score (lower is denser/better)
}

// Add implements solver.Cost.
func (t TightestFitCost) Add(other solver.Cost) solver.Cost {
	if _, ok := other.(solver.MaxCost); ok {
		return other
	}
	b, ok := other.(TightestFitCost)
	if !ok {
		return t
	}
	return TightestFitCost{
		FitScore:     t.FitScore + b.FitScore,
		DensityScore: t.DensityScore + b.DensityScore,
	}
}

// Less implements solver.Cost. Less means "better".
func (t TightestFitCost) Less(other interfaces.Score) bool {
	if _, ok := other.(solver.MaxCost); ok {
		return true
	}
	b, ok := other.(TightestFitCost)
	if !ok {
		return false
	}
	if t.FitScore != b.FitScore {
		return t.FitScore < b.FitScore
	}
	return t.DensityScore < b.DensityScore
}

// String implements fmt.Stringer.
func (t TightestFitCost) String() string {
	return fmt.Sprintf("Fit: %d, Density: %d", t.FitScore, t.DensityScore)
}

// DegradationCost represents the degradation penalty (minimize degraded cubes).
type DegradationCost int

// Add implements solver.Cost.
func (d DegradationCost) Add(other solver.Cost) solver.Cost {
	if _, ok := other.(solver.MaxCost); ok {
		return other
	}
	b, ok := other.(DegradationCost)
	if !ok {
		return d
	}
	return d + b
}

// Less implements solver.Cost. Less means "better" (fewer degraded cubes used).
func (d DegradationCost) Less(other interfaces.Score) bool {
	if _, ok := other.(solver.MaxCost); ok {
		return true
	}
	b, ok := other.(DegradationCost)
	if !ok {
		return false
	}
	return d < b
}

// String implements fmt.Stringer.
func (d DegradationCost) String() string {
	return fmt.Sprintf("DegradedCubes: %d", int(d))
}

// DegradationPreemptionCost combines preemption cost, degradation penalty, and swap cost.
type DegradationPreemptionCost struct {
	PreemptionCost  PreemptionCost
	SwapCost        SwapCost
	DegradationCost DegradationCost // Number of normal partitions used (minimize this to prefer degraded)
}

// Add implements solver.Cost.
func (c DegradationPreemptionCost) Add(other solver.Cost) solver.Cost {
	if _, ok := other.(solver.MaxCost); ok {
		return other
	}
	b, ok := other.(DegradationPreemptionCost)
	if !ok {
		return c
	}
	// Handle nil PreemptionCost safely
	cPreempt := c.PreemptionCost
	if cPreempt == nil {
		cPreempt = PreemptionCost{}
	}
	bPreempt := b.PreemptionCost
	if bPreempt == nil {
		bPreempt = PreemptionCost{}
	}
	return DegradationPreemptionCost{
		PreemptionCost:  cPreempt.Add(bPreempt).(PreemptionCost),
		SwapCost:        c.SwapCost.Add(b.SwapCost).(SwapCost),
		DegradationCost: c.DegradationCost.Add(b.DegradationCost).(DegradationCost),
	}
}

// Less implements solver.Cost. Less means "better" (cheaper preemption, fewer normal partitions on tie, or fewer swapped cubes on tie).
func (c DegradationPreemptionCost) Less(other interfaces.Score) bool {
	if _, ok := other.(solver.MaxCost); ok {
		return true
	}
	b, ok := other.(DegradationPreemptionCost)
	if !ok {
		return false
	}
	// Handle nil PreemptionCost safely
	cPreempt := c.PreemptionCost
	if cPreempt == nil {
		cPreempt = PreemptionCost{}
	}
	bPreempt := b.PreemptionCost
	if bPreempt == nil {
		bPreempt = PreemptionCost{}
	}
	if cPreempt.Less(bPreempt) {
		return true
	}
	if bPreempt.Less(cPreempt) {
		return false
	}
	if c.DegradationCost.Less(b.DegradationCost) {
		return true
	}
	if b.DegradationCost.Less(c.DegradationCost) {
		return false
	}
	return c.SwapCost.Less(b.SwapCost)
}

// String implements fmt.Stringer.
func (c DegradationPreemptionCost) String() string {
	return fmt.Sprintf("Preemption: %s, DegradationCost: %d, Swaps: %d", c.PreemptionCost, int(c.DegradationCost), int(c.SwapCost))
}

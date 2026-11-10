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

package solver

import (
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

// Cost defines the mathematical interface required for cost elements inside the solver.
// Any implementation of this interface must satisfy the Monotonicity property:
// If C1 < C2, then C1 + A <= C2 + A for any cost A.
type Cost interface {
	interfaces.Score
	// Add returns the combined cost of two decisions.
	Add(other Cost) Cost
}

// Item represents a generic item that can be selected by the solver.
type Item struct {
	Name     string      // Unique identifier for the item (useful for logging/debugging)
	Weight   int         // Capacity/weight this item provides (must be > 0)
	Cost     Cost        // The cost associated with selecting this item
	Metadata interface{} // Optional custom payload to map the selected item back to physical domain models
}

// MaxCost represents mathematical infinity (+Inf) in the solver.
// It serves as the initial cost for unreachable targets.
type MaxCost struct{}

// Add on MaxCost always returns MaxCost (infinity + cost = infinity).
func (m MaxCost) Add(other Cost) Cost {
	return m
}

// Less on MaxCost always returns false (infinity is never cheaper than any cost).
func (m MaxCost) Less(other interfaces.Score) bool {
	return false
}

// SolveCover solves the minimum cost cover problem.
//
// Goal: Find a subset of items with a combined weight of AT LEAST targetWeight that minimizes
// the total combined Cost.
//
// Parameters:
//   - items: The list of available items to choose from.
//   - targetWeight: The minimum weight capacity required.
//
// Returns:
//   - []Item: The optimal subset of selected items.
//   - Cost: The minimum total cost of the selection (returns MaxCost if no feasible solution exists).
func SolveCover(items []Item, targetWeight int) ([]Item, Cost) {
	n := len(items)
	if targetWeight <= 0 {
		return nil, costZero()
	}
	if n == 0 {
		return nil, MaxCost{}
	}

	// dp[i][w] stores the minimum cost to get at least w weight using a subset of the first i items.
	// Dimensions: (n + 1) x (targetWeight + 1)
	dp := make([][]Cost, n+1)
	for i := 0; i <= n; i++ {
		dp[i] = make([]Cost, targetWeight+1)
		for w := 0; w <= targetWeight; w++ {
			dp[i][w] = MaxCost{}
		}
	}
	// Base case: 0 weight requires 0 cost
	dp[0][0] = costZero()

	// Fill the solver table
	for i := 1; i <= n; i++ {
		item := items[i-1]
		for w := 0; w <= targetWeight; w++ {
			// Option 1: Do not use item i
			dp[i][w] = dp[i-1][w]

			// Option 2: Use item i
			prevW := w - item.Weight
			if prevW < 0 {
				prevW = 0 // Weight required cannot go below 0
			}
			if _, isMax := dp[i-1][prevW].(MaxCost); !isMax {
				newCost := dp[i-1][prevW].Add(item.Cost)
				if newCost.Less(dp[i][w]) {
					dp[i][w] = newCost
				}
			}
		}
	}

	finalCost := dp[n][targetWeight]
	if _, isMax := finalCost.(MaxCost); isMax {
		return nil, MaxCost{} // Not enough resources to satisfy targetWeight
	}

	// Backtrack to find which items were selected.
	// To resolve ties deterministically and favor earlier items in the list
	// (matching legacy greedy tie-breaker heuristics), we prefer NOT using
	// the item if we can get the exact same optimal cost without it.
	var selected []Item
	currW := targetWeight
	for i := n; i > 0; i-- {
		if currW == 0 {
			break // Target weight met
		}
		item := items[i-1]
		prevW := currW - item.Weight
		if prevW < 0 {
			prevW = 0
		}

		canAvoid := false
		if _, isMax := dp[i-1][currW].(MaxCost); !isMax {
			costWithoutItem := dp[i-1][currW]
			// Check equality: costWithoutItem == dp[i][currW]
			if !dp[i][currW].Less(costWithoutItem) && !costWithoutItem.Less(dp[i][currW]) {
				canAvoid = true
			}
		}

		if canAvoid {
			// Prefer not using this item to push the selection to earlier items (left-bias)
		} else {
			// Must use this item to achieve the optimal cost
			selected = append(selected, item)
			currW = prevW
		}
	}

	return selected, finalCost
}

// helper to return a zero cost for base cases.
type zeroCost struct{}

func (z zeroCost) Add(other Cost) Cost              { return other }
func (z zeroCost) Less(other interfaces.Score) bool { _, ok := other.(MaxCost); return ok }

func costZero() Cost {
	return zeroCost{}
}

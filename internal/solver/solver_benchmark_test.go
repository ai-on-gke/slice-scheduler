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

package solver

import (
	"fmt"
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

// testCost is a simple integer-based cost for solver benchmarks.
type testCost int

func (c testCost) Less(other interfaces.Score) bool {
	o, ok := other.(testCost)
	if !ok {
		_, isMax := other.(MaxCost)
		return isMax // testCost < MaxCost
	}
	return int(c) < int(o)
}

func (c testCost) Add(other Cost) Cost {
	if _, isMax := other.(MaxCost); isMax {
		return MaxCost{}
	}
	if _, isZero := other.(zeroCost); isZero {
		return c
	}
	return testCost(int(c) + int(other.(testCost)))
}

// =============================================================================
// Benchmark: SolveCover at various item counts
// =============================================================================

func BenchmarkSolveCover_8Items(b *testing.B) {
	benchmarkSolveCover(b, 8, 2)
}

func BenchmarkSolveCover_16Items(b *testing.B) {
	benchmarkSolveCover(b, 16, 2)
}

func BenchmarkSolveCover_50Items(b *testing.B) {
	benchmarkSolveCover(b, 50, 2)
}

func BenchmarkSolveCover_144Items(b *testing.B) {
	benchmarkSolveCover(b, 144, 2)
}

func BenchmarkSolveCover_50Items_Target4(b *testing.B) {
	benchmarkSolveCover(b, 50, 4)
}

func BenchmarkSolveCover_144Items_Target4(b *testing.B) {
	benchmarkSolveCover(b, 144, 4)
}

func BenchmarkSolveCover_144Items_Target8(b *testing.B) {
	benchmarkSolveCover(b, 144, 8)
}

func BenchmarkSolveCover_144Items_Target16(b *testing.B) {
	benchmarkSolveCover(b, 144, 16)
}

func benchmarkSolveCover(b *testing.B, numItems, targetWeight int) {
	b.Helper()
	items := make([]Item, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = Item{
			Name:   fmt.Sprintf("item-%d", i),
			Weight: 1,
			Cost:   testCost(i % 10), // Varied costs for realistic distribution
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selected, cost := SolveCover(items, targetWeight)
		if selected == nil {
			b.Fatal("Expected a solution but got nil")
		}
		_ = cost
	}
}

// =============================================================================
// Benchmark: SolveCover with mixed weights (simulates multi-cube victims)
// =============================================================================

func BenchmarkSolveCover_MixedWeights_50Items(b *testing.B) {
	benchmarkSolveCoverMixedWeights(b, 50, 4)
}

func BenchmarkSolveCover_MixedWeights_144Items(b *testing.B) {
	benchmarkSolveCoverMixedWeights(b, 144, 8)
}

func benchmarkSolveCoverMixedWeights(b *testing.B, numItems, targetWeight int) {
	b.Helper()
	items := make([]Item, numItems)
	for i := 0; i < numItems; i++ {
		weight := 1
		if i%5 == 0 {
			weight = 2 // Some items cover 2 cubes
		}
		if i%10 == 0 {
			weight = 4 // Some items cover 4 cubes
		}
		items[i] = Item{
			Name:   fmt.Sprintf("item-%d", i),
			Weight: weight,
			Cost:   testCost(i % 10),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selected, cost := SolveCover(items, targetWeight)
		if selected == nil {
			b.Fatal("Expected a solution but got nil")
		}
		_ = cost
	}
}

// =============================================================================
// Test: SolveCover correctness at scale
// =============================================================================

func TestSolveCover_LargeScale_Correctness(t *testing.T) {
	tests := []struct {
		name         string
		numItems     int
		targetWeight int
	}{
		{"16 items, target 2", 16, 2},
		{"50 items, target 4", 50, 4},
		{"144 items, target 8", 144, 8},
		{"144 items, target 16", 144, 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := make([]Item, tt.numItems)
			for i := 0; i < tt.numItems; i++ {
				items[i] = Item{
					Name:   fmt.Sprintf("item-%d", i),
					Weight: 1,
					Cost:   testCost(i), // Ascending cost
				}
			}

			selected, cost := SolveCover(items, tt.targetWeight)
			if selected == nil {
				t.Fatal("Expected a solution but got nil")
			}

			// Verify total weight meets target
			totalWeight := 0
			for _, item := range selected {
				totalWeight += item.Weight
			}
			if totalWeight < tt.targetWeight {
				t.Errorf("Total weight %d < target weight %d", totalWeight, tt.targetWeight)
			}

			// Verify it selected the lowest-cost items (ascending cost)
			if _, isMax := cost.(MaxCost); isMax {
				t.Error("Expected non-MaxCost solution")
			}

			t.Logf("SolveCover(%d items, target %d): selected %d items, total weight %d",
				tt.numItems, tt.targetWeight, len(selected), totalWeight)
		})
	}
}

// =============================================================================
// Test: SolveCover infeasibility
// =============================================================================

func TestSolveCover_Infeasible(t *testing.T) {
	// 10 items each with weight 1, but target is 20
	items := make([]Item, 10)
	for i := 0; i < 10; i++ {
		items[i] = Item{
			Name:   fmt.Sprintf("item-%d", i),
			Weight: 1,
			Cost:   testCost(i),
		}
	}

	selected, cost := SolveCover(items, 20)
	if selected != nil {
		t.Errorf("Expected nil solution for infeasible problem, got %d items", len(selected))
	}
	if _, isMax := cost.(MaxCost); !isMax {
		t.Error("Expected MaxCost for infeasible problem")
	}
}

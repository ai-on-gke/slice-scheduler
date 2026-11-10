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
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/google/go-cmp/cmp"
)

// mockCost implements Cost for testing.
type mockCost struct {
	Val int
}

func (m mockCost) Add(other Cost) Cost {
	if _, ok := other.(MaxCost); ok {
		return other
	}
	if _, ok := other.(zeroCost); ok {
		return m
	}
	o := other.(mockCost)
	return mockCost{m.Val + o.Val}
}

func (m mockCost) Less(other interfaces.Score) bool {
	if _, ok := other.(MaxCost); ok {
		return true
	}
	if _, ok := other.(zeroCost); ok {
		return false // zeroCost is cheaper than any positive cost
	}
	o := other.(mockCost)
	return m.Val < o.Val
}

func TestSolveCover(t *testing.T) {
	testCases := []struct {
		name         string
		items        []Item
		targetWeight int
		wantSelected []Item
		wantCost     Cost
	}{
		{
			name:         "Empty items",
			items:        []Item{},
			targetWeight: 5,
			wantSelected: nil,
			wantCost:     MaxCost{},
		},
		{
			name:         "Zero target weight",
			items:        []Item{{Name: "A", Weight: 2, Cost: mockCost{10}}},
			targetWeight: 0,
			wantSelected: nil,
			wantCost:     zeroCost{},
		},
		{
			name:         "Simple Match",
			items:        []Item{{Name: "A", Weight: 2, Cost: mockCost{10}}},
			targetWeight: 2,
			wantSelected: []Item{{Name: "A", Weight: 2, Cost: mockCost{10}}},
			wantCost:     mockCost{10},
		},
		{
			name: "Tie-breaker (Favor Earlier)",
			items: []Item{
				{Name: "A", Weight: 1, Cost: mockCost{5}},
				{Name: "B", Weight: 1, Cost: mockCost{5}},
			},
			targetWeight: 1,
			wantSelected: []Item{{Name: "A", Weight: 1, Cost: mockCost{5}}},
			wantCost:     mockCost{5},
		},
		{
			name:         "Over-satisfaction (Single Item)",
			items:        []Item{{Name: "A", Weight: 3, Cost: mockCost{10}}},
			targetWeight: 2,
			wantSelected: []Item{{Name: "A", Weight: 3, Cost: mockCost{10}}},
			wantCost:     mockCost{10},
		},
		{
			name: "Standard Cover Selection",
			items: []Item{
				{Name: "A", Weight: 2, Cost: mockCost{10}},
				{Name: "B", Weight: 3, Cost: mockCost{15}},
				{Name: "C", Weight: 1, Cost: mockCost{5}},
			},
			targetWeight: 4,
			wantSelected: []Item{
				{Name: "C", Weight: 1, Cost: mockCost{5}},
				{Name: "B", Weight: 3, Cost: mockCost{15}},
			},
			wantCost: mockCost{20},
		},
		{
			name: "Unfeasible Target",
			items: []Item{
				{Name: "A", Weight: 2, Cost: mockCost{10}},
			},
			targetWeight: 5,
			wantSelected: nil,
			wantCost:     MaxCost{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotSelected, gotCost := SolveCover(tc.items, tc.targetWeight)

			if diff := cmp.Diff(tc.wantSelected, gotSelected); diff != "" {
				t.Errorf("SolveCover() selected items mismatch (-want +got):\n%s", diff)
			}

			// Compare costs
			costsEqual := false
			if _, isMaxWant := tc.wantCost.(MaxCost); isMaxWant {
				_, isMaxGot := gotCost.(MaxCost)
				costsEqual = isMaxGot
			} else if _, isZeroWant := tc.wantCost.(zeroCost); isZeroWant {
				_, isZeroGot := gotCost.(zeroCost)
				costsEqual = isZeroGot
			} else {
				costsEqual = !tc.wantCost.Less(gotCost) && !gotCost.Less(tc.wantCost)
			}

			if !costsEqual {
				t.Errorf("SolveCover() cost mismatch: want %v, got %v", tc.wantCost, gotCost)
			}
		})
	}
}

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
	"testing"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/solver"
)

func TestPreemptionCost_Less(t *testing.T) {
	tests := []struct {
		name string
		a    PreemptionCost
		b    PreemptionCost
		want bool // a < b (a is strictly better/cheaper than b)
	}{
		{
			name: "Priority Primacy - Higher priority is strictly worse",
			a:    NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10, Topology: "4x4x4"}}), // 1 slice at pri 10
			b:    NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 5, Topology: "8x8x8"}}),  // 1 slice at pri 5
			want: false,                                                                         // a is worse (more expensive) than b
		},
		{
			name: "Priority Primacy - Lower priority is strictly better",
			a:    NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 5, Topology: "4x4x4"}}),
			b:    NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10, Topology: "4x4x4"}}),
			want: true, // a is better than b
		},
		{
			name: "Slice Count Tie-breaking - Fewer slices is better",
			a: NewPreemptionCost([]*interfaces.SliceInfo{
				{Priority: 10, Topology: "4x4x4"},
			}),
			b: NewPreemptionCost([]*interfaces.SliceInfo{
				{Priority: 10, Topology: "4x4x4"},
				{Priority: 10, Topology: "4x4x4"},
			}),
			want: true,
		},
		{
			name: "Capacity Tie-breaking - Smaller footprint is better",
			a:    NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10, Topology: "4x4x4"}}), // 64 chips
			b:    NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10, Topology: "4x4x8"}}), // 128 chips
			want: true,
		},
		{
			name: "Mixed Priorities - Comparison stops at first difference",
			a: NewPreemptionCost([]*interfaces.SliceInfo{
				{Priority: 10, Topology: "4x4x4"}, // Same
				{Priority: 5, Topology: "4x4x4"},  // Fewer at pri 5
			}),
			b: NewPreemptionCost([]*interfaces.SliceInfo{
				{Priority: 10, Topology: "4x4x4"},
				{Priority: 5, Topology: "4x4x4"},
				{Priority: 5, Topology: "4x4x4"},
			}),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Less(tt.b)
			if got != tt.want {
				t.Errorf("Less() mismatch: got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPreemptionCost_Add(t *testing.T) {
	a := NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10, Topology: "4x4x4"}})
	b := NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 5, Topology: "4x4x8"}})

	sumCost := a.Add(b).(PreemptionCost)

	if len(sumCost) != 2 {
		t.Fatalf("Add() expected length 2, got %d: %+v", len(sumCost), sumCost)
	}

	// Since they are sorted descending: priority 10 first, then priority 5.
	if sumCost[0].Priority != 10 || sumCost[0].Slices != 1 || sumCost[0].Capacity != 64 {
		t.Errorf("Add() index 0 mismatch: got %+v", sumCost[0])
	}
	if sumCost[1].Priority != 5 || sumCost[1].Slices != 1 || sumCost[1].Capacity != 128 {
		t.Errorf("Add() index 1 mismatch: got %+v", sumCost[1])
	}
}

func TestPlacementCost_Less(t *testing.T) {
	tests := []struct {
		name string
		a    PlacementCost
		b    PlacementCost
		want bool // a is strictly better than b
	}{
		{
			name: "Preemption wins - cheaper preemption is better",
			a: PlacementCost{
				Preemption: NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 5}}),
				Spread:     10, // higher spread but cheaper preemption
			},
			b: PlacementCost{
				Preemption: NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
				Spread:     1,
			},
			want: true,
		},
		{
			name: "Spread tie-break - fewer superpods is better",
			a: PlacementCost{
				Preemption: NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
				Spread:     1, // identical preemption, lower spread
			},
			b: PlacementCost{
				Preemption: NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
				Spread:     2,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Less(tt.b)
			if got != tt.want {
				t.Errorf("Less() mismatch: got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTightestFitCost_Less(t *testing.T) {
	tests := []struct {
		name string
		a    TightestFitCost
		b    TightestFitCost
		want bool // a is strictly better than b
	}{
		{
			name: "Fit score wins - smaller fit is better",
			a:    TightestFitCost{FitScore: 64, DensityScore: 100},
			b:    TightestFitCost{FitScore: 128, DensityScore: 1},
			want: true,
		},
		{
			name: "Density score tie-break - smaller density score is better",
			a:    TightestFitCost{FitScore: 64, DensityScore: 1},
			b:    TightestFitCost{FitScore: 64, DensityScore: 2},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Less(tt.b)
			if got != tt.want {
				t.Errorf("Less() mismatch: got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDegradationCost_Less(t *testing.T) {
	tests := []struct {
		name string
		a    DegradationCost
		b    DegradationCost
		want bool // a is strictly better than b (fewer degraded cubes is better)
	}{
		{
			name: "fewer degraded cubes is better",
			a:    DegradationCost(1),
			b:    DegradationCost(2),
			want: true,
		},
		{
			name: "more degraded cubes is worse",
			a:    DegradationCost(5),
			b:    DegradationCost(3),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Less(tt.b)
			if got != tt.want {
				t.Errorf("Less() mismatch: got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDegradationPreemptionCost_Less(t *testing.T) {
	tests := []struct {
		name string
		a    DegradationPreemptionCost
		b    DegradationPreemptionCost
		want bool
	}{
		{
			name: "Preemption wins",
			a: DegradationPreemptionCost{
				PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 5}}),
				DegradationCost: DegradationCost(10),
			},
			b: DegradationPreemptionCost{
				PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
				DegradationCost: DegradationCost(1),
			},
			want: true,
		},
		{
			name: "Degradation tie-break",
			a: DegradationPreemptionCost{
				PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
				DegradationCost: DegradationCost(1),
			},
			b: DegradationPreemptionCost{
				PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
				DegradationCost: DegradationCost(2),
			},
			want: true,
		},
		{
			name: "Swap tie-break after degradation",
			a: DegradationPreemptionCost{
				PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
				DegradationCost: DegradationCost(1),
				SwapCost:        SwapCost(0),
			},
			b: DegradationPreemptionCost{
				PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
				DegradationCost: DegradationCost(1),
				SwapCost:        SwapCost(1),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Less(tt.b)
			if got != tt.want {
				t.Errorf("Less() mismatch: got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDegradationPreemptionCost_Less_NilPreemption(t *testing.T) {
	a := DegradationPreemptionCost{
		PreemptionCost:  nil,
		DegradationCost: DegradationCost(1),
	}
	b := DegradationPreemptionCost{
		PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
		DegradationCost: DegradationCost(0),
	}
	// a has nil preemption (zero cost), b has preemption at priority 10 → a is cheaper
	if !a.Less(b) {
		t.Errorf("Expected nil preemption (a) to be less than non-nil (b)")
	}
	if b.Less(a) {
		t.Errorf("Expected non-nil preemption (b) NOT to be less than nil (a)")
	}
}

func TestDegradationPreemptionCost_Add(t *testing.T) {
	a := DegradationPreemptionCost{
		PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10, Topology: "4x4x4"}}),
		DegradationCost: DegradationCost(1),
	}
	b := DegradationPreemptionCost{
		PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 5, Topology: "4x4x4"}}),
		DegradationCost: DegradationCost(2),
	}
	sum := a.Add(b).(DegradationPreemptionCost)
	if sum.DegradationCost != 3 {
		t.Errorf("Add() DegradationCost: got %d, want 3", sum.DegradationCost)
	}
	if len(sum.PreemptionCost) != 2 {
		t.Errorf("Add() PreemptionCost length: got %d, want 2", len(sum.PreemptionCost))
	}
}

func TestDegradationPreemptionCost_Add_MaxCost(t *testing.T) {
	a := DegradationPreemptionCost{
		PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
		DegradationCost: DegradationCost(1),
	}
	result := a.Add(solver.MaxCost{})
	if _, ok := result.(solver.MaxCost); !ok {
		t.Errorf("Add(MaxCost) should return MaxCost, got %T", result)
	}
}

func TestDegradationPreemptionCost_String(t *testing.T) {
	c := DegradationPreemptionCost{
		PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10, Topology: "4x4x4"}}),
		DegradationCost: DegradationCost(3),
	}
	s := c.String()
	if s == "" {
		t.Error("String() returned empty string")
	}
}

func TestSwapCost_Methods(t *testing.T) {
	s1 := SwapCost(2)
	s2 := SwapCost(3)

	// Test Add
	if sum := s1.Add(s2).(SwapCost); sum != SwapCost(5) {
		t.Errorf("expected 2 + 3 = 5, got %v", sum)
	}

	// Test Less
	if !s1.Less(s2) {
		t.Errorf("expected 2 < 3")
	}
	if s2.Less(s1) {
		t.Errorf("expected 3 NOT < 2")
	}

	// Test String
	if s1.String() != "Swaps: 2" {
		t.Errorf("expected String() 'Swaps: 2', got %s", s1.String())
	}
}

func TestDegradationPreemptionCost_Methods(t *testing.T) {
	c1 := DegradationPreemptionCost{
		PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
		DegradationCost: DegradationCost(1),
		SwapCost:        SwapCost(1),
	}
	c2 := DegradationPreemptionCost{
		PreemptionCost:  NewPreemptionCost([]*interfaces.SliceInfo{{Priority: 10}}),
		DegradationCost: DegradationCost(1),
		SwapCost:        SwapCost(2),
	}

	if !c1.Less(c2) {
		t.Errorf("expected c1 (SwapCost 1) < c2 (SwapCost 2)")
	}
	if c2.Less(c1) {
		t.Errorf("expected c2 (SwapCost 2) NOT < c1 (SwapCost 1)")
	}

	sum := c1.Add(c2).(DegradationPreemptionCost)
	if sum.SwapCost != SwapCost(3) {
		t.Errorf("Add() SwapCost: got %d, want 3", sum.SwapCost)
	}
}

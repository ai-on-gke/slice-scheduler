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

package util

import (
	"testing"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
)

func TestNumOfCubes(t *testing.T) {
	tests := []struct {
		name     string
		topology string
		want     int
	}{
		{name: "Cached single cube", topology: "4x4x4", want: 1},
		{name: "Cached two cubes", topology: "4x4x8", want: 2},
		{name: "Cached sub-cube is zero cubes", topology: "2x2x2", want: 0},
		{name: "Uncached but valid", topology: "1x1x64", want: 1},
		{name: "Uncached beyond the pre-populated 144-cube cache", topology: "4x4x580", want: 145},
		{name: "Largest dimension accepted", topology: "9999x1x1", want: 9999 / 64},
		{name: "Largest topology expressible does not overflow", topology: "9999x9999x9999", want: 9999 * 9999 * 9999 / 64},
		{name: "Two dimensions is not a cube topology", topology: "16x16", want: -1},
		{name: "Non-numeric dimension", topology: "4x4xz", want: -1},
		{name: "Zero dimension", topology: "0x4x4", want: -1},
		{name: "Negative dimension", topology: "-4x4x4", want: -1},
		{name: "Dimension just past the limit", topology: "10000x1x1", want: -1},
		{name: "Overflow", topology: "2097152x2097152x2097152", want: -1},
		{name: "Huge but non-overflowing product", topology: "10000x10000x10000", want: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NumOfCubes(tt.topology); got != tt.want {
				t.Errorf("NumOfCubes(%q) = %v, want %v", tt.topology, got, tt.want)
			}
		})
	}
}

func TestChipCount(t *testing.T) {
	tests := []struct {
		name     string
		topology string
		want     int
	}{
		{name: "Cached 2D topology", topology: "16x16", want: 256},
		{name: "Cached 3D cube", topology: "4x4x4", want: 64},
		{name: "Cached sub-cube", topology: "2x2x1", want: 4},
		{name: "Uncached 2D topology", topology: "32x32", want: 1024},
		{name: "Uncached 3D topology", topology: "4x4x580", want: 9280},
		{name: "Largest dimensions accepted", topology: "9999x9999", want: 9999 * 9999},
		{name: "Largest topology expressible does not overflow", topology: "9999x9999x9999", want: 9999 * 9999 * 9999},
		{name: "Single dimension", topology: "16", want: 0},
		{name: "Four dimensions", topology: "4x4x4x4", want: 0},
		{name: "Non-numeric dimension", topology: "4xz", want: 0},
		{name: "Zero dimension", topology: "0x16", want: 0},
		{name: "Dimension just past the limit", topology: "10000x9999", want: 0},
		{name: "Overflow", topology: "2097152x2097152x2097152", want: 0},
		{name: "Huge but non-overflowing product", topology: "10000x10000x10000", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ChipCount(tt.topology); got != tt.want {
				t.Errorf("ChipCount(%q) = %v, want %v", tt.topology, got, tt.want)
			}
		})
	}
}

func TestIsSingleCubeOrLess(t *testing.T) {
	tests := []struct {
		name     string
		topology string
		want     bool
	}{
		{name: "Single cube", topology: "4x4x4", want: true},
		{name: "Sub-cube", topology: "2x2x2", want: true},
		{name: "Two cubes", topology: "4x4x8", want: false},
		{name: "Out-of-range dimension", topology: "10000x10000x10000", want: false},
		{name: "Dimension just past the limit", topology: "10000x1x1", want: false},
		{name: "Non-numeric dimension", topology: "4x4xz", want: false},
		{name: "Zero dimension", topology: "0x4x4", want: false},
		{name: "Two dimensions is not a cube topology", topology: "16x16", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSingleCubeOrLess(tt.topology); got != tt.want {
				t.Errorf("IsSingleCubeOrLess(%q) = %v, want %v", tt.topology, got, tt.want)
			}
		})
	}
}

func TestIsPartitionAvailable(t *testing.T) {
	tests := []struct {
		name string
		info partitiontreev1.PartitionInfo
		want bool
	}{
		{
			name: "HEALTHY state with no occupancy and no sliceId is available",
			info: partitiontreev1.PartitionInfo{State: partitiontreev1.PartitionStateHealthy},
			want: true,
		},
		{
			name: "HEALTHY state with OCCUPIED occupancy is not available",
			info: partitiontreev1.PartitionInfo{
				State:     partitiontreev1.PartitionStateHealthy,
				Occupancy: partitiontreev1.OccupancyOccupied,
			},
			want: false,
		},
		{
			name: "HEALTHY state with sliceId is not available",
			info: partitiontreev1.PartitionInfo{
				State:   partitiontreev1.PartitionStateHealthy,
				SliceId: "some-slice",
			},
			want: false,
		},
		{
			name: "BROKEN state is not available",
			info: partitiontreev1.PartitionInfo{State: partitiontreev1.PartitionStateUnhealthy},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPartitionAvailable(&tt.info); got != tt.want {
				t.Errorf("IsPartitionAvailable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsPartitionOccupied(t *testing.T) {
	tests := []struct {
		name string
		info partitiontreev1.PartitionInfo
		want bool
	}{
		{
			name: "OCCUPIED occupancy is occupied",
			info: partitiontreev1.PartitionInfo{
				Occupancy: partitiontreev1.OccupancyOccupied,
			},
			want: true,
		},
		{
			name: "EMPTY occupancy is not occupied",
			info: partitiontreev1.PartitionInfo{
				Occupancy: partitiontreev1.OccupancyEmpty,
			},
			want: false,
		},
		{
			name: "No occupancy set is not occupied",
			info: partitiontreev1.PartitionInfo{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPartitionOccupied(&tt.info); got != tt.want {
				t.Errorf("IsPartitionOccupied() = %v, want %v", got, tt.want)
			}
		})
	}
}

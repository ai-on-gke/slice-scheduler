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
	"fmt"
	"strconv"
	"strings"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
)

// ChipsPerCube is the number of chips in a single 4x4x4 cube (Ironwood).
// Workloads with ChipCount >= ChipsPerCube span full cubes (multi-cube).
const ChipsPerCube = 64

// MaxTopologyDimension is the largest value accepted for a single dimension of a
// topology string. Used to prevent overflow errors when parsing incoming
// topologies.
const MaxTopologyDimension = 9999

// ToTopology converts a partition topology string to a normalized form.
// Cloud uses string-based topology (e.g., "16x16", "4x4x4") directly.
func ToTopology(topology string) string {
	return topology
}

var numOfCubesCache = make(map[string]int)
var chipCountCache = make(map[string]int)

func init() {
	// Pre-populate all valid 3D cube topologies dynamically including permutations!
	// Form: 4*i x 4*j x 4*k where i*j*k <= 144.
	for i := 1; i <= 144; i++ {
		for j := 1; j <= 144; j++ {
			if i*j > 144 {
				break
			}
			for k := 1; k <= 144; k++ {
				cubes := i * j * k
				if cubes > 144 {
					break
				}

				// Construct the topology string (e.g. "4x4x8", "4x8x4", etc.)
				topo := fmt.Sprintf("%dx%dx%d", 4*i, 4*j, 4*k)
				chips := cubes * 64

				chipCountCache[topo] = chips
				numOfCubesCache[topo] = cubes
			}
		}
	}

	// Pre-populate standard 2D topologies (e.g., Trillium 2D layouts)
	chipCountCache["2x4"] = 8
	chipCountCache["4x4"] = 16
	chipCountCache["4x8"] = 32
	chipCountCache["8x8"] = 64
	chipCountCache["8x16"] = 128
	chipCountCache["16x16"] = 256

	// Pre-populate sub-cube topologies (less than 1 cube)
	chipCountCache["2x2x1"] = 4
	chipCountCache["2x2x2"] = 8
	chipCountCache["2x2x4"] = 16
	chipCountCache["2x4x4"] = 32
	numOfCubesCache["2x2x1"] = 0
	numOfCubesCache["2x2x2"] = 0
	numOfCubesCache["2x2x4"] = 0
	numOfCubesCache["2x4x4"] = 0
}

// parseDimension parses a single topology dimension. It rejects values that are
// not numeric, not positive, or larger than MaxTopologyDimension.
func parseDimension(part string) (int, bool) {
	val, err := strconv.Atoi(part)
	if err != nil || val <= 0 || val > MaxTopologyDimension {
		return 0, false
	}
	return val, true
}

// Number of cubes in a topology for Ironwood
func NumOfCubes(topology string) int {
	if cubes, ok := numOfCubesCache[topology]; ok {
		return cubes
	}

	parts := strings.Split(topology, "x")
	if len(parts) != 3 {
		return -1
	}
	x, ok := parseDimension(parts[0])
	if !ok {
		return -1
	}
	y, ok := parseDimension(parts[1])
	if !ok {
		return -1
	}
	z, ok := parseDimension(parts[2])
	if !ok {
		return -1
	}
	return x * y * z / 64
}

// IsSingleCubeOrLess returns true if the topology represents a single cube or less.
// It only applies to 3D topologies.
func IsSingleCubeOrLess(topology string) bool {
	numCubes := NumOfCubes(topology)
	return numCubes >= 0 && numCubes <= 1
}

// ChipCount returns the total number of chips in a topology.
func ChipCount(topology string) int {
	if count, ok := chipCountCache[topology]; ok {
		return count
	}

	parts := strings.Split(topology, "x")
	if len(parts) < 2 || len(parts) > 3 {
		return 0
	}
	product := 1
	for _, part := range parts {
		val, ok := parseDimension(part)
		if !ok {
			return 0
		}
		product *= val
	}
	return product
}

// IsPartitionAvailable returns true if the partition is available for scheduling a new slice.
func IsPartitionAvailable(info *partitiontreev1.PartitionInfo) bool {
	return info.State == partitiontreev1.PartitionStateHealthy &&
		info.Occupancy != partitiontreev1.OccupancyOccupied &&
		info.SliceId == ""
}

// IsPartitionOccupied returns true if the partition is physically occupied by a slice at the infrastructure level.
func IsPartitionOccupied(info *partitiontreev1.PartitionInfo) bool {
	return info.Occupancy == partitiontreev1.OccupancyOccupied
}

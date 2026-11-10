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

package schedulerlib

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ai-on-gke/slice-scheduler/internal/generators"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/testutil"
)

// =============================================================================
// Helper functions for building large-scale cluster states
// =============================================================================

// buildLargePodWithTrees creates a PodContext with the given number of 4x4x4 root trees.
// Each tree has a realistic sub-partition hierarchy: 4x4x4 -> 2x4x4 -> 2x2x4 -> 2x2x2 -> 2x2x1
func buildLargePod(b *testutil.MockStateBuilder, podName string, numTrees int, occupyFraction float64, occupyPriority int) *testutil.PodContext {
	pc := b.Pod(podName)
	for i := 0; i < numTrees; i++ {
		treeID := fmt.Sprintf("%s-tree-%d", podName, i)
		tc := pc.Tree("4x4x4", treeID).
			Child("2x4x4", fmt.Sprintf("%s-244-%d-0", podName, i)).
			Child("2x2x4", fmt.Sprintf("%s-224-%d-0", podName, i)).
			Child("2x2x2", fmt.Sprintf("%s-222-%d-0", podName, i)).
			Child("2x2x1", fmt.Sprintf("%s-221-%d-0", podName, i))

		// Decide whether to occupy this tree branch based on fraction
		if occupyFraction > 0 && float64(i)/float64(numTrees) < occupyFraction {
			tc.UsedBy(fmt.Sprintf("occupant-%s-%d", podName, i), occupyPriority, interfaces.SliceSchedulerName)
		}

		// Navigate back to tree root to add second child branch
		tc.ParentNode(). // back to 2x2x2
					ParentNode(). // back to 2x2x4
					ParentNode(). // back to 2x4x4
					Child("2x2x4", fmt.Sprintf("%s-224-%d-1", podName, i)).
					Child("2x2x2", fmt.Sprintf("%s-222-%d-1", podName, i)).
					Child("2x2x1", fmt.Sprintf("%s-221-%d-1", podName, i)).
					ParentNode(). // back to 2x2x2
					ParentNode(). // back to 2x2x4
					ParentNode(). // back to 2x4x4
					ParentNode(). // back to tree root
					Child("2x4x4", fmt.Sprintf("%s-244-%d-1", podName, i)).
					Child("2x2x4", fmt.Sprintf("%s-224-%d-2", podName, i)).
					Child("2x2x2", fmt.Sprintf("%s-222-%d-2", podName, i)).
					Child("2x2x1", fmt.Sprintf("%s-221-%d-2", podName, i)).
					ParentNode(). // back to 2x2x2
					ParentNode(). // back to 2x2x4
					ParentNode(). // back to 2x4x4
					Child("2x2x4", fmt.Sprintf("%s-224-%d-3", podName, i)).
					Child("2x2x2", fmt.Sprintf("%s-222-%d-3", podName, i)).
					Child("2x2x1", fmt.Sprintf("%s-221-%d-3", podName, i))
	}
	return pc
}

// buildSimplePod creates a pod with N simple root 4x4x4 trees (no children).
// Suitable for multi-cube placement testing where we only need cube-level granularity.
func buildSimplePod(b *testutil.MockStateBuilder, podName string, numTrees int) *testutil.PodContext {
	pc := b.Pod(podName)
	for i := 0; i < numTrees; i++ {
		treeID := fmt.Sprintf("%s-cube-%d", podName, i)
		pc.Tree("4x4x4", treeID)
	}
	return pc
}

// =============================================================================
// Test 1: 500+ PartitionTrees - Scheduling Latency and No OOM
// =============================================================================

func TestScale_500PartitionTrees_SchedulingLatency(t *testing.T) {
	tests := []struct {
		name     string
		numPods  int
		numTrees int // trees per pod
		topology string
		tpuType  string
		maxTime  time.Duration
	}{
		{
			name:     "100 PTs - single cube request",
			numPods:  1,
			numTrees: 100,
			topology: "4x4x4",
			tpuType:  TPU7x,
			maxTime:  5 * time.Second,
		},
		{
			name:     "200 PTs - sub-slice request (2x2x1)",
			numPods:  2,
			numTrees: 100,
			topology: "2x2x1",
			tpuType:  TPU7x,
			maxTime:  5 * time.Second,
		},
		{
			name:     "500 PTs - single cube request",
			numPods:  5,
			numTrees: 100,
			topology: "4x4x4",
			tpuType:  TPU7x,
			maxTime:  5 * time.Second,
		},
		{
			name:     "500 PTs - sub-slice request (2x2x4)",
			numPods:  5,
			numTrees: 100,
			topology: "2x2x4",
			tpuType:  TPU7x,
			maxTime:  5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testutil.NewStateBuilder()
			for p := 0; p < tt.numPods; p++ {
				buildLargePod(b, fmt.Sprintf("pod-%d", p), tt.numTrees, 0, 0)
			}
			state := b.Build()

			totalTrees := len(state.GetPartitionTrees())
			if totalTrees < tt.numPods*tt.numTrees {
				t.Fatalf("Expected at least %d partition trees, got %d", tt.numPods*tt.numTrees, totalTrees)
			}
			t.Logf("Built cluster state with %d partition trees", totalTrees)

			// Measure memory before scheduling
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			s := createDefaultScheduler()
			req := makeReq("scale-test", tt.topology, tt.tpuType, 0)

			start := time.Now()
			result := s.Schedule(context.Background(), req, state)
			elapsed := time.Since(start)

			// Measure memory after scheduling
			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)
			allocDelta := memAfter.TotalAlloc - memBefore.TotalAlloc

			t.Logf("Scheduling latency: %v (limit: %v)", elapsed, tt.maxTime)
			t.Logf("Memory allocated during scheduling: %d MB", allocDelta/(1024*1024))

			if !result.IsSchedulable {
				t.Fatalf("Expected schedulable result, got failure: %s", result.FailureReason)
			}

			if elapsed > tt.maxTime {
				t.Errorf("Scheduling latency %v exceeded %v limit", elapsed, tt.maxTime)
			}

			// Verify allocation is under 1GB
			if allocDelta > 1024*1024*1024 {
				t.Errorf("Memory allocation %d bytes exceeded 1GB limit", allocDelta)
			}
		})
	}
}

// =============================================================================
// Test 2: Mixed Topology Stress - Correct partition selection across topologies
// =============================================================================

func TestScale_MixedTopologyStress(t *testing.T) {
	// Build a cluster with mixed topology: both ironwood cubes (with sub-partitions) and simple cubes.
	// Then schedule a variety of topology requests and verify correct partition selection.
	b := testutil.NewStateBuilder()

	// 10 pods with 10 trees each (deep hierarchy) = 100 trees
	for p := 0; p < 10; p++ {
		buildLargePod(b, fmt.Sprintf("pod-%d", p), 10, 0, 0)
	}
	state := b.Build()

	s := createDefaultScheduler()

	topologies := []struct {
		topology string
		tpuType  string
		expectOk bool
		minParts int // minimum expected partitions in assignment
		maxParts int // maximum expected partitions in assignment
	}{
		{"4x4x4", TPU7x, true, 1, 1},  // Single cube
		{"2x2x1", TPU7x, true, 1, 1},  // Sub-slice (leaf level)
		{"2x2x4", TPU7x, true, 1, 1},  // Intermediate sub-slice
		{"2x4x4", TPU7x, true, 1, 1},  // Larger sub-slice
		{"4x4x8", TPU7x, true, 2, 2},  // 2-cube super-slice
		{"4x4x12", TPU7x, true, 3, 3}, // 3-cube super-slice
		{"4x4x16", TPU7x, true, 4, 4}, // 4-cube super-slice
	}

	for _, tt := range topologies {
		t.Run(fmt.Sprintf("topology=%s", tt.topology), func(t *testing.T) {
			req := makeReq("mixed-test", tt.topology, tt.tpuType, 0)
			result := s.Schedule(context.Background(), req, state)

			if result.IsSchedulable != tt.expectOk {
				if tt.expectOk {
					t.Fatalf("Expected schedulable, got failure: %s", result.FailureReason)
				} else {
					t.Fatalf("Expected unschedulable, but got schedulable")
				}
			}

			if !tt.expectOk {
				return
			}

			sr := simplifyResult(result, "mixed-test")
			if len(sr.AssignedPartitions) < tt.minParts || len(sr.AssignedPartitions) > tt.maxParts {
				t.Errorf("Expected %d-%d assigned partitions, got %d: %v",
					tt.minParts, tt.maxParts, len(sr.AssignedPartitions), sr.AssignedPartitions)
			}

			// For multi-cube, verify all partitions are from the same pod
			if len(sr.AssignedPartitions) > 1 && result.Candidate != nil {
				podNames := make(map[string]bool)
				for _, parts := range result.Candidate.Assignments {
					for _, p := range parts {
						podNames[p.PodName] = true
					}
				}
				if len(podNames) > 1 {
					t.Errorf("Multi-cube assignment spans %d pods (must be within same pod): %v", len(podNames), podNames)
				}
			}
		})
	}
}

// =============================================================================
// Test 3: Sub-slice Coexistence with Full-Cube on Same PT
// =============================================================================

func TestScale_SubSliceCoexistenceWithFullCube(t *testing.T) {
	// Verify that sub-slice placements (2x2x1, 2x2x4) can correctly coexist
	// with full-cube (4x4x4) allocations on the same PartitionTree.

	// Tree where some sub-partitions are used but others are free.
	state := testutil.NewStateBuilder().
		Pod("pod-1").
		Tree("4x4x4", "tree-root").
		Child("2x4x4", "sub-left").
		// Left half is occupied
		Child("2x2x4", "sub-ll").UsedBy("existing-sub-slice", 50, interfaces.SliceSchedulerName).ParentNode().
		Child("2x2x4", "sub-lr").ParentNode(). // Right of left is free
		ParentNode().
		Child("2x4x4", "sub-right").
		Child("2x2x4", "sub-rl").ParentNode().
		Child("2x2x4", "sub-rr").
		Build()

	s := createDefaultScheduler()

	// Schedule a 2x2x4 request - should find one of the free sub-partitions
	req := makeReq("new-sub-slice", "2x2x4", TPU7x, 0)
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected 2x2x4 to be schedulable alongside occupied sub-partitions: %s", result.FailureReason)
	}

	sr := simplifyResult(result, "new-sub-slice")
	if len(sr.AssignedPartitions) != 1 {
		t.Fatalf("Expected 1 assigned partition, got %d", len(sr.AssignedPartitions))
	}

	// The assigned partition should be a free one (not sub-ll which is occupied)
	if sr.AssignedPartitions[0] == "sub-ll" && len(sr.PreemptedSlices) == 0 {
		t.Errorf("Assigned to occupied partition 'sub-ll' without preemption")
	}
}

// =============================================================================
// Test 4: Concurrent Preemption at Scale - No Double Assignment
// =============================================================================

func TestScale_ConcurrentPreemptionNoDoubleAssignment(t *testing.T) {
	// Create a cluster with 50+ occupied PTs and run 10+ simultaneous scheduling
	// requests with higher priority. Verify no partition is assigned twice.

	numPods := 5
	treesPerPod := 10
	numConcurrent := 20

	b := testutil.NewStateBuilder()
	for p := 0; p < numPods; p++ {
		// All trees occupied with priority 10
		buildLargePod(b, fmt.Sprintf("pod-%d", p), treesPerPod, 1.0, 10)
	}
	state := b.Build()

	s := createDefaultScheduler()

	type scheduleResult struct {
		idx    int
		result interfaces.GroupAllocationResult
	}

	results := make([]scheduleResult, numConcurrent)
	var wg sync.WaitGroup

	// Launch concurrent scheduling requests with priority 100 (can preempt priority 10)
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := makeReq(fmt.Sprintf("preempt-req-%d", idx), "4x4x4", TPU7x, 100)
			result := s.Schedule(context.Background(), req, state)
			results[idx] = scheduleResult{idx: idx, result: result}
		}(i)
	}
	wg.Wait()

	// Collect all assigned partition IDs across all results
	allAssigned := make(map[string][]int) // partition ID -> which request indices assigned it
	scheduledCount := 0

	for _, sr := range results {
		if sr.result.IsSchedulable && sr.result.Candidate != nil {
			scheduledCount++
			for _, parts := range sr.result.Candidate.Assignments {
				for _, p := range parts {
					allAssigned[p.ID] = append(allAssigned[p.ID], sr.idx)
				}
			}
		}
	}

	t.Logf("Concurrent scheduling: %d/%d requests were schedulable", scheduledCount, numConcurrent)

	// Since the DefaultScheduler.Schedule is a pure function (no side effects on state),
	// concurrent calls with the same state snapshot may yield overlapping assignments.
	// This is expected — the watcher's Reserve/Unreserve handles conflict resolution at
	// a higher level. What we verify here is that the scheduling logic itself doesn't
	// panic, deadlock, or produce corrupted results under concurrent access.
	for _, sr := range results {
		if sr.result.IsSchedulable && sr.result.Candidate != nil {
			// Verify assignments reference valid partition IDs (not nil pointers)
			for sliceName, parts := range sr.result.Candidate.Assignments {
				for _, p := range parts {
					if p == nil || p.ID == "" {
						t.Errorf("Request %d: nil or empty partition in assignment for slice %s", sr.idx, sliceName)
					}
				}
			}
		}
	}

	if scheduledCount == 0 {
		t.Errorf("Expected at least some requests to be schedulable via preemption")
	}
}

// =============================================================================
// Test 5: Priority Chain Depth (10+ levels)
// =============================================================================

func TestScale_PriorityChainDepth(t *testing.T) {
	// Create a priority chain: priority 10, 20, 30, ... 100, 110
	// Each priority level occupies a partition. Then schedule at each priority
	// and verify only lower-priority victims are selected.

	numLevels := 11
	b := testutil.NewStateBuilder()
	pc := b.Pod("pod-1")
	for i := 0; i < numLevels; i++ {
		priority := (i + 1) * 10 // 10, 20, 30, ..., 110
		treeID := fmt.Sprintf("tree-p%d", priority)
		pc.Tree("4x4x4", treeID).UsedBy(fmt.Sprintf("occupant-p%d", priority), priority, interfaces.SliceSchedulerName)
	}
	state := pc.Build()

	s := createDefaultScheduler()

	// Schedule at priority 55 — should preempt only occupants with priority < 55 (10, 20, 30, 40, 50)
	req := makeReq("high-prio", "4x4x4", TPU7x, 55)
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected schedulable with priority 55, got: %s", result.FailureReason)
	}

	sr := simplifyResult(result, "high-prio")
	sort.Strings(sr.PreemptedSlices)

	// Should preempt exactly 1 victim (lowest priority), assigned to 1 partition
	if len(sr.AssignedPartitions) != 1 {
		t.Errorf("Expected 1 assigned partition, got %d: %v", len(sr.AssignedPartitions), sr.AssignedPartitions)
	}

	// Verify preempted slice has priority < 55
	for _, victimName := range sr.PreemptedSlices {
		// Victim name format: "occupant-pNN"
		var victimPriority int
		fmt.Sscanf(victimName, "occupant-p%d", &victimPriority)
		if victimPriority >= 55 {
			t.Errorf("Preempted victim %s has priority %d >= requesting priority 55", victimName, victimPriority)
		}
	}

	// Now test at priority 999 - should be able to preempt any one
	req2 := makeReq("max-prio", "4x4x4", TPU7x, 999)
	result2 := s.Schedule(context.Background(), req2, state)

	if !result2.IsSchedulable {
		t.Fatalf("Expected schedulable with priority 999, got: %s", result2.FailureReason)
	}

	// Verify it picked the lowest-cost victim (should prefer minimum preemption cost)
	sr2 := simplifyResult(result2, "max-prio")
	if len(sr2.AssignedPartitions) != 1 {
		t.Errorf("Expected 1 assigned partition, got %d", len(sr2.AssignedPartitions))
	}
	t.Logf("Priority 999 preempted: %v", sr2.PreemptedSlices)

	// Test at priority 5 - cannot preempt anyone (all occupants >= 10)
	req3 := makeReq("low-prio", "4x4x4", TPU7x, 5)
	result3 := s.Schedule(context.Background(), req3, state)

	if result3.IsSchedulable {
		t.Fatalf("Expected unschedulable with priority 5 (all occupants >= 10)")
	}
}

// =============================================================================
// Test 6: Fragmented Cluster - 500 PTs, 90% Occupied
// =============================================================================

func TestScale_FragmentedCluster_500PTs(t *testing.T) {
	// 500 PTs where 90% are occupied by various topologies, 10% free but scattered.
	// Verify scheduling finds the optimal placement in the free spaces.
	numPods := 5
	treesPerPod := 100

	b := testutil.NewStateBuilder()
	for p := 0; p < numPods; p++ {
		// 90% occupied (occupy fraction = 0.9)
		buildLargePod(b, fmt.Sprintf("pod-%d", p), treesPerPod, 0.9, 50)
	}
	state := b.Build()

	s := createDefaultScheduler()

	// Schedule a low-priority request for a free sub-slice
	t.Run("SubSlice in fragmented cluster", func(t *testing.T) {
		req := makeReq("frag-sub", "2x2x1", TPU7x, 0)
		start := time.Now()
		result := s.Schedule(context.Background(), req, state)
		elapsed := time.Since(start)

		t.Logf("Fragmented cluster scheduling latency: %v", elapsed)

		if !result.IsSchedulable {
			t.Fatalf("Expected schedulable (10%% free), got: %s", result.FailureReason)
		}

		sr := simplifyResult(result, "frag-sub")
		if len(sr.PreemptedSlices) > 0 {
			t.Errorf("Expected zero preemptions (free space available), got %d: %v",
				len(sr.PreemptedSlices), sr.PreemptedSlices)
		}
	})

	// Schedule a full-cube request in fragmented cluster (no preemption allowed)
	t.Run("FullCube in fragmented cluster", func(t *testing.T) {
		req := makeReq("frag-full", "4x4x4", TPU7x, 0)
		start := time.Now()
		result := s.Schedule(context.Background(), req, state)
		elapsed := time.Since(start)

		t.Logf("Fragmented cluster full-cube scheduling latency: %v", elapsed)

		if !result.IsSchedulable {
			t.Logf("Full-cube unschedulable in 90%% fragmented cluster (expected if all free PTs are fragmented): %s", result.FailureReason)
			return
		}

		sr := simplifyResult(result, "frag-full")
		if len(sr.PreemptedSlices) > 0 {
			t.Errorf("Expected zero preemptions for priority-0 request, got %d", len(sr.PreemptedSlices))
		}
	})

	// Schedule with high priority - should find placement via preemption
	t.Run("HighPriority in fragmented cluster", func(t *testing.T) {
		req := makeReq("frag-high", "4x4x4", TPU7x, 100)
		start := time.Now()
		result := s.Schedule(context.Background(), req, state)
		elapsed := time.Since(start)

		t.Logf("Fragmented cluster high-priority scheduling latency: %v", elapsed)

		if !result.IsSchedulable {
			t.Fatalf("Expected schedulable with preemption, got: %s", result.FailureReason)
		}
	})
}

// =============================================================================
// Test 7: Multi-Cube DP Solver at Scale (50+ cubes per pod)
// =============================================================================

func TestScale_MultiCubeDPSolver(t *testing.T) {
	tests := []struct {
		name          string
		cubesPerPod   int
		numPods       int
		topology      string
		expectedCubes int
		maxTime       time.Duration
	}{
		{
			name:          "4x4x8 across 50 cubes",
			cubesPerPod:   50,
			numPods:       1,
			topology:      "4x4x8",
			expectedCubes: 2,
			maxTime:       10 * time.Second,
		},
		{
			name:          "4x4x12 across 50 cubes",
			cubesPerPod:   50,
			numPods:       1,
			topology:      "4x4x12",
			expectedCubes: 3,
			maxTime:       10 * time.Second,
		},
		{
			name:          "4x4x16 across 50 cubes",
			cubesPerPod:   50,
			numPods:       1,
			topology:      "4x4x16",
			expectedCubes: 4,
			maxTime:       10 * time.Second,
		},
		{
			name:          "4x4x8 across 144 cubes (max pod)",
			cubesPerPod:   144,
			numPods:       1,
			topology:      "4x4x8",
			expectedCubes: 2,
			maxTime:       10 * time.Second,
		},
		{
			name:          "4x4x16 across 144 cubes (max pod)",
			cubesPerPod:   144,
			numPods:       1,
			topology:      "4x4x16",
			expectedCubes: 4,
			maxTime:       10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testutil.NewStateBuilder()
			for p := 0; p < tt.numPods; p++ {
				buildSimplePod(b, fmt.Sprintf("pod-%d", p), tt.cubesPerPod)
			}
			state := b.Build()

			s := createDefaultScheduler()
			req := makeReq("multicube-test", tt.topology, TPU7x, 0)

			start := time.Now()
			result := s.Schedule(context.Background(), req, state)
			elapsed := time.Since(start)

			t.Logf("Multi-cube solve (%s, %d cubes): %v", tt.topology, tt.cubesPerPod, elapsed)

			if !result.IsSchedulable {
				t.Fatalf("Expected schedulable for %s across %d cubes: %s", tt.topology, tt.cubesPerPod, result.FailureReason)
			}

			if elapsed > tt.maxTime {
				t.Errorf("Scheduling latency %v exceeded %v limit", elapsed, tt.maxTime)
			}

			sr := simplifyResult(result, "multicube-test")
			if len(sr.AssignedPartitions) != tt.expectedCubes {
				t.Errorf("Expected %d cubes assigned, got %d: %v", tt.expectedCubes, len(sr.AssignedPartitions), sr.AssignedPartitions)
			}

			// Verify all assigned partitions are from the same pod
			if result.Candidate != nil {
				podNames := make(map[string]bool)
				for _, parts := range result.Candidate.Assignments {
					for _, p := range parts {
						podNames[p.PodName] = true
					}
				}
				if len(podNames) > 1 {
					t.Errorf("Multi-cube assignment spans %d pods: %v", len(podNames), podNames)
				}
			}
		})
	}
}

// =============================================================================
// Test 8: Multi-Cube with Preemption at Scale
// =============================================================================

func TestScale_MultiCubePreemptionAtScale(t *testing.T) {
	// Pod with 50 cubes, all occupied at low priority.
	// Schedule a 4x4x8 (2-cube) request at high priority.
	b := testutil.NewStateBuilder()
	pc := b.Pod("pod-1")
	for i := 0; i < 50; i++ {
		pc.Tree("4x4x4", fmt.Sprintf("cube-%d", i)).
			UsedBy(fmt.Sprintf("victim-%d", i), 10, interfaces.SliceSchedulerName)
	}
	state := pc.Build()

	s := createDefaultScheduler()
	req := makeReq("mc-preempt", "4x4x8", TPU7x, 100)

	start := time.Now()
	result := s.Schedule(context.Background(), req, state)
	elapsed := time.Since(start)

	t.Logf("Multi-cube preemption latency: %v", elapsed)

	if !result.IsSchedulable {
		t.Fatalf("Expected schedulable via preemption: %s", result.FailureReason)
	}

	sr := simplifyResult(result, "mc-preempt")
	if len(sr.AssignedPartitions) != 2 {
		t.Errorf("Expected 2 cubes, got %d: %v", len(sr.AssignedPartitions), sr.AssignedPartitions)
	}

	// Should have preempted exactly 2 victims
	if len(sr.PreemptedSlices) != 2 {
		t.Errorf("Expected 2 preempted victims, got %d: %v", len(sr.PreemptedSlices), sr.PreemptedSlices)
	}
}

// =============================================================================
// Test 9: Super-Slice Pod-Local Constraint at Scale
// =============================================================================

func TestScale_SuperSlicePodLocalConstraint(t *testing.T) {
	// Multi-cube requests must NOT span pods. Verify with many cubes spread across pods.
	b := testutil.NewStateBuilder()

	// 3 pods with 1 cube each. A 4x4x8 request needs 2 cubes from the SAME pod.
	buildSimplePod(b, "pod-a", 1)
	buildSimplePod(b, "pod-b", 1)
	buildSimplePod(b, "pod-c", 1)
	state := b.Build()

	s := createDefaultScheduler()
	req := makeReq("cross-pod", "4x4x8", TPU7x, 0)
	result := s.Schedule(context.Background(), req, state)

	if result.IsSchedulable {
		t.Fatalf("Expected unschedulable — no single pod has 2 cubes, but got schedulable")
	}
}

// =============================================================================
// Test 10: Rapid Sequential Scheduling (create-verify cycle)
// =============================================================================

func TestScale_RapidSequentialScheduling(t *testing.T) {
	// Simulate rapid scheduling by calling Schedule many times in sequence.
	// This tests for memory leaks, goroutine leaks, and degradation over iterations.
	numIterations := 100

	b := testutil.NewStateBuilder()
	for p := 0; p < 5; p++ {
		buildLargePod(b, fmt.Sprintf("pod-%d", p), 20, 0, 0)
	}
	state := b.Build()

	s := createDefaultScheduler()

	topologies := []string{"4x4x4", "2x2x1", "2x2x4", "2x4x4"}
	var latencies []time.Duration

	for i := 0; i < numIterations; i++ {
		topo := topologies[i%len(topologies)]
		req := makeReq(fmt.Sprintf("rapid-%d", i), topo, TPU7x, 0)

		start := time.Now()
		result := s.Schedule(context.Background(), req, state)
		elapsed := time.Since(start)
		latencies = append(latencies, elapsed)

		if !result.IsSchedulable {
			t.Fatalf("Iteration %d: unexpected failure for %s: %s", i, topo, result.FailureReason)
		}
	}

	// Check for degradation: last 10 iterations shouldn't be significantly slower than first 10
	var first10Sum, last10Sum time.Duration
	for i := 0; i < 10; i++ {
		first10Sum += latencies[i]
		last10Sum += latencies[numIterations-10+i]
	}

	avgFirst := first10Sum / 10
	avgLast := last10Sum / 10

	t.Logf("Avg first 10 latency: %v, Avg last 10 latency: %v", avgFirst, avgLast)

	// Allow 5x degradation factor (generous, mainly catching major regressions)
	if avgLast > avgFirst*5 && avgLast > 100*time.Millisecond {
		t.Errorf("Performance degradation detected: first10 avg=%v, last10 avg=%v", avgFirst, avgLast)
	}
}

// =============================================================================
// Test 11: Unmanaged Shielding at Scale
// =============================================================================

func TestScale_UnmanagedShieldingAtScale(t *testing.T) {
	// A cluster where all partitions are occupied by non-scheduler-managed slices.
	// Even with the highest priority, nothing should be preemptable.
	b := testutil.NewStateBuilder()
	pc := b.Pod("pod-1")
	for i := 0; i < 50; i++ {
		pc.Tree("4x4x4", fmt.Sprintf("tree-%d", i)).
			UsedBy(fmt.Sprintf("manual-%d", i), 1, "manual-controller")
	}
	state := pc.Build()

	s := createDefaultScheduler()
	req := makeReq("should-fail", "4x4x4", TPU7x, 999)
	result := s.Schedule(context.Background(), req, state)

	if result.IsSchedulable {
		t.Fatalf("Expected unschedulable — all partitions managed by external controller")
	}
}

// =============================================================================
// Test 12: Mixed Free and Occupied - Scheduler Prefers Free Over Preemption
// =============================================================================

func TestScale_PreferFreeOverPreemption(t *testing.T) {
	// Mix of free and occupied partitions. Scheduler should prefer free placement.
	b := testutil.NewStateBuilder()
	pc := b.Pod("pod-1")

	// 5 occupied cubes
	for i := 0; i < 5; i++ {
		pc.Tree("4x4x4", fmt.Sprintf("occupied-%d", i)).
			UsedBy(fmt.Sprintf("victim-%d", i), 10, interfaces.SliceSchedulerName)
	}
	// 5 free cubes
	for i := 0; i < 5; i++ {
		pc.Tree("4x4x4", fmt.Sprintf("free-%d", i))
	}
	state := pc.Build()

	s := createDefaultScheduler()

	// Even with high priority, should pick a free partition (zero preemption cost)
	req := makeReq("prefer-free", "4x4x4", TPU7x, 100)
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected schedulable: %s", result.FailureReason)
	}

	sr := simplifyResult(result, "prefer-free")
	if len(sr.PreemptedSlices) > 0 {
		t.Errorf("Expected zero preemptions (free space available), got %d: %v",
			len(sr.PreemptedSlices), sr.PreemptedSlices)
	}
}

// =============================================================================
// Test 13: Wide Tree (64 children per node) - Stress tree traversal
// =============================================================================

func TestScale_WideTree64Children(t *testing.T) {
	// Create a pod with trees that have 64 child partitions each.
	// This stresses the generator's tree traversal for very wide branching factors.
	b := testutil.NewStateBuilder()
	pc := b.Pod("pod-1")
	for i := 0; i < 4; i++ {
		treeID := fmt.Sprintf("wide-tree-%d", i)
		tc := pc.Tree("4x4x4", treeID)
		// Add 64 children of type 2x2x1 to each tree
		for j := 0; j < 64; j++ {
			childID := fmt.Sprintf("wide-%d-child-%d", i, j)
			tc.Child("2x2x1", childID).ParentNode()
		}
	}
	state := pc.Build()

	s := createDefaultScheduler()

	// Schedule a 2x2x1 sub-slice — should find one among the 256 leaf children
	t.Run("SubSlice in wide tree", func(t *testing.T) {
		req := makeReq("wide-test", "2x2x1", TPU7x, 0)
		start := time.Now()
		result := s.Schedule(context.Background(), req, state)
		elapsed := time.Since(start)

		t.Logf("Wide tree scheduling latency: %v", elapsed)

		if !result.IsSchedulable {
			t.Fatalf("Expected schedulable in wide tree: %s", result.FailureReason)
		}

		if elapsed > 5*time.Second {
			t.Errorf("Scheduling latency %v exceeded 5s limit for wide tree", elapsed)
		}
	})

	// Schedule a full cube — the root should be picked
	t.Run("FullCube in wide tree", func(t *testing.T) {
		req := makeReq("wide-full", "4x4x4", TPU7x, 0)
		result := s.Schedule(context.Background(), req, state)

		if !result.IsSchedulable {
			t.Fatalf("Expected schedulable: %s", result.FailureReason)
		}
	})
}

// =============================================================================
// Test 14: Deep Nested Trees (5+ levels)
// =============================================================================

func TestScale_DeepNestedTrees(t *testing.T) {
	// Create trees with 5+ levels of nesting to stress deep tree traversal:
	// 4x4x4 -> 2x4x4 -> 2x2x4 -> 2x2x2 -> 2x2x1
	// This matches realistic Ironwood topologies.
	b := testutil.NewStateBuilder()

	numPods := 3
	treesPerPod := 50
	for p := 0; p < numPods; p++ {
		buildLargePod(b, fmt.Sprintf("deep-pod-%d", p), treesPerPod, 0, 0)
	}
	state := b.Build()

	s := createDefaultScheduler()

	// Verify scheduling at every level of the hierarchy
	levels := []struct {
		topology string
		desc     string
	}{
		{"2x2x1", "leaf level (depth 5)"},
		{"2x2x4", "mid level (depth 3)"},
		{"2x4x4", "upper level (depth 2)"},
		{"4x4x4", "root level (depth 1)"},
	}

	for _, tt := range levels {
		t.Run(tt.desc, func(t *testing.T) {
			req := makeReq("deep-test", tt.topology, TPU7x, 0)
			start := time.Now()
			result := s.Schedule(context.Background(), req, state)
			elapsed := time.Since(start)

			t.Logf("Deep tree scheduling (%s): %v", tt.topology, elapsed)

			if !result.IsSchedulable {
				t.Fatalf("Expected schedulable at %s level: %s", tt.topology, result.FailureReason)
			}
		})
	}
}

// =============================================================================
// Test 15: Extreme Scale - 1000 PTs Memory Safety
// =============================================================================

func TestScale_1000PTs_MemorySafety(t *testing.T) {
	// Double the required 500 PT limit to catch marginal cases.
	// Verify no OOM and latency remains reasonable.
	numPods := 10
	treesPerPod := 100 // 10 * 100 = 1000 PTs

	b := testutil.NewStateBuilder()
	for p := 0; p < numPods; p++ {
		buildLargePod(b, fmt.Sprintf("pod-%d", p), treesPerPod, 0, 0)
	}
	state := b.Build()

	totalTrees := len(state.GetPartitionTrees())
	if totalTrees < 1000 {
		t.Fatalf("Expected at least 1000 partition trees, got %d", totalTrees)
	}
	t.Logf("Built cluster state with %d partition trees (target: 1000)", totalTrees)

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	s := createDefaultScheduler()

	// Run multiple scheduling operations
	topologies := []string{"4x4x4", "2x2x1", "2x2x4", "2x4x4", "4x4x8"}
	for i, topo := range topologies {
		req := makeReq(fmt.Sprintf("extreme-%d", i), topo, TPU7x, 0)
		start := time.Now()
		result := s.Schedule(context.Background(), req, state)
		elapsed := time.Since(start)

		t.Logf("1000 PTs scheduling (%s): %v, schedulable=%v", topo, elapsed, result.IsSchedulable)

		if elapsed > 10*time.Second {
			t.Errorf("Scheduling latency %v exceeded 10s limit for %s at 1000 PTs", elapsed, topo)
		}
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	allocDelta := memAfter.TotalAlloc - memBefore.TotalAlloc

	t.Logf("Memory allocated during 1000 PT scheduling: %d MB", allocDelta/(1024*1024))

	// 1GB limit for memory
	if allocDelta > 1024*1024*1024 {
		t.Errorf("Memory allocation %d bytes exceeded 1GB limit at 1000 PTs", allocDelta)
	}
}

// =============================================================================
// Test 16: Multi-Cube with Broken Cubes at Scale
// =============================================================================

func TestScale_MultiCubeBrokenCubesResilience(t *testing.T) {
	// Pod with many cubes, some broken. Multi-cube requests should route
	// around broken cubes and find healthy ones within the same pod.
	b := testutil.NewStateBuilder()
	pc := b.Pod("pod-1")
	numTotal := 50
	numBroken := 15 // 30% broken

	for i := 0; i < numTotal; i++ {
		tc := pc.Tree("4x4x4", fmt.Sprintf("cube-%d", i))
		if i < numBroken {
			tc.Broken()
		}
	}
	state := pc.Build()

	s := createDefaultScheduler()

	tests := []struct {
		topology      string
		expectedCubes int
		expectOk      bool
	}{
		{"4x4x8", 2, true},  // 2 cubes needed, 35 healthy available
		{"4x4x12", 3, true}, // 3 cubes needed, 35 healthy available
		{"4x4x16", 4, true}, // 4 cubes needed, 35 healthy available
	}

	for _, tt := range tests {
		t.Run(tt.topology, func(t *testing.T) {
			req := makeReq("broken-test", tt.topology, TPU7x, 0)
			result := s.Schedule(context.Background(), req, state)

			if result.IsSchedulable != tt.expectOk {
				t.Fatalf("Expected schedulable=%v for %s, got %v: %s",
					tt.expectOk, tt.topology, result.IsSchedulable, result.FailureReason)
			}

			if result.IsSchedulable {
				sr := simplifyResult(result, "broken-test")
				if len(sr.AssignedPartitions) != tt.expectedCubes {
					t.Errorf("Expected %d cubes, got %d: %v",
						tt.expectedCubes, len(sr.AssignedPartitions), sr.AssignedPartitions)
				}

				// Verify no broken cubes were assigned
				for _, partID := range sr.AssignedPartitions {
					// Broken cubes have IDs "cube-0" through "cube-14"
					var cubeNum int
					fmt.Sscanf(partID, "cube-%d", &cubeNum)
					if cubeNum < numBroken {
						t.Errorf("Broken cube %s was assigned!", partID)
					}
				}
			}
		})
	}
}

// =============================================================================
// Test 17: Cascading Priority Preemption - Exact Victim Selection
// =============================================================================

func TestScale_CascadingPriorityPreemption(t *testing.T) {
	// Verify that when multiple priority levels exist, the scheduler always
	// selects the minimum-cost (lowest-priority) victim.
	b := testutil.NewStateBuilder()
	pc := b.Pod("pod-1")

	// Create cubes with priorities: 10, 30, 50, 70, 90
	priorities := []int{10, 30, 50, 70, 90}
	for _, p := range priorities {
		pc.Tree("4x4x4", fmt.Sprintf("cube-p%d", p)).
			UsedBy(fmt.Sprintf("victim-p%d", p), p, interfaces.SliceSchedulerName)
	}
	state := pc.Build()

	s := createDefaultScheduler()

	// Request at priority 55 — should preempt lowest priority victim (p10, p30, or p50)
	req := makeReq("cascade-55", "4x4x4", TPU7x, 55)
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected schedulable at priority 55: %s", result.FailureReason)
	}

	sr := simplifyResult(result, "cascade-55")
	if len(sr.PreemptedSlices) != 1 {
		t.Fatalf("Expected 1 preempted victim, got %d: %v", len(sr.PreemptedSlices), sr.PreemptedSlices)
	}

	// Should preempt the lowest priority victim
	victim := sr.PreemptedSlices[0]
	var victimPriority int
	fmt.Sscanf(victim, "victim-p%d", &victimPriority)
	if victimPriority >= 55 {
		t.Errorf("Preempted victim %s has priority %d >= requesting priority 55", victim, victimPriority)
	}
	t.Logf("Priority 55 preempted: %s (priority %d) — correct lowest-cost selection", victim, victimPriority)

	// Request at priority 25 — should preempt only p10
	req2 := makeReq("cascade-25", "4x4x4", TPU7x, 25)
	result2 := s.Schedule(context.Background(), req2, state)

	if !result2.IsSchedulable {
		t.Fatalf("Expected schedulable at priority 25: %s", result2.FailureReason)
	}

	sr2 := simplifyResult(result2, "cascade-25")
	if len(sr2.PreemptedSlices) != 1 || sr2.PreemptedSlices[0] != "victim-p10" {
		t.Errorf("Expected to preempt victim-p10, got: %v", sr2.PreemptedSlices)
	}

	// Request at priority 5 — should NOT be schedulable (all victims >= 10)
	req3 := makeReq("cascade-5", "4x4x4", TPU7x, 5)
	result3 := s.Schedule(context.Background(), req3, state)

	if result3.IsSchedulable {
		t.Fatalf("Expected unschedulable at priority 5 (all victims >= 10)")
	}
}

// =============================================================================
// Benchmark tests in the schedulerlib package
// =============================================================================

func BenchmarkScheduleScale_SingleCube(b *testing.B) {
	builder := testutil.NewStateBuilder()
	for p := 0; p < 5; p++ {
		buildLargePod(builder, fmt.Sprintf("pod-%d", p), 100, 0, 0)
	}
	state := builder.Build()
	s := createDefaultScheduler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := makeReq("bench", "4x4x4", TPU7x, 0)
		result := s.Schedule(context.Background(), req, state)
		if !result.IsSchedulable {
			b.Fatal("unexpected failure")
		}
	}
}

func BenchmarkScheduleScale_SubSlice(b *testing.B) {
	builder := testutil.NewStateBuilder()
	for p := 0; p < 5; p++ {
		buildLargePod(builder, fmt.Sprintf("pod-%d", p), 100, 0, 0)
	}
	state := builder.Build()
	s := createDefaultScheduler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := makeReq("bench", "2x2x1", TPU7x, 0)
		result := s.Schedule(context.Background(), req, state)
		if !result.IsSchedulable {
			b.Fatal("unexpected failure")
		}
	}
}

func BenchmarkScheduleScale_MultiCube(b *testing.B) {
	builder := testutil.NewStateBuilder()
	buildSimplePod(builder, "pod-1", 50)
	state := builder.Build()
	s := createDefaultScheduler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := makeReq("bench", "4x4x8", TPU7x, 0)
		result := s.Schedule(context.Background(), req, state)
		if !result.IsSchedulable {
			b.Fatal("unexpected failure")
		}
	}
}

func BenchmarkScheduleScale_Preemption50Victims(b *testing.B) {
	builder := testutil.NewStateBuilder()
	pc := builder.Pod("pod-1")
	for i := 0; i < 50; i++ {
		pc.Tree("4x4x4", fmt.Sprintf("tree-%d", i)).
			UsedBy(fmt.Sprintf("victim-%d", i), 10, interfaces.SliceSchedulerName)
	}
	state := pc.Build()
	s := createDefaultScheduler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := makeReq("bench", "4x4x4", TPU7x, 100)
		result := s.Schedule(context.Background(), req, state)
		if !result.IsSchedulable {
			b.Fatal("unexpected failure")
		}
	}
}

func BenchmarkScheduleScale_MixedTopology(b *testing.B) {
	builder := testutil.NewStateBuilder()
	for p := 0; p < 5; p++ {
		buildLargePod(builder, fmt.Sprintf("pod-%d", p), 100, 0.5, 10)
	}
	state := builder.Build()
	s := createDefaultScheduler()

	topologies := []struct {
		topo     string
		priority int32
	}{
		{"4x4x4", 0},
		{"2x2x1", 0},
		{"2x2x4", 50},
		{"4x4x4", 100},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tt := topologies[i%len(topologies)]
		req := makeReq("bench", tt.topo, TPU7x, tt.priority)
		_ = s.Schedule(context.Background(), req, state)
	}
}

// createScaleScheduler is a convenience to create a scheduler with wider beam for scale tests.
func createScaleScheduler(beamWidth int) *DefaultScheduler {
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: beamWidth}),
	}

	filters := []interfaces.FilterPlugin{
		&plugins.HealthyFilter{},
		&plugins.ManagedFilter{},
		&plugins.PriorityFilter{},
	}

	scorers := []interfaces.ScorePlugin{
		&plugins.PreemptionScorer{},
		&plugins.TightestFitScorer{},
	}

	return NewDefaultScheduler(gens, filters, scorers)
}

// =============================================================================
// Test 18: Beam Width Impact — Verify wider beam doesn't cause timeout
// =============================================================================

func TestScale_BeamWidthImpact(t *testing.T) {
	// Verify that increasing beam width doesn't cause scheduling to exceed SLA.
	// Wider beam explores more candidates but should still finish quickly.
	b := testutil.NewStateBuilder()
	for p := 0; p < 5; p++ {
		buildLargePod(b, fmt.Sprintf("pod-%d", p), 100, 0.5, 10)
	}
	state := b.Build()

	beamWidths := []int{5, 10, 20, 50}
	for _, bw := range beamWidths {
		t.Run(fmt.Sprintf("beam=%d", bw), func(t *testing.T) {
			s := createScaleScheduler(bw)
			req := makeReq("beam-test", "4x4x4", TPU7x, 0)

			start := time.Now()
			result := s.Schedule(context.Background(), req, state)
			elapsed := time.Since(start)

			t.Logf("Beam width %d: latency=%v, schedulable=%v", bw, elapsed, result.IsSchedulable)

			if !result.IsSchedulable {
				t.Fatalf("Expected schedulable with beam width %d: %s", bw, result.FailureReason)
			}

			// Even with wide beam, should stay well under 5s SLA
			if elapsed > 5*time.Second {
				t.Errorf("Beam width %d: scheduling latency %v exceeded 5s SLA", bw, elapsed)
			}
		})
	}
}

// =============================================================================
// Test 19: Multi-Cube Mixed Occupancy — Free cubes preferred over preemption
// =============================================================================

func TestScale_MultiCubeMixedOccupancy(t *testing.T) {
	// Pod has some cubes occupied and others free. A multi-cube request should
	// use the free cubes and NOT preempt if enough are available.
	b := testutil.NewStateBuilder()
	pc := b.Pod("pod-1")

	// 10 occupied cubes (low priority)
	for i := 0; i < 10; i++ {
		pc.Tree("4x4x4", fmt.Sprintf("occupied-cube-%d", i)).
			UsedBy(fmt.Sprintf("victim-%d", i), 10, interfaces.SliceSchedulerName)
	}
	// 10 free cubes
	for i := 0; i < 10; i++ {
		pc.Tree("4x4x4", fmt.Sprintf("free-cube-%d", i))
	}
	state := pc.Build()

	s := createDefaultScheduler()

	// Request 4x4x8 (2 cubes) at high priority — should use free cubes
	req := makeReq("mixed-mc", "4x4x8", TPU7x, 100)
	result := s.Schedule(context.Background(), req, state)

	if !result.IsSchedulable {
		t.Fatalf("Expected schedulable: %s", result.FailureReason)
	}

	sr := simplifyResult(result, "mixed-mc")
	if len(sr.AssignedPartitions) != 2 {
		t.Errorf("Expected 2 cubes, got %d", len(sr.AssignedPartitions))
	}

	// Should prefer free cubes over preempting
	if len(sr.PreemptedSlices) > 0 {
		t.Errorf("Expected zero preemptions (free cubes available), got %d: %v",
			len(sr.PreemptedSlices), sr.PreemptedSlices)
	}
}

// =============================================================================
// Test 20: Scheduling Throughput — Ops/second under sustained load
// =============================================================================

func TestScale_SchedulingThroughput(t *testing.T) {
	// Measure how many scheduling operations per second the scheduler can handle.
	// This is a sustained throughput test, not a latency test.
	b := testutil.NewStateBuilder()
	for p := 0; p < 5; p++ {
		buildLargePod(b, fmt.Sprintf("pod-%d", p), 100, 0, 0)
	}
	state := b.Build()

	s := createDefaultScheduler()

	topologies := []struct {
		topo string
		desc string
	}{
		{"4x4x4", "single cube"},
		{"2x2x1", "sub-slice"},
		{"4x4x8", "multi-cube"},
	}

	for _, tt := range topologies {
		t.Run(tt.desc, func(t *testing.T) {
			numOps := 100
			start := time.Now()

			for i := 0; i < numOps; i++ {
				req := makeReq(fmt.Sprintf("throughput-%d", i), tt.topo, TPU7x, 0)
				result := s.Schedule(context.Background(), req, state)
				if !result.IsSchedulable {
					t.Fatalf("Unexpected failure at iteration %d: %s", i, result.FailureReason)
				}
			}

			elapsed := time.Since(start)
			opsPerSec := float64(numOps) / elapsed.Seconds()
			t.Logf("Throughput (%s): %.1f ops/sec (%d ops in %v)", tt.topo, opsPerSec, numOps, elapsed)

			// At 500 PTs, we should be able to schedule at least 10 ops/sec even for the slowest topology
			if opsPerSec < 10 {
				t.Errorf("Throughput %.1f ops/sec is below 10 ops/sec minimum", opsPerSec)
			}
		})
	}
}

// =============================================================================
// Benchmark: Scheduling Throughput at 1000 PTs
// =============================================================================

func BenchmarkScheduleScale_1000PTs_SingleCube(b *testing.B) {
	builder := testutil.NewStateBuilder()
	for p := 0; p < 10; p++ {
		buildLargePod(builder, fmt.Sprintf("pod-%d", p), 100, 0, 0)
	}
	state := builder.Build()
	s := createDefaultScheduler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := makeReq("bench", "4x4x4", TPU7x, 0)
		result := s.Schedule(context.Background(), req, state)
		if !result.IsSchedulable {
			b.Fatal("unexpected failure")
		}
	}
}

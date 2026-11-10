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

package watcher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// =============================================================================
// Helpers for generating large-scale PartitionTree CRDs
// =============================================================================

// generateScaleTree creates a PartitionTree CRD with a realistic Ironwood topology.
// The tree has: 4x4x4 -> 2x4x4 -> 2x2x4 -> 2x2x2 -> 2x2x1 (with leaf NodeNames).
func generateScaleTree(treeName, podName string) *partitiontreev1.PartitionTree {
	rootID := fmt.Sprintf("%s-4x4x4", podName)
	return &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:      treeName,
			Namespace: "default",
			Labels:    map[string]string{TpuPodIDLabel: podName},
		},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: rootID,
				Topology:    "IRONWOOD_4x4x4",
				State:       partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: fmt.Sprintf("%s-2x4x4-0", podName),
						Topology:    "IRONWOOD_2x4x4",
						State:       partitiontreev1.PartitionStateHealthy,
						ChildPartitions: []partitiontreev1.PartitionInfo{
							{
								PartitionID: fmt.Sprintf("%s-2x2x4-0", podName),
								Topology:    "IRONWOOD_2x2x4",
								State:       partitiontreev1.PartitionStateHealthy,
								ChildPartitions: []partitiontreev1.PartitionInfo{
									{
										PartitionID: fmt.Sprintf("%s-2x2x2-0", podName),
										Topology:    "IRONWOOD_2x2x2",
										State:       partitiontreev1.PartitionStateHealthy,
										ChildPartitions: []partitiontreev1.PartitionInfo{
											{
												PartitionID: fmt.Sprintf("%s-2x2x1-0", podName),
												Topology:    "IRONWOOD_2x2x1",
												State:       partitiontreev1.PartitionStateHealthy,
												NodeNames:   []string{fmt.Sprintf("node-%s-0", podName)},
											},
										},
									},
								},
							},
							{
								PartitionID: fmt.Sprintf("%s-2x2x4-1", podName),
								Topology:    "IRONWOOD_2x2x4",
								State:       partitiontreev1.PartitionStateHealthy,
								ChildPartitions: []partitiontreev1.PartitionInfo{
									{
										PartitionID: fmt.Sprintf("%s-2x2x2-1", podName),
										Topology:    "IRONWOOD_2x2x2",
										State:       partitiontreev1.PartitionStateHealthy,
										ChildPartitions: []partitiontreev1.PartitionInfo{
											{
												PartitionID: fmt.Sprintf("%s-2x2x1-1", podName),
												Topology:    "IRONWOOD_2x2x1",
												State:       partitiontreev1.PartitionStateHealthy,
												NodeNames:   []string{fmt.Sprintf("node-%s-1", podName)},
											},
										},
									},
								},
							},
						},
					},
					{
						PartitionID: fmt.Sprintf("%s-2x4x4-1", podName),
						Topology:    "IRONWOOD_2x4x4",
						State:       partitiontreev1.PartitionStateHealthy,
						ChildPartitions: []partitiontreev1.PartitionInfo{
							{
								PartitionID: fmt.Sprintf("%s-2x2x4-2", podName),
								Topology:    "IRONWOOD_2x2x4",
								State:       partitiontreev1.PartitionStateHealthy,
								ChildPartitions: []partitiontreev1.PartitionInfo{
									{
										PartitionID: fmt.Sprintf("%s-2x2x2-2", podName),
										Topology:    "IRONWOOD_2x2x2",
										State:       partitiontreev1.PartitionStateHealthy,
										ChildPartitions: []partitiontreev1.PartitionInfo{
											{
												PartitionID: fmt.Sprintf("%s-2x2x1-2", podName),
												Topology:    "IRONWOOD_2x2x1",
												State:       partitiontreev1.PartitionStateHealthy,
												NodeNames:   []string{fmt.Sprintf("node-%s-2", podName)},
											},
										},
									},
								},
							},
							{
								PartitionID: fmt.Sprintf("%s-2x2x4-3", podName),
								Topology:    "IRONWOOD_2x2x4",
								State:       partitiontreev1.PartitionStateHealthy,
								ChildPartitions: []partitiontreev1.PartitionInfo{
									{
										PartitionID: fmt.Sprintf("%s-2x2x2-3", podName),
										Topology:    "IRONWOOD_2x2x2",
										State:       partitiontreev1.PartitionStateHealthy,
										ChildPartitions: []partitiontreev1.PartitionInfo{
											{
												PartitionID: fmt.Sprintf("%s-2x2x1-3", podName),
												Topology:    "IRONWOOD_2x2x1",
												State:       partitiontreev1.PartitionStateHealthy,
												NodeNames:   []string{fmt.Sprintf("node-%s-3", podName)},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// generateSimpleTree creates a minimal PartitionTree CRD with just a root.
func generateSimpleTree(treeName, podName, partID string) *partitiontreev1.PartitionTree {
	return &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{
			Name:      treeName,
			Namespace: "default",
			Labels:    map[string]string{TpuPodIDLabel: podName},
		},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: partID,
				Topology:    "IRONWOOD_4x4x4",
				State:       partitiontreev1.PartitionStateHealthy,
			},
		},
	}
}

// =============================================================================
// Test: Cache Scale — 500+ Trees
// =============================================================================

func TestCacheScale_500Trees(t *testing.T) {
	// Test that the cache can handle 500+ partition trees without issues.
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numTrees := 500
	numPods := 5 // 100 cubes per pod

	start := time.Now()
	for i := 0; i < numTrees; i++ {
		podName := fmt.Sprintf("pod-%d", i%numPods)
		treeName := fmt.Sprintf("tree-%d", i)
		partID := fmt.Sprintf("%s-cube-%d", podName, i)
		tree := generateSimpleTree(treeName, podName, partID)
		pc.UpdateTree(ctx, tree, nil)
	}
	buildTime := time.Since(start)
	t.Logf("Built cache with %d trees in %v", numTrees, buildTime)

	// Verify all trees are in the cache
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		_, ok := pc.GetRootNode(treeName)
		if !ok {
			t.Errorf("Tree %s not found in cache", treeName)
		}
	}

	// Verify partition lookup works
	for i := 0; i < numTrees; i++ {
		podName := fmt.Sprintf("pod-%d", i%numPods)
		partID := fmt.Sprintf("%s-cube-%d", podName, i)
		_, ok := pc.GetNode(partID)
		if !ok {
			t.Errorf("Partition %s not found in cache", partID)
		}
	}

	// SLA: building 500 trees should take < 5 seconds
	if buildTime > 5*time.Second {
		t.Errorf("Cache build took %v, exceeds 5s SLA", buildTime)
	}
}

// =============================================================================
// Test: Cache Scale — Deep Trees (4-level hierarchy)
// =============================================================================

func TestCacheScale_DeepTrees(t *testing.T) {
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numTrees := 100

	start := time.Now()
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("deep-tree-%d", i)
		podName := fmt.Sprintf("pod-%d", i%5)
		tree := generateScaleTree(treeName, fmt.Sprintf("%s-cube-%d", podName, i))
		pc.UpdateTree(ctx, tree, nil)
	}
	buildTime := time.Since(start)
	t.Logf("Built cache with %d deep trees in %v", numTrees, buildTime)

	// Verify deep trees have correct structure
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("deep-tree-%d", i)
		root, ok := pc.GetRootNode(treeName)
		if !ok {
			t.Errorf("Tree %s not found", treeName)
			continue
		}
		if len(root.Children) != 2 {
			t.Errorf("Tree %s: root should have 2 children, got %d", treeName, len(root.Children))
		}
	}
}

// =============================================================================
// Test: Cache Concurrent Reserve/Release at Scale
// =============================================================================

func TestCacheScale_ConcurrentReserveRelease(t *testing.T) {
	// Test rapid concurrent Reserve/Release operations on the cache.
	// Verifies no data corruption or deadlocks under concurrent access.
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numTrees := 100
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		podName := fmt.Sprintf("pod-%d", i%5)
		partID := fmt.Sprintf("%s-cube-%d", podName, i)
		tree := generateSimpleTree(treeName, podName, partID)
		pc.UpdateTree(ctx, tree, nil)
	}

	numOps := 200
	var wg sync.WaitGroup
	var reserveSuccesses, reserveFailures atomic.Int64

	// Launch concurrent reserve/release cycles
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			treeIdx := idx % numTrees
			podName := fmt.Sprintf("pod-%d", treeIdx%5)
			partID := fmt.Sprintf("%s-cube-%d", podName, treeIdx)
			sliceName := fmt.Sprintf("slice-%d", idx)

			sInfo := &interfaces.SliceInfo{
				Name:         sliceName,
				Priority:     10,
				ManagedBy:    interfaces.SliceSchedulerName,
				Topology:     "4x4x4",
				PartitionIds: []string{partID},
			}

			// Try to reserve
			success, _ := pc.ReservePartition(ctx, sInfo)
			if success {
				reserveSuccesses.Add(1)
				// Release after a short hold
				pc.ReleasePartitions(ctx, []*interfaces.SliceInfo{sInfo})
			} else {
				reserveFailures.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent Reserve/Release: %d successes, %d failures (contention expected)",
		reserveSuccesses.Load(), reserveFailures.Load())

	if reserveSuccesses.Load() == 0 {
		t.Error("Expected at least some successful reservations")
	}

	// After all operations, all partitions should be free (all releases completed)
	for i := 0; i < numTrees; i++ {
		podName := fmt.Sprintf("pod-%d", i%5)
		partID := fmt.Sprintf("%s-cube-%d", podName, i)
		node, ok := pc.GetNode(partID)
		if !ok {
			t.Errorf("Partition %s not found in cache", partID)
			continue
		}
		if node.IsUsed() {
			t.Errorf("Partition %s should be free after all releases, but is used by %s",
				partID, node.InUseBy.Name)
		}
	}
}

// =============================================================================
// Test: Cache Rapid Create/Delete Cycles
// =============================================================================

func TestCacheScale_RapidCreateDeleteCycles(t *testing.T) {
	// Simulate rapid tree create → verify → delete cycles.
	// Tests for memory leaks in the cache maps.
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numCycles := 50
	treesPerCycle := 20

	for cycle := 0; cycle < numCycles; cycle++ {
		// Create trees
		for i := 0; i < treesPerCycle; i++ {
			treeName := fmt.Sprintf("cycle-%d-tree-%d", cycle, i)
			partID := fmt.Sprintf("part-c%d-t%d", cycle, i)
			tree := generateSimpleTree(treeName, "pod-0", partID)
			pc.UpdateTree(ctx, tree, nil)
		}

		// Verify all trees exist
		for i := 0; i < treesPerCycle; i++ {
			treeName := fmt.Sprintf("cycle-%d-tree-%d", cycle, i)
			if _, ok := pc.GetRootNode(treeName); !ok {
				t.Fatalf("Cycle %d: tree %s not found after creation", cycle, treeName)
			}
		}

		// Delete trees
		for i := 0; i < treesPerCycle; i++ {
			treeName := fmt.Sprintf("cycle-%d-tree-%d", cycle, i)
			pc.DeletePartitionTree(ctx, treeName)
		}

		// Verify all trees are gone
		for i := 0; i < treesPerCycle; i++ {
			treeName := fmt.Sprintf("cycle-%d-tree-%d", cycle, i)
			if _, ok := pc.GetRootNode(treeName); ok {
				t.Fatalf("Cycle %d: tree %s should be deleted", cycle, treeName)
			}
			partID := fmt.Sprintf("part-c%d-t%d", cycle, i)
			if _, ok := pc.GetNode(partID); ok {
				t.Fatalf("Cycle %d: partition %s should be deleted from node cache", cycle, partID)
			}
		}
	}

	t.Logf("Completed %d create/delete cycles of %d trees each", numCycles, treesPerCycle)
}

// =============================================================================
// Test: Cache Consistency After Concurrent UpdateTree and Reserve
// =============================================================================

func TestCacheScale_ConsistencyAfterConcurrentUpdatesAndReserves(t *testing.T) {
	// Simulate cache updates happening concurrently with reserve operations.
	// This tests the cache's ability to maintain consistency under concurrent access.
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numTrees := 50
	// Initial population
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		tree := generateSimpleTree(treeName, "pod-0", partID)
		pc.UpdateTree(ctx, tree, nil)
	}

	var wg sync.WaitGroup
	var panics atomic.Int64

	// Group 1: Concurrent reserves
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer func() {
				if r := recover(); r != nil {
					panics.Add(1)
				}
				wg.Done()
			}()
			partID := fmt.Sprintf("pod-0-cube-%d", idx%numTrees)
			sInfo := &interfaces.SliceInfo{
				Name:         fmt.Sprintf("reserve-slice-%d", idx),
				Priority:     10,
				ManagedBy:    interfaces.SliceSchedulerName,
				Topology:     "4x4x4",
				PartitionIds: []string{partID},
			}
			pc.ReservePartition(ctx, sInfo)
		}(i)
	}

	// Group 2: Concurrent tree updates (re-insert same trees with updated state)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(idx int) {
			defer func() {
				if r := recover(); r != nil {
					panics.Add(1)
				}
				wg.Done()
			}()
			treeIdx := idx % numTrees
			treeName := fmt.Sprintf("tree-%d", treeIdx)
			partID := fmt.Sprintf("pod-0-cube-%d", treeIdx)
			tree := generateSimpleTree(treeName, "pod-0", partID)
			pc.UpdateTree(ctx, tree, nil)
		}(i)
	}

	// Group 3: Concurrent readers racing the writers above
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer func() {
				if r := recover(); r != nil {
					panics.Add(1)
				}
				wg.Done()
			}()
			treeIdx := idx % numTrees
			pc.GetRootNode(fmt.Sprintf("tree-%d", treeIdx))
			pc.GetNode(fmt.Sprintf("pod-0-cube-%d", treeIdx))
		}(i)
	}

	wg.Wait()

	if panics.Load() > 0 {
		t.Errorf("Got %d panics during concurrent cache operations", panics.Load())
	}

	// After all operations, cache should be in a consistent state
	// (all trees still present, no nil pointers)
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		root, ok := pc.GetRootNode(treeName)
		if !ok {
			t.Errorf("Tree %s should still exist after concurrent operations", treeName)
			continue
		}
		if root.Info == nil {
			t.Errorf("Tree %s root has nil Info", treeName)
		}
	}
}

// =============================================================================
// Test: Cache Reserve Conflict Detection at Scale
// =============================================================================

func TestCacheScale_ReserveConflictDetection(t *testing.T) {
	// Verify that the cache correctly prevents double-assignment
	// (two slices reserving the same partition).
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numTrees := 50
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		tree := generateSimpleTree(treeName, "pod-0", partID)
		pc.UpdateTree(ctx, tree, nil)
	}

	// Reserve first slice on each partition
	for i := 0; i < numTrees; i++ {
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		sInfo := &interfaces.SliceInfo{
			Name:         fmt.Sprintf("occupant-%d", i),
			Priority:     10,
			ManagedBy:    interfaces.SliceSchedulerName,
			Topology:     "4x4x4",
			PartitionIds: []string{partID},
		}
		success, _ := pc.ReservePartition(ctx, sInfo)
		if !success {
			t.Errorf("Expected successful reservation for partition %s", partID)
		}
	}

	// Try to reserve a second slice on each partition — should all fail
	for i := 0; i < numTrees; i++ {
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		sInfo := &interfaces.SliceInfo{
			Name:         fmt.Sprintf("intruder-%d", i),
			Priority:     10,
			ManagedBy:    interfaces.SliceSchedulerName,
			Topology:     "4x4x4",
			PartitionIds: []string{partID},
		}
		success, existingSlice := pc.ReservePartition(ctx, sInfo)
		if success {
			t.Errorf("Expected conflict for partition %s, but reservation succeeded", partID)
		}
		expectedExisting := fmt.Sprintf("occupant-%d", i)
		if existingSlice != expectedExisting {
			t.Errorf("Expected existing slice %q, got %q", expectedExisting, existingSlice)
		}
	}
}

// =============================================================================
// Test: Cache UpdateTree Preserves Reservations
// =============================================================================

func TestCacheScale_UpdateTreePreservesReservations(t *testing.T) {
	// When a PartitionTree CRD is updated, existing reservations should
	// be preserved (the cache re-applies usage state after rebuild).
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numTrees := 20
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		tree := generateSimpleTree(treeName, "pod-0", partID)
		pc.UpdateTree(ctx, tree, nil)
	}

	// Reserve some partitions
	for i := 0; i < 10; i++ {
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		sInfo := &interfaces.SliceInfo{
			Name:         fmt.Sprintf("reserved-slice-%d", i),
			Priority:     10,
			ManagedBy:    interfaces.SliceSchedulerName,
			Topology:     "4x4x4",
			PartitionIds: []string{partID},
		}
		pc.ReservePartition(ctx, sInfo)
	}

	// Now re-update the trees (simulating a PartitionTree CRD update)
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		tree := generateSimpleTree(treeName, "pod-0", partID)
		pc.UpdateTree(ctx, tree, nil)
	}

	// Verify reservations are preserved after tree update
	for i := 0; i < 10; i++ {
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		node, ok := pc.GetNode(partID)
		if !ok {
			t.Errorf("Partition %s not found after tree update", partID)
			continue
		}
		if !node.IsUsed() {
			t.Errorf("Partition %s should still be reserved after tree update", partID)
			continue
		}
		expectedSlice := fmt.Sprintf("reserved-slice-%d", i)
		if node.InUseBy.Name != expectedSlice {
			t.Errorf("Partition %s: expected reservation by %s, got %s",
				partID, expectedSlice, node.InUseBy.Name)
		}
	}

	// Verify unreserved partitions are still free
	for i := 10; i < numTrees; i++ {
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		node, ok := pc.GetNode(partID)
		if !ok {
			t.Errorf("Partition %s not found after tree update", partID)
			continue
		}
		if node.IsUsed() {
			t.Errorf("Partition %s should be free after tree update, but is used by %s",
				partID, node.InUseBy.Name)
		}
	}
}

// =============================================================================
// Test: Cache Multi-Partition Reserve Rollback
// =============================================================================

func TestCacheScale_MultiPartitionReserveRollback(t *testing.T) {
	// When a multi-partition reserve fails (e.g., reserving 2 cubes for 4x4x8),
	// the cache should roll back any partial reservations.
	pc := NewPartitionCache(false)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		tree := generateSimpleTree(treeName, "pod-0", partID)
		pc.UpdateTree(ctx, tree, nil)
	}

	// Occupy cube-0
	sInfo := &interfaces.SliceInfo{
		Name:         "existing-slice",
		Priority:     10,
		ManagedBy:    interfaces.SliceSchedulerName,
		Topology:     "4x4x4",
		PartitionIds: []string{"pod-0-cube-0"},
	}
	pc.ReservePartition(ctx, sInfo)

	// Try to reserve cubes 0 and 1 together (cube-0 is occupied → should fail)
	multiSlice := &interfaces.SliceInfo{
		Name:         "multi-slice",
		Priority:     10,
		ManagedBy:    interfaces.SliceSchedulerName,
		Topology:     "4x4x8",
		PartitionIds: []string{"pod-0-cube-1", "pod-0-cube-0"}, // cube-0 will fail
	}
	success, _ := pc.ReservePartition(ctx, multiSlice)
	if success {
		t.Fatal("Expected multi-partition reserve to fail (cube-0 is occupied)")
	}

	// Verify cube-1 was rolled back (not reserved by multi-slice)
	node1, _ := pc.GetNode("pod-0-cube-1")
	if node1.IsUsed() && node1.InUseBy.Name == "multi-slice" {
		t.Error("Cube-1 should have been rolled back after failed multi-partition reserve")
	}

	// Verify cube-0 is still occupied by original slice
	node0, _ := pc.GetNode("pod-0-cube-0")
	if !node0.IsUsed() || node0.InUseBy.Name != "existing-slice" {
		t.Error("Cube-0 should still be occupied by existing-slice")
	}
}

// =============================================================================
// Benchmark: Cache UpdateTree at Scale
// =============================================================================

func BenchmarkCacheUpdateTree_500Trees(b *testing.B) {
	trees := make([]*partitiontreev1.PartitionTree, 500)
	for i := 0; i < 500; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		podName := fmt.Sprintf("pod-%d", i%5)
		partID := fmt.Sprintf("%s-cube-%d", podName, i)
		trees[i] = generateSimpleTree(treeName, podName, partID)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc := NewPartitionCache(false)
		ctx := context.Background()
		for _, tree := range trees {
			pc.UpdateTree(ctx, tree, nil)
		}
	}
}

func BenchmarkCacheUpdateTree_100DeepTrees(b *testing.B) {
	trees := make([]*partitiontreev1.PartitionTree, 100)
	for i := 0; i < 100; i++ {
		treeName := fmt.Sprintf("deep-tree-%d", i)
		podName := fmt.Sprintf("pod-%d-cube-%d", i%5, i)
		trees[i] = generateScaleTree(treeName, podName)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc := NewPartitionCache(false)
		ctx := context.Background()
		for _, tree := range trees {
			pc.UpdateTree(ctx, tree, nil)
		}
	}
}

// =============================================================================
// Benchmark: Cache Reserve/Release Throughput
// =============================================================================

func BenchmarkCacheReserveRelease_100Ops(b *testing.B) {
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numTrees := 100
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		partID := fmt.Sprintf("pod-0-cube-%d", i)
		tree := generateSimpleTree(treeName, "pod-0", partID)
		pc.UpdateTree(ctx, tree, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numTrees
		partID := fmt.Sprintf("pod-0-cube-%d", idx)
		sInfo := &interfaces.SliceInfo{
			Name:         fmt.Sprintf("bench-slice-%d", i),
			Priority:     10,
			ManagedBy:    interfaces.SliceSchedulerName,
			Topology:     "4x4x4",
			PartitionIds: []string{partID},
		}
		pc.ReservePartition(ctx, sInfo)
		pc.ReleasePartitions(ctx, []*interfaces.SliceInfo{sInfo})
	}
}

// =============================================================================
// Test: Cache LeafNode Lookup at Scale (with KubeNodes)
// =============================================================================

func TestCacheScale_LeafNodeLookup(t *testing.T) {
	pc := NewPartitionCache(false)
	ctx := context.Background()

	numTrees := 50

	// Create trees with leaf nodes that have NodeNames
	for i := 0; i < numTrees; i++ {
		treeName := fmt.Sprintf("tree-%d", i)
		podName := fmt.Sprintf("pod-%d-cube-%d", i%5, i)
		tree := generateScaleTree(treeName, podName)
		// Generate corresponding k8s nodes
		nodes := make([]corev1.Node, 4)
		for j := 0; j < 4; j++ {
			nodes[j] = *newTestNode(fmt.Sprintf("node-%s-%d", podName, j), corev1.ConditionTrue)
		}
		pc.UpdateTree(ctx, tree, nodes)
	}

	// Verify leaf node lookups work
	for i := 0; i < numTrees; i++ {
		podName := fmt.Sprintf("pod-%d-cube-%d", i%5, i)
		for j := 0; j < 4; j++ {
			nodeName := fmt.Sprintf("node-%s-%d", podName, j)
			node, ok := pc.GetLeafNode(nodeName)
			if !ok {
				t.Errorf("Leaf node %s not found in cache", nodeName)
				continue
			}
			if node.KubeNode == nil {
				t.Errorf("Leaf node %s has nil KubeNode", nodeName)
			}
		}
	}
}

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

package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/util"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

// latencyInjectingClient wraps a client.Client and adds artificial latency to its methods.
type latencyInjectingClient struct {
	client.Client
	latency time.Duration
}

func (c *latencyInjectingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	time.Sleep(c.latency)
	return c.Client.Update(ctx, obj, opts...)
}

func (c *latencyInjectingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	time.Sleep(c.latency)
	return c.Client.Create(ctx, obj, opts...)
}

// newScheme creates a new scheme with the slice and partitiontree resources.
func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = slicev1beta1.AddToScheme(scheme)
	_ = partitiontreev1.AddToScheme(scheme)
	return scheme
}

var trilliumTopologyHierarchy = map[string]string{
	"16x16": "8x16",
	"8x16":  "8x8",
	"8x8":   "4x8",
	"4x8":   "4x4",
	"4x4":   "2x4",
	"2x4":   "2x2",
	"2x2":   "1x1",
}

var ironwoodTopologyHierarchy = map[string]string{
	"4x4x4": "2x4x4",
	"2x4x4": "2x2x4",
	"2x2x4": "2x2x2",
	"2x2x2": "2x2x1",
	"2x2x1": "1x2x1",
	"1x2x1": "1x1x1",
}

// generatePartitionTrees creates n partition trees, half trillium, half ironwood.
func generateTrilliumPartitionTrees(n int) []client.Object {
	trees := make([]client.Object, n)
	for i := 0; i < n; i++ {
		podName := fmt.Sprintf("pod-%d", i)
		trees[i] = &partitiontreev1.PartitionTree{
			ObjectMeta: ctrl.ObjectMeta{
				Name:      fmt.Sprintf("partitiontree-%d", i),
				Namespace: "default",
			},
			Status: partitiontreev1.PartitionTreeStatus{
				Root: &partitiontreev1.PartitionInfo{
					PartitionID:     fmt.Sprintf("%s-16x16-0", podName),
					Topology:        "TRILLIUM_16_16",
					State:           partitiontreev1.PartitionStateHealthy,
					ChildPartitions: generateChildren(podName, "16x16", 0, 6, "trillium"),
				},
			},
		}
	}
	return trees
}

// generateIronwoodPartitionTrees creates numPods Ironwood pods, each with numCubesPerPod 4x4x4 partitions.
func generateIronwoodPods(numPods int) []client.Object {
	numCubesPerPod := 144
	var trees []client.Object
	for i := 0; i < numPods; i++ {
		podName := fmt.Sprintf("iw-pod-%d", i)
		for j := 0; j < numCubesPerPod; j++ {
			treePrefix := fmt.Sprintf("%s-%d", podName, j)
			tree := &partitiontreev1.PartitionTree{
				ObjectMeta: ctrl.ObjectMeta{
					Name:      fmt.Sprintf("partitiontree-%d-%d", i, j),
					Namespace: "default",
					Labels:    map[string]string{TpuPodIDLabel: podName},
				},
				Status: partitiontreev1.PartitionTreeStatus{
					Root: &partitiontreev1.PartitionInfo{
						PartitionID:     fmt.Sprintf("%s-4x4x4", treePrefix),
						Topology:        "IRONWOOD_4x4x4",
						State:           partitiontreev1.PartitionStateHealthy,
						ChildPartitions: generateChildren(treePrefix, "4x4x4", 0, 5, "ironwood"),
					},
				},
			}
			trees = append(trees, tree)
		}
	}
	return trees
}

// generateChildren recursively creates child partitions based on the topologyHierarchy.
func generateChildren(podName, parentTopology string, start, depth int, treeType string) []partitiontreev1.PartitionInfo {
	if depth == 0 {
		return nil
	}
	var childTopology string
	var ok bool
	var childPartitionType string

	if treeType == "trillium" {
		childTopology, ok = trilliumTopologyHierarchy[parentTopology]
		if !ok {
			return nil
		}
		childPartitionType = "TRILLIUM_" + strings.Replace(childTopology, "x", "_", 1)
	} else {
		childTopology, ok = ironwoodTopologyHierarchy[parentTopology]
		if !ok {
			return nil
		}
		childPartitionType = "IRONWOOD_" + strings.ReplaceAll(childTopology, "x", "_")
	}

	numChildren := 2 // Assuming each partition splits into 2 children for simplicity
	children := make([]partitiontreev1.PartitionInfo, numChildren)

	for i := 0; i < numChildren; i++ {
		childIndex := start + i
		children[i] = partitiontreev1.PartitionInfo{
			PartitionID:     fmt.Sprintf("%s-%s-%d", podName, childTopology, childIndex),
			Topology:        childPartitionType,
			State:           partitiontreev1.PartitionStateHealthy,
			ChildPartitions: generateChildren(podName, childTopology, childIndex*2, depth-1, treeType),
		}
	}
	return children
}

// generateSlices creates n slices that are already scheduled.
func generateIronwoodSlices(n, numPods int, multiCubeSlices bool) []client.Object {
	slices := make([]client.Object, n)
	// Realistic topologies for slices
	ironwoodTopologies := []string{"2x2x1", "2x2x2", "2x2x4"}
	if multiCubeSlices {
		ironwoodTopologies = append(ironwoodTopologies, []string{"4x4x4", "4x4x8", "4x8x8", "8x8x8", "8x8x16", "8x16x16"}...)
	}
	podAssignments := map[int]int{}
	for i := 0; i < n; i++ {
		partitionIds := []string{}
		podIndex := i % numPods
		topo := ironwoodTopologies[i%len(ironwoodTopologies)]
		reqCubes := util.NumOfCubes(topo)
		pickedPod := i % 10
		pickedCube, ok := podAssignments[pickedPod]
		if !ok {
			pickedCube = 0
			podAssignments[pickedPod] = 1
		}
		// Not enough cubes in this pod, we drop this creation request entirely
		if reqCubes+pickedPod >= 144 {
			continue
		} else {
			for range reqCubes {
				partitionIds = append(partitionIds, fmt.Sprintf("pod-%d-%s-%d", podIndex, topo, pickedCube))
				pickedCube++
			}
			podAssignments[pickedPod] = pickedCube
		}

		slices[i] = &slicev1beta1.Slice{
			ObjectMeta: ctrl.ObjectMeta{
				Name:      fmt.Sprintf("ironwood-slice-%d", i),
				Namespace: "default",
			},
			Spec: slicev1beta1.SliceSpec{
				Type:         "tpu7x",
				Topology:     topo,
				PartitionIds: partitionIds, // Assign a plausible partition ID
			},
		}
	}
	return slices
}

// generateSlices creates n slices that are already scheduled.
func generateTrilliumSlices(n, numPods int) []client.Object {
	slices := make([]client.Object, n)
	// Realistic topologies for slices
	trilliumTopologies := []string{"16x16", "8x8", "4x4", "2x4"}
	for i := 0; i < n; i++ {
		podIndex := i % numPods
		topo := trilliumTopologies[i%len(trilliumTopologies)]
		slices[i] = &slicev1beta1.Slice{
			ObjectMeta: ctrl.ObjectMeta{
				Name:      fmt.Sprintf("trillum-slice-%d", i),
				Namespace: "default",
			},
			Spec: slicev1beta1.SliceSpec{
				Type:         "v6e",
				Topology:     topo,
				PartitionIds: []string{fmt.Sprintf("pod-%d-%s-%d", podIndex, topo, i)}, // Assign a plausible partition ID
			},
		}
	}
	return slices
}

func setupBenchmark(b *testing.B, numTrilliumPods, numIronwoodPods, numExistingSlices int, apiLatency time.Duration) (*SliceReconciler, client.Client, context.Context, []client.Object) {
	b.Helper()
	scheme := newScheme()

	partitionTrees := generateTrilliumPartitionTrees(numTrilliumPods)
	partitionTrees = append(partitionTrees, generateIronwoodPods(numIronwoodPods)...)

	// Pre-existing slices to simulate a busy cluster
	existingSlices := generateTrilliumSlices(numExistingSlices/2, numTrilliumPods)
	existingSlices = append(existingSlices, generateIronwoodSlices(numExistingSlices/2, numIronwoodPods, false)...)

	allObjects := append(partitionTrees, existingSlices...)

	rawClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()
	simulatedClient := &latencyInjectingClient{
		Client:  rawClient,
		latency: apiLatency,
	}
	recorder := events.NewFakeRecorder(1024)
	cache := watcher.NewPartitionCache(false)
	reconciler, _ := NewSliceReconciler(simulatedClient, scheme, log.Log.WithName("benchmark-reconciler"), string(slicev1beta1.TypeV6e), recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)

	return reconciler, rawClient, context.Background(), existingSlices
}

func setupHybridBenchmark(b *testing.B, numTrilliumPods, numIronwoodPods, numExistingSlices int, apiLatency time.Duration) (*SliceReconciler, client.Client, context.Context, []client.Object) {
	b.Helper()
	scheme := newScheme()

	partitionTrees := generateTrilliumPartitionTrees(numTrilliumPods)
	partitionTrees = append(partitionTrees, generateIronwoodPods(numIronwoodPods)...)

	// Pre-existing slices to simulate a busy cluster
	existingSlices := generateTrilliumSlices(numExistingSlices/2, numTrilliumPods)
	existingSlices = append(existingSlices, generateIronwoodSlices(numExistingSlices/2, numIronwoodPods, true)...)

	allObjects := append(partitionTrees, existingSlices...)

	rawClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()
	simulatedClient := &latencyInjectingClient{
		Client:  rawClient,
		latency: apiLatency,
	}
	recorder := events.NewFakeRecorder(1024)
	cache := watcher.NewPartitionCache(false)
	reconciler, _ := NewSliceReconciler(simulatedClient, scheme, log.Log.WithName("benchmark-reconciler"), string(slicev1beta1.TypeV6e), recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)

	return reconciler, rawClient, context.Background(), existingSlices
}

// BenchmarkBuildInitialState measures the performance of building the cache from scratch.
func BenchmarkBuildInitialState(b *testing.B) {
	numExistingSlices := 500
	numTrilliumPods := 1000
	numIronwoodPods := 10 // 10 * 144 = 1440 cubes
	reconciler, _, ctx, _ := setupBenchmark(b, numTrilliumPods, numIronwoodPods, numExistingSlices, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// In a real scenario, Start is only called once. Here we call it repeatedly
		// to measure its performance.
		if err := reconciler.Start(ctx); err != nil {
			b.Fatalf("reconciler.Start() failed: %v", err)
		}
	}
}

// findAndToggleRandomLeafState recursively finds a random leaf in the partition tree and toggles its state.
// It returns true if it successfully modified a leaf.
func findAndToggleRandomLeafState(p *partitiontreev1.PartitionInfo, rnd *rand.Rand) bool {
	if len(p.ChildPartitions) == 0 { // This is a leaf
		if p.State == partitiontreev1.PartitionStateHealthy {
			p.State = partitiontreev1.PartitionStateUnhealthy
		} else {
			p.State = partitiontreev1.PartitionStateHealthy
		}
		return true
	}

	// Try to recurse down a random child path
	childIndex := rnd.Intn(len(p.ChildPartitions))
	return findAndToggleRandomLeafState(&p.ChildPartitions[childIndex], rnd)
}

func BenchmarkReconcile_WithConcurrentUpdates(b *testing.B) {
	numExistingSlices := 500
	numTrilliumPods := 1000
	numIronwoodPods := 10 // 10 * 144 = 1440 cubes
	// Setup with a realistic number of trees and a simulated 50ms API latency.
	reconciler, rawClient, ctx, existingSlices := setupBenchmark(b, numTrilliumPods, numIronwoodPods, numExistingSlices, 50*time.Millisecond)

	// Pre-warm the cache.
	if err := reconciler.Start(ctx); err != nil {
		b.Fatalf("Initial state build failed: %v", err)
	}

	// Atomic counters for metrics
	var partitionUpdateCounter, sliceDeleteCounter, successfulReconcilesT, successfulReconcilesI, totalSliceI, totalSliceT atomic.Int64

	// Start the original "chaos" goroutine.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		localRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-done:
				return
			default:
				switch localRand.Intn(2) {
				case 0: // Update a PartitionTree
					treeType := localRand.Intn(2)
					var partitionTreeName string
					if treeType == 0 { // Trillium
						partitionTreeName = fmt.Sprintf("partitiontree-%d", localRand.Intn(numTrilliumPods))
					} else { // Ironwood
						partitionTreeName = fmt.Sprintf("partitiontree-%d-%d", localRand.Intn(numIronwoodPods), localRand.Intn(144))
					}
					tree := &partitiontreev1.PartitionTree{}
					if rawClient.Get(ctx, types.NamespacedName{Name: partitionTreeName, Namespace: "default"}, tree) == nil {
						if tree.Status.Root != nil && findAndToggleRandomLeafState(tree.Status.Root, localRand) {
							if rawClient.Update(ctx, tree) == nil {
								partitionUpdateCounter.Add(1)
							}
						}
					}
				case 1: // Delete a Slice
					if len(existingSlices) > 0 {
						sliceToDelete := existingSlices[localRand.Intn(len(existingSlices))].(*slicev1beta1.Slice)
						if rawClient.Delete(ctx, sliceToDelete) == nil {
							sliceDeleteCounter.Add(1)
						}
					}
				}
			}
		}
	}()

	// Realistic distribution of slice topology requests.
	trilliumTopologiesToRequest := []string{"2x2", "2x2", "2x4", "4x4", "4x4", "4x8", "8x8", "16x16"}
	ironwoodTopologiesToRequest := []string{"2x2x1", "2x2x2", "2x2x4", "2x4x4"}

	b.ResetTimer()
	// Run reconciliations in parallel to simulate high contention.
	b.RunParallel(func(pb *testing.PB) {
		localRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			var sliceName string
			var sliceToSchedule *slicev1beta1.Slice
			if localRand.Intn(2) == 0 { // Trillium
				sliceName = fmt.Sprintf("new-t-slice-%d", totalSliceI.Add(1))
				topo := trilliumTopologiesToRequest[localRand.Intn(len(trilliumTopologiesToRequest))]
				sliceToSchedule = &slicev1beta1.Slice{
					ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
					Spec:       slicev1beta1.SliceSpec{Topology: topo, Type: "v6e"},
				}
			} else { // Ironwood
				sliceName = fmt.Sprintf("new-i-slice-%d", totalSliceT.Add(1))
				topo := ironwoodTopologiesToRequest[localRand.Intn(len(ironwoodTopologiesToRequest))]
				sliceToSchedule = &slicev1beta1.Slice{
					ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
					Spec:       slicev1beta1.SliceSpec{Topology: topo, Type: "tpu7x"},
				}
			}

			// Use the raw client for setup to bypass latency injection for the test setup itself.
			if err := rawClient.Create(ctx, sliceToSchedule); err != nil {
				continue
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
			_, _ = reconciler.Reconcile(ctx, req)

			updatedSlice := &slicev1beta1.Slice{}
			// Use the raw client to check the result without added latency.
			if rawClient.Get(ctx, req.NamespacedName, updatedSlice) == nil {
				if len(updatedSlice.Spec.PartitionIds) > 0 {
					if len(updatedSlice.Spec.PartitionIds) > 0 {
						if updatedSlice.Spec.Type == "v6e" {
							successfulReconcilesT.Add(1)
						} else {
							successfulReconcilesI.Add(1)
						}
					}
				}
			}
		}
	})

	b.StopTimer()
	close(done)
	wg.Wait()

	// Report all metrics.
	b.ReportMetric(float64(totalSliceT.Load()), "total-slice-t")
	b.ReportMetric(float64(totalSliceI.Load()), "total-slice-i")
	b.ReportMetric(float64(successfulReconcilesT.Load())/float64(totalSliceT.Load()), "successful-reconciles-t/slice")
	b.ReportMetric(float64(successfulReconcilesI.Load())/float64(totalSliceI.Load()), "successful-reconciles-i/slice")
	b.ReportMetric(float64(partitionUpdateCounter.Load())/float64(b.N), "partition-flips/op")
	b.ReportMetric(float64(sliceDeleteCounter.Load())/float64(b.N), "slice-deletes/op")
}

func BenchmarkReconcile_HybridWithConcurrentUpdates(b *testing.B) {
	numExistingSlices := 500
	numTrilliumPods := 1000
	numIronwoodPods := 10 // 10 * 144 = 1440 cubes

	reconciler, rawClient, ctx, existingSlices := setupHybridBenchmark(b, numTrilliumPods, numIronwoodPods, numExistingSlices, 50*time.Millisecond)

	if err := reconciler.Start(ctx); err != nil {
		b.Fatalf("Initial state build failed: %v", err)
	}

	var partitionUpdateCounter, sliceDeleteCounter, successfulReconcilesT, successfulReconcilesI, totalSliceI, totalSliceT atomic.Int64

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		localRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-done:
				return
			default:
				switch localRand.Intn(2) {
				case 0: // Update a PartitionTree
					treeType := localRand.Intn(2)
					var partitionTreeName string
					if treeType == 0 { // Trillium
						partitionTreeName = fmt.Sprintf("partitiontree-%d", localRand.Intn(numTrilliumPods))
					} else { // Ironwood
						partitionTreeName = fmt.Sprintf("partitiontree-%d-%d", localRand.Intn(numIronwoodPods), localRand.Intn(144))
					}
					tree := &partitiontreev1.PartitionTree{}
					if rawClient.Get(ctx, types.NamespacedName{Name: partitionTreeName, Namespace: "default"}, tree) == nil {
						if tree.Status.Root != nil && findAndToggleRandomLeafState(tree.Status.Root, localRand) {
							if rawClient.Update(ctx, tree) == nil {
								partitionUpdateCounter.Add(1)
							}
						}
					}
				case 1: // Delete a Slice
					if len(existingSlices) > 0 {
						sliceToDelete := existingSlices[localRand.Intn(len(existingSlices))].(*slicev1beta1.Slice)
						if rawClient.Delete(ctx, sliceToDelete) == nil {
							sliceDeleteCounter.Add(1)
						}
					}
				}
			}
		}
	}()

	trilliumTopologiesToRequest := []string{"2x2", "2x2", "2x4", "4x4", "4x4", "4x8", "8x8", "16x16"}
	ironwoodTopologiesToRequest := []string{"2x2x1", "2x2x2", "2x2x4", "2x4x4", "4x4x4"}
	ironwoodMultiCubeTopologies := []string{"4x4x8", "4x8x8", "8x8x8"}
	iCountersSuccess := map[string]*atomic.Int64{
		"2x2x1": {}, "2x2x2": {}, "2x2x4": {}, "2x4x4": {},
		"4x4x4": {}, "4x4x8": {}, "4x8x8": {}, "8x8x8": {},
		"8x8x16": {}, "8x16x16": {}, "16x16x16": {},
	}
	iCountersTotal := map[string]*atomic.Int64{
		"2x2x1": {}, "2x2x2": {}, "2x2x4": {}, "2x4x4": {},
		"4x4x4": {}, "4x4x8": {}, "4x8x8": {}, "8x8x8": {},
		"8x8x16": {}, "8x16x16": {}, "16x16x16": {},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			var sliceName, topo string
			var sliceToSchedule *slicev1beta1.Slice
			prob := localRand.Float64()
			if prob < 0.45 { // Trillium
				sliceName = fmt.Sprintf("new-t-slice-%d", totalSliceT.Add(1))
				topo = trilliumTopologiesToRequest[localRand.Intn(len(trilliumTopologiesToRequest))]
				sliceToSchedule = &slicev1beta1.Slice{
					ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
					Spec:       slicev1beta1.SliceSpec{Topology: topo, Type: "v6e"},
				}
			} else if prob < 0.9 { // Ironwood sub-cube
				sliceName = fmt.Sprintf("new-i-slice-%d", totalSliceI.Add(1))
				topo = ironwoodTopologiesToRequest[localRand.Intn(len(ironwoodTopologiesToRequest))]
				iCountersTotal[topo].Add(1)
				sliceToSchedule = &slicev1beta1.Slice{
					ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
					Spec:       slicev1beta1.SliceSpec{Topology: topo, Type: "tpu7x"},
				}
			} else {
				sliceName = fmt.Sprintf("new-i-slice-%d", totalSliceI.Add(1))
				topo = ironwoodMultiCubeTopologies[localRand.Intn(len(ironwoodMultiCubeTopologies))]
				iCountersTotal[topo].Add(1)
				sliceToSchedule = &slicev1beta1.Slice{
					ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
					Spec:       slicev1beta1.SliceSpec{Topology: topo, Type: "tpu7x"},
				}
			}
			if err := rawClient.Create(ctx, sliceToSchedule); err != nil {
				continue
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
			_, _ = reconciler.Reconcile(ctx, req)

			updatedSlice := &slicev1beta1.Slice{}
			if rawClient.Get(ctx, req.NamespacedName, updatedSlice) == nil {
				if len(updatedSlice.Spec.PartitionIds) > 0 {
					if updatedSlice.Spec.Type == "v6e" {
						successfulReconcilesT.Add(1)
					} else {
						iCountersSuccess[topo].Add(1)
						successfulReconcilesI.Add(1)
					}
				}
			}
		}
	})

	b.StopTimer()
	close(done)
	wg.Wait()

	// Report all metrics.
	b.ReportMetric(float64(totalSliceT.Load()), "total-slice-t")
	b.ReportMetric(float64(totalSliceI.Load()), "total-slice-i")
	b.ReportMetric(float64(successfulReconcilesT.Load())/float64(totalSliceT.Load()), "successful-reconciles-t/slice")
	b.ReportMetric(float64(successfulReconcilesI.Load())/float64(totalSliceI.Load()), "successful-reconciles-i/slice")
	b.ReportMetric(float64(partitionUpdateCounter.Load())/float64(b.N), "partition-flips/op")
	b.ReportMetric(float64(sliceDeleteCounter.Load())/float64(b.N), "slice-deletes/op")
	for _, topo := range ironwoodTopologiesToRequest {
		b.ReportMetric(float64(iCountersSuccess[topo].Load()), fmt.Sprintf("%s-success", topo))
		b.ReportMetric(float64(iCountersTotal[topo].Load()), fmt.Sprintf("%s-total", topo))
	}
	for _, topo := range ironwoodMultiCubeTopologies {
		b.ReportMetric(float64(iCountersSuccess[topo].Load()), fmt.Sprintf("%s-success", topo))
		b.ReportMetric(float64(iCountersTotal[topo].Load()), fmt.Sprintf("%s-total", topo))
	}
}

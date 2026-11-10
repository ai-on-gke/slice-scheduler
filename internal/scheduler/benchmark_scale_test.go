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
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/generators"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/schedulerlib"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

// createTestSchedulerLib constructs a real DefaultScheduler with all standard plugins.
// This matches the production configuration in cmd/main.go.
func createTestSchedulerLib() interfaces.SchedulerLibrary {
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 10}),
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
	return schedulerlib.NewDefaultScheduler(gens, filters, scorers)
}

// setupScaleBenchmark creates a reconciler with a properly initialized PartitionCache
// AND a real schedulerLib. Unlike setupBenchmark (which passes nil for both),
// this setup supports calling Start() and produces real scheduling results.
func setupScaleBenchmark(b testing.TB, numTrilliumPods, numIronwoodPods, numExistingSlices int, tpuType string) (*SliceReconciler, client.Client, context.Context) {
	b.Helper()
	scheme := newScheme()

	var allObjects []client.Object
	allObjects = append(allObjects, generateTrilliumPartitionTrees(numTrilliumPods)...)
	allObjects = append(allObjects, generateIronwoodPods(numIronwoodPods)...)

	if numExistingSlices > 0 && numTrilliumPods > 0 {
		allObjects = append(allObjects, generateTrilliumSlices(numExistingSlices, numTrilliumPods)...)
	}
	if numExistingSlices > 0 && numIronwoodPods > 0 {
		allObjects = append(allObjects, generateIronwoodSlices(numExistingSlices, numIronwoodPods, false)...)
	}

	rawClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()
	recorder := events.NewFakeRecorder(1024)
	cache := watcher.NewPartitionCache(false)
	// Pass a real schedulerLib instead of nil
	sLib := createTestSchedulerLib()
	reconciler, err := NewSliceReconciler(rawClient, scheme, log.Log.WithName("scale-bench"), tpuType, recorder, false, sLib, cache, 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		b.Fatalf("NewSliceReconciler failed: %v", err)
	}

	return reconciler, rawClient, context.Background()
}

// =============================================================================
// BenchmarkSchedule_100PTs: 100 partition trees, single-cube scheduling
// =============================================================================

func BenchmarkSchedule_100PTs_Scale(b *testing.B) {
	reconciler, rawClient, ctx := setupScaleBenchmark(b, 100, 0, 50, string(slicev1beta1.TypeV6e))
	if err := reconciler.Start(ctx); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	var successCount atomic.Int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sliceName := fmt.Sprintf("scale100-slice-%d", i)
		slice := &slicev1beta1.Slice{
			ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
			Spec:       slicev1beta1.SliceSpec{Topology: "4x4", Type: "v6e"},
		}
		if err := rawClient.Create(ctx, slice); err != nil {
			continue
		}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
		_, _ = reconciler.Reconcile(ctx, req)

		updated := &slicev1beta1.Slice{}
		if rawClient.Get(ctx, req.NamespacedName, updated) == nil {
			if len(updated.Spec.PartitionIds) > 0 {
				successCount.Add(1)
			}
		}
	}
	b.ReportMetric(float64(successCount.Load())/float64(b.N), "success-rate")
}

// =============================================================================
// BenchmarkSchedule_500PTs: 500 partition trees, scheduling under load
// =============================================================================

func BenchmarkSchedule_500PTs_Scale(b *testing.B) {
	reconciler, rawClient, ctx := setupScaleBenchmark(b, 500, 0, 200, string(slicev1beta1.TypeV6e))
	if err := reconciler.Start(ctx); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	var successCount atomic.Int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sliceName := fmt.Sprintf("scale500-slice-%d", i)
		slice := &slicev1beta1.Slice{
			ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
			Spec:       slicev1beta1.SliceSpec{Topology: "4x4", Type: "v6e"},
		}
		if err := rawClient.Create(ctx, slice); err != nil {
			continue
		}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
		_, _ = reconciler.Reconcile(ctx, req)

		updated := &slicev1beta1.Slice{}
		if rawClient.Get(ctx, req.NamespacedName, updated) == nil {
			if len(updated.Spec.PartitionIds) > 0 {
				successCount.Add(1)
			}
		}
	}
	b.ReportMetric(float64(successCount.Load())/float64(b.N), "success-rate")
}

// =============================================================================
// BenchmarkPreemption_50Victims: 50 occupied cubes, preemption scheduling
// =============================================================================

func BenchmarkPreemption_50Victims_Scale(b *testing.B) {
	reconciler, rawClient, ctx := setupScaleBenchmark(b, 50, 0, 50, string(slicev1beta1.TypeV6e))
	if err := reconciler.Start(ctx); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sliceName := fmt.Sprintf("preempt50-slice-%d", i)
		slice := &slicev1beta1.Slice{
			ObjectMeta: ctrl.ObjectMeta{
				Name:      sliceName,
				Namespace: "default",
				Annotations: map[string]string{
					SlicePriorityAnnotation: "999",
				},
			},
			Spec: slicev1beta1.SliceSpec{Topology: "4x4", Type: "v6e"},
		}
		if err := rawClient.Create(ctx, slice); err != nil {
			continue
		}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
		_, _ = reconciler.Reconcile(ctx, req)
	}
}

// =============================================================================
// BenchmarkMultiCubeSolve_16Cubes_Scale: Ironwood 16-cube pod, multi-cube scheduling
// =============================================================================

func BenchmarkMultiCubeSolve_16Cubes_Scale(b *testing.B) {
	reconciler, rawClient, ctx := setupScaleBenchmark(b, 0, 1, 0, string(slicev1beta1.TypeTpu7x))
	if err := reconciler.Start(ctx); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	var successCount atomic.Int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sliceName := fmt.Sprintf("mc16-slice-%d", i)
		slice := &slicev1beta1.Slice{
			ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
			Spec:       slicev1beta1.SliceSpec{Topology: "4x4x8", Type: "tpu7x"}, // 2-cube
		}
		if err := rawClient.Create(ctx, slice); err != nil {
			continue
		}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
		_, _ = reconciler.Reconcile(ctx, req)

		updated := &slicev1beta1.Slice{}
		if rawClient.Get(ctx, req.NamespacedName, updated) == nil {
			if len(updated.Spec.PartitionIds) > 0 {
				successCount.Add(1)
			}
		}
	}
	b.ReportMetric(float64(successCount.Load())/float64(b.N), "success-rate")
}

// =============================================================================
// BenchmarkMultiCubeSolve_50Cubes_Scale: Ironwood pod, multi-cube scheduling
// =============================================================================

func BenchmarkMultiCubeSolve_50Cubes_Scale(b *testing.B) {
	reconciler, rawClient, ctx := setupScaleBenchmark(b, 0, 1, 0, string(slicev1beta1.TypeTpu7x))
	if err := reconciler.Start(ctx); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	multiCubeTopologies := []string{"4x4x8", "4x4x12", "4x4x16"}
	var successCount atomic.Int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topo := multiCubeTopologies[i%len(multiCubeTopologies)]
		sliceName := fmt.Sprintf("mc50-slice-%d", i)
		slice := &slicev1beta1.Slice{
			ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
			Spec:       slicev1beta1.SliceSpec{Topology: topo, Type: "tpu7x"},
		}
		if err := rawClient.Create(ctx, slice); err != nil {
			continue
		}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
		_, _ = reconciler.Reconcile(ctx, req)

		updated := &slicev1beta1.Slice{}
		if rawClient.Get(ctx, req.NamespacedName, updated) == nil {
			if len(updated.Spec.PartitionIds) > 0 {
				successCount.Add(1)
			}
		}
	}
	b.ReportMetric(float64(successCount.Load())/float64(b.N), "success-rate")
}

// =============================================================================
// BenchmarkCacheSync_500PTs_Scale: Cache building with 500 partition trees
// =============================================================================

func BenchmarkCacheSync_500PTs_Scale(b *testing.B) {
	scheme := newScheme()

	var allObjects []client.Object
	allObjects = append(allObjects, generateTrilliumPartitionTrees(500)...)
	allObjects = append(allObjects, generateTrilliumSlices(200, 500)...)

	rawClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recorder := events.NewFakeRecorder(1024)
		cache := watcher.NewPartitionCache(false)
		reconciler, err := NewSliceReconciler(rawClient, scheme, log.Log.WithName("cache-bench"), string(slicev1beta1.TypeV6e), recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)
		if err != nil {
			b.Fatalf("NewSliceReconciler failed: %v", err)
		}
		if err := reconciler.Start(context.Background()); err != nil {
			b.Fatalf("Start failed: %v", err)
		}
	}
}

// =============================================================================
// BenchmarkMixedTopologyScheduling_Scale: Mixed topology under concurrent load
// =============================================================================

func BenchmarkMixedTopologyScheduling_Scale(b *testing.B) {
	reconciler, rawClient, ctx := setupScaleBenchmark(b, 100, 3, 0, string(slicev1beta1.TypeV6e))
	if err := reconciler.Start(ctx); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	topologies := []struct {
		topo    string
		tpuType slicev1beta1.Type
	}{
		{"4x4", "v6e"},
		{"8x8", "v6e"},
		{"2x2x1", "tpu7x"},
		{"2x2x4", "tpu7x"},
		{"4x4x4", "tpu7x"},
		{"4x4x8", "tpu7x"},
	}

	var successCount, totalCount atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := int(totalCount.Add(1))
			tt := topologies[idx%len(topologies)]
			sliceName := fmt.Sprintf("mixed-scale-%d", idx)
			slice := &slicev1beta1.Slice{
				ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
				Spec:       slicev1beta1.SliceSpec{Topology: tt.topo, Type: tt.tpuType},
			}
			if err := rawClient.Create(ctx, slice); err != nil {
				continue
			}
			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
			_, _ = reconciler.Reconcile(ctx, req)

			updated := &slicev1beta1.Slice{}
			if rawClient.Get(ctx, req.NamespacedName, updated) == nil {
				if len(updated.Spec.PartitionIds) > 0 {
					successCount.Add(1)
				}
			}
		}
	})
	b.ReportMetric(float64(successCount.Load())/max(float64(totalCount.Load()), 1), "success-rate")
	b.ReportMetric(float64(totalCount.Load()), "total-scheduled")
}

// =============================================================================
// BenchmarkBuildInitialState_500PTs_Scale: cache build for 500 PTs
// =============================================================================

func BenchmarkBuildInitialState_500PTs_Scale(b *testing.B) {
	scheme := newScheme()

	var allObjects []client.Object
	allObjects = append(allObjects, generateTrilliumPartitionTrees(250)...)
	allObjects = append(allObjects, generateIronwoodPods(2)...)
	allObjects = append(allObjects, generateTrilliumSlices(150, 250)...)
	allObjects = append(allObjects, generateIronwoodSlices(150, 2, false)...)

	rawClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recorder := events.NewFakeRecorder(1024)
		cache := watcher.NewPartitionCache(false)
		reconciler, err := NewSliceReconciler(rawClient, scheme, log.Log.WithName("build-bench"), string(slicev1beta1.TypeV6e), recorder, false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)
		if err != nil {
			b.Fatalf("NewSliceReconciler failed: %v", err)
		}
		if err := reconciler.Start(context.Background()); err != nil {
			b.Fatalf("Start failed: %v", err)
		}
	}
}

// =============================================================================
// TestScale_SchedulingLatencyTargets: Verify latency SLAs with the reconciler
// =============================================================================

func TestScale_SchedulingLatencyTargets(t *testing.T) {
	tests := []struct {
		name          string
		trilliumPods  int
		ironwoodPods  int
		existSlices   int
		topology      string
		tpuType       string
		maxP99Latency time.Duration
	}{
		{
			name:          "SingleCube 500 PTs",
			trilliumPods:  500,
			ironwoodPods:  0,
			existSlices:   200,
			topology:      "4x4",
			tpuType:       "v6e",
			maxP99Latency: 5 * time.Second,
		},
		{
			name:          "Ironwood SubSlice 500 PTs",
			trilliumPods:  0,
			ironwoodPods:  4, // 4 * 144 = 576 trees
			existSlices:   100,
			topology:      "2x2x1",
			tpuType:       "tpu7x",
			maxP99Latency: 5 * time.Second,
		},
		{
			name:          "MultiCube 4x4x8 at 500 PTs",
			trilliumPods:  0,
			ironwoodPods:  4, // 576 trees
			existSlices:   50,
			topology:      "4x4x8",
			tpuType:       "tpu7x",
			maxP99Latency: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler, rawClient, ctx := setupScaleBenchmark(t, tt.trilliumPods, tt.ironwoodPods, tt.existSlices, tt.tpuType)
			if err := reconciler.Start(ctx); err != nil {
				t.Fatalf("Start failed: %v", err)
			}

			numRequests := 20
			latencies := make([]time.Duration, numRequests)

			for i := 0; i < numRequests; i++ {
				sliceName := fmt.Sprintf("latency-test-%d", i)
				slice := &slicev1beta1.Slice{
					ObjectMeta: ctrl.ObjectMeta{Name: sliceName, Namespace: "default"},
					Spec:       slicev1beta1.SliceSpec{Topology: tt.topology, Type: slicev1beta1.Type(tt.tpuType)},
				}
				if err := rawClient.Create(ctx, slice); err != nil {
					t.Fatalf("Create failed: %v", err)
				}

				req := ctrl.Request{NamespacedName: types.NamespacedName{Name: sliceName, Namespace: "default"}}
				start := time.Now()
				_, _ = reconciler.Reconcile(ctx, req)
				latencies[i] = time.Since(start)
			}

			sortDurations(latencies)
			p50 := latencies[len(latencies)/2]
			p99Idx := int(float64(len(latencies)) * 0.99)
			if p99Idx >= len(latencies) {
				p99Idx = len(latencies) - 1
			}
			p99 := latencies[p99Idx]

			t.Logf("Latency p50: %v, p99: %v (limit: %v)", p50, p99, tt.maxP99Latency)

			if p99 > tt.maxP99Latency {
				t.Errorf("P99 latency %v exceeded target %v", p99, tt.maxP99Latency)
			}
		})
	}
}

// sortDurations sorts a slice of time.Duration in ascending order.
func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}

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

package schedulerlib

import (
	"context"
	"testing"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/generators"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins/taint"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// cubeTree is a healthy 4x4x4 cube split into two 2x4x4 leaves; leafTaints lands on the
// first leaf only, the way the partition-tree controller records a node taint.
func cubeTree(pod string, leafTaints ...partitiontreev1.PartitionTaint) *partitiontreev1.PartitionTree {
	return &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: pod, Labels: map[string]string{watcher.TpuPodIDLabel: pod}},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: pod + "-cube",
				Topology:    "4x4x4",
				State:       partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{PartitionID: pod + "-half-0", Topology: "2x4x4", State: partitiontreev1.PartitionStateHealthy, Taints: leafTaints},
					{PartitionID: pod + "-half-1", Topology: "2x4x4", State: partitiontreev1.PartitionStateHealthy},
				},
			},
		},
	}
}

// TestTaintFilter_SkipsCubeWithTaintedLeaf drives the real cache, watcher and scheduler with
// the taint on a leaf, where the partition-tree controller puts node taints. The cube is
// rejected both because the cache unions the leaf's taint onto it and because the generator
// requires every descendant of a candidate to pass the filters.
func TestTaintFilter_SkipsCubeWithTaintedLeaf(t *testing.T) {
	unreachable := partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnreachable, Effect: partitiontreev1.TaintEffectNoExecute}
	req := interfaces.GroupAllocationRequest{
		GroupName: "test-group",
		Slices:    []interfaces.SliceRequirement{{SliceName: "slice1", Topology: "4x4x4", Type: TPU7x}},
		Priority:  100,
	}
	s := NewDefaultScheduler(
		[]interfaces.PlacementGenerator{generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 10})},
		[]interfaces.FilterPlugin{&taint.TaintFilter{}, &plugins.HealthyFilter{}, &plugins.ManagedFilter{}, &plugins.PriorityFilter{}},
		[]interfaces.ScorePlugin{&plugins.PreemptionScorer{}, &plugins.TightestFitScorer{}},
	)

	t.Run("picks the untainted cube", func(t *testing.T) {
		cache := watcher.NewPartitionCache(false, watcher.WithTaintFiltering(true))
		cache.UpdateTree(context.Background(), cubeTree("pod-a", unreachable), nil)
		cache.UpdateTree(context.Background(), cubeTree("pod-b"), nil)

		result := s.Schedule(context.Background(), req, watcher.NewPartitionWatcher(cache).GetState())
		if !result.IsSchedulable {
			t.Fatalf("expected a placement, got: %s", result.FailureReason)
		}
		if got := result.Candidate.Assignments["slice1"][0].ID; got != "pod-b-cube" {
			t.Errorf("placed on %s, want pod-b-cube", got)
		}
	})

	t.Run("unschedulable when every cube has a tainted leaf", func(t *testing.T) {
		cache := watcher.NewPartitionCache(false, watcher.WithTaintFiltering(true))
		cache.UpdateTree(context.Background(), cubeTree("pod-a", unreachable), nil)
		cache.UpdateTree(context.Background(), cubeTree("pod-b", unreachable), nil)

		if result := s.Schedule(context.Background(), req, watcher.NewPartitionWatcher(cache).GetState()); result.IsSchedulable {
			t.Errorf("placed on %s, want no placement", result.Candidate.Assignments["slice1"][0].ID)
		}
	})

	t.Run("taints ignored when filtering is disabled", func(t *testing.T) {
		cache := watcher.NewPartitionCache(false)
		cache.UpdateTree(context.Background(), cubeTree("pod-a", unreachable), nil)
		cache.UpdateTree(context.Background(), cubeTree("pod-b", unreachable), nil)

		if result := s.Schedule(context.Background(), req, watcher.NewPartitionWatcher(cache).GetState()); !result.IsSchedulable {
			t.Errorf("expected a placement with taint filtering disabled, got: %s", result.FailureReason)
		}
	})
}

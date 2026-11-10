/*
Copyright 2026.

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
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/metrics"
	"github.com/ai-on-gke/slice-scheduler/internal/pq"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

func TestUpdateQueueDepthMetric_EmptiedTopologyReadsZero(t *testing.T) {
	metrics.SliceQueueDepth.Reset()
	t.Cleanup(metrics.SliceQueueDepth.Reset)

	r := &SliceReconciler{tpuType: TPU7x, schedulingQueue: pq.NewPriorityQueue()}
	heap.Push(r.schedulingQueue, &pq.Item{GroupId: "a", Topology: "2x2x1"})
	heap.Push(r.schedulingQueue, &pq.Item{GroupId: "b", Topology: "2x2x1"})
	heap.Push(r.schedulingQueue, &pq.Item{GroupId: "c", Topology: "4x4x4"})
	r.updateQueueDepthMetric()

	depth := func(topology string) float64 {
		return testutil.ToFloat64(metrics.SliceQueueDepth.WithLabelValues(TPU7x, topology))
	}
	if got := depth("2x2x1"); got != 2 {
		t.Errorf("2x2x1 depth = %v, want 2", got)
	}
	if got := depth("4x4x4"); got != 1 {
		t.Errorf("4x4x4 depth = %v, want 1", got)
	}

	// Pop both 2x2x1 groups, keeping 4x4x4 queued.
	var kept []*pq.Item
	for r.schedulingQueue.Len() > 0 {
		if item := heap.Pop(r.schedulingQueue).(*pq.Item); item.Topology != "2x2x1" {
			kept = append(kept, item)
		}
	}
	for _, item := range kept {
		heap.Push(r.schedulingQueue, item)
	}
	r.updateQueueDepthMetric()

	if n := testutil.CollectAndCount(metrics.SliceQueueDepth); n != 2 {
		t.Errorf("SliceQueueDepth has %d series, want 2", n)
	}
	if got := depth("2x2x1"); got != 0 {
		t.Errorf("emptied 2x2x1 depth = %v, want 0", got)
	}
	if got := depth("4x4x4"); got != 1 {
		t.Errorf("4x4x4 depth = %v, want 1", got)
	}
}

func TestFinalizeSchedulingSuccess_PreemptedSlicesUseVictimPod(t *testing.T) {
	metrics.PreemptedSlicesTotal.Reset()
	t.Cleanup(metrics.PreemptedSlicesTotal.Reset)

	cache := watcher.NewPartitionCache(false)
	for _, pod := range []string{"pod-a", "pod-b"} {
		cache.UpdateTree(context.Background(), &partitiontreev1.PartitionTree{
			ObjectMeta: metav1.ObjectMeta{Name: pod + "-tree", Labels: map[string]string{watcher.TpuPodIDLabel: pod}},
			Status: partitiontreev1.PartitionTreeStatus{
				Root: &partitiontreev1.PartitionInfo{
					PartitionID: pod + "-cube",
					Topology:    "4x4x4",
					State:       partitiontreev1.PartitionStateHealthy,
				},
			},
		}, nil)
	}

	r := &SliceReconciler{tpuType: TPU7x, cache: cache}
	r.finalizeSchedulingSuccess(logr.Discard(), &BindingTask{
		groupId: "preemptor",
		candidate: &interfaces.PlacementCandidate{
			Assignments: map[string][]*interfaces.PartitionInfo{
				"preemptor-0": {{ID: "pod-a-cube", PodName: "pod-a"}},
			},
		},
		victims: []*interfaces.SliceInfo{
			{Name: "victim-0", Topology: "4x4x4", PartitionIds: []string{"pod-b-cube"}},
		},
		firstTopology: "4x4x4",
		startTime:     time.Now(),
	})

	if got := testutil.ToFloat64(metrics.PreemptedSlicesTotal.WithLabelValues(TPU7x, "4x4x4", "pod-b")); got != 1 {
		t.Errorf("preempted_slices_total for the victim's pod = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(metrics.PreemptedSlicesTotal); n != 1 {
		t.Errorf("preempted_slices_total has %d series, want 1", n)
	}
}

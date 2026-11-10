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
	"context"
	"slices"
	"testing"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// withLeafReadiness returns a copy of tree whose leaves carry the NodeReady condition
// given by ready, keyed by partition ID. A leaf missing from ready has no condition.
func withLeafReadiness(tree *partitiontreev1.PartitionTree, ready map[string]metav1.ConditionStatus) *partitiontreev1.PartitionTree {
	tree = tree.DeepCopy()
	var walk func(info *partitiontreev1.PartitionInfo)
	walk = func(info *partitiontreev1.PartitionInfo) {
		if len(info.ChildPartitions) == 0 {
			if status, ok := ready[info.PartitionID]; ok {
				info.Conditions = []metav1.Condition{{Type: partitiontreev1.ConditionTypeNodeReady, Status: status}}
			}
			return
		}
		for i := range info.ChildPartitions {
			walk(&info.ChildPartitions[i])
		}
	}
	walk(tree.Status.Root)
	return tree
}

// TestTryScheduleSliceGroup_LeafReadinessFromTree checks that, with node readiness on, a
// tree that first appears with an empty status, whose nodes the scheduler never saw, is
// scheduled from its leaves' NodeReady conditions.
func TestTryScheduleSliceGroup_LeafReadinessFromTree(t *testing.T) {
	ctx := context.Background()
	allLeaves := []string{"cube-a-0", "cube-a-1", "cube-b-0", "cube-b-1"}
	readiness := func(status metav1.ConditionStatus, ids ...string) map[string]metav1.ConditionStatus {
		m := make(map[string]metav1.ConditionStatus, len(ids))
		for _, id := range ids {
			m[id] = status
		}
		return m
	}

	tests := []struct {
		name  string
		ready map[string]metav1.ConditionStatus
		want  []string // nil means the slice stays unscheduled
	}{
		{name: "all leaves ready", ready: readiness(metav1.ConditionTrue, allLeaves...), want: []string{"cube-a"}},
		{
			name: "unready leaf excludes its cube",
			ready: func() map[string]metav1.ConditionStatus {
				m := readiness(metav1.ConditionTrue, allLeaves...)
				m["cube-a-1"] = metav1.ConditionFalse
				return m
			}(),
			want: []string{"cube-b"},
		},
		{name: "no NodeReady conditions", ready: nil, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			populated := withLeafReadiness(noExecuteTree(), tt.ready)
			empty := populated.DeepCopy()
			empty.Status = partitiontreev1.PartitionTreeStatus{}
			slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName)

			fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).
				WithObjects(populated.DeepCopy(), slice.DeepCopy()).Build()
			cache := watcher.NewPartitionCache(true)
			r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, events.NewFakeRecorder(10),
				true, taintAwareScheduler(), cache, 0, 0, policy.NewCloudPolicy(), 0)
			if err != nil {
				t.Fatalf("NewSliceReconciler() error: %v", err)
			}

			// A node event before the tree exists finds no leaf to update.
			r.handleNodeCreate(ctx, event.TypedCreateEvent[client.Object]{Object: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
				Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
			}})
			// The tree is created with no status, which the create handler skips, and is
			// then filled in by an update that brings no k8s nodes.
			r.handlePartitionTreeCreate(ctx, event.TypedCreateEvent[client.Object]{Object: empty}, newTestQueue())
			r.handlePartitionTreeUpdate(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: empty, ObjectNew: populated}, newTestQueue())

			r.updateGroupCache(ctx, slice)
			r.tryScheduleSliceGroup(ctx, "owned-group")

			if got := getSlice(t, r, "owned").Spec.PartitionIds; !slices.Equal(got, tt.want) {
				t.Errorf("PartitionIds = %v, want %v", got, tt.want)
			}
		})
	}
}

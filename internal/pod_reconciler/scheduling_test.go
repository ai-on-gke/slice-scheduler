/*
Copyright 2025.

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

package pod_reconciler

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

// placedSlice is a slice this scheduler owns, placed on part-1 (node-1, node-2)
// and not being evicted. It has no topology, so the policy's node health check
// passes without Node objects.
func placedSlice(name string) *slicev1beta1.Slice {
	slice := drainSlice(name)
	delete(slice.Annotations, interfaces.EvictPodsAnnotation)
	slice.Spec.Topology = ""
	return slice
}

func gated(pod *corev1.Pod) *corev1.Pod {
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "leaderworkerset.sigs.k8s.io/exclusive-placement"}}
	return pod
}

func TestHandleScheduling_HonoursSchedulingGates(t *testing.T) {
	tests := []struct {
		name      string
		pods      []client.Object
		wantBound []string
	}{
		{
			name: "gated leader is skipped and the worker still binds",
			pods: []client.Object{
				gated(drainPod("lws-0", "s", "", corev1.PodPending)),
				drainPod("lws-1", "s", "", corev1.PodPending),
			},
			wantBound: []string{"lws-1"},
		},
		{
			name:      "only gated pods: nothing is bound",
			pods:      []client.Object{gated(drainPod("lws-0", "s", "", corev1.PodPending))},
			wantBound: nil,
		},
		{
			name:      "ungated leader binds",
			pods:      []client.Object{drainPod("lws-0", "s", "", corev1.PodPending)},
			wantBound: []string{"lws-0"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newDrainTestEnv(t, nil, append(tt.pods, placedSlice("s"))...)
			if _, err := env.reconcile(t, "s"); err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}
			if !slices.Equal(env.sub.bound, tt.wantBound) {
				t.Errorf("bound = %v, want %v", env.sub.bound, tt.wantBound)
			}
		})
	}
}

func TestHandleScheduling_DoesNotBindToADeletingSlice(t *testing.T) {
	slice := placedSlice("s")
	slice.Finalizers = []string{"test/keep"}
	slice.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	env := newDrainTestEnv(t, nil, slice, drainPod("w-0", "s", "", corev1.PodPending))

	if _, err := env.reconcile(t, "s"); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if len(env.sub.bound) != 0 {
		t.Errorf("bound = %v onto a deleting slice, want none", env.sub.bound)
	}
}

// TestHandleScheduling_OnePodFailureDoesNotStopThePass checks that a pod whose
// binding fails, or whose index can't be resolved, doesn't keep the slice's
// other pods from being bound, and that the failure is still reported.
func TestHandleScheduling_OnePodFailureDoesNotStopThePass(t *testing.T) {
	unindexed := drainPod("no-index", "s", "", corev1.PodPending)
	env := newDrainTestEnv(t, nil, placedSlice("s"),
		drainPod("w-0", "s", "", corev1.PodPending),
		drainPod("w-1", "s", "", corev1.PodPending),
		unindexed)
	env.sub.failFor = map[string]error{"w-0": fmt.Errorf("injected")}

	if _, err := env.reconcile(t, "s"); err == nil {
		t.Error("Reconcile() error = nil, want the failures reported")
	}
	if !slices.Equal(env.sub.bound, []string{"w-1"}) {
		t.Errorf("bound = %v, want [w-1]", env.sub.bound)
	}
}

// TestTrimPodKeepsWhatTheReconcilerReads checks the informer transform against
// every pod field the pod reconciler reads. A field TrimPod drops reads as
// empty in the binary while tests built from full pods still pass.
func TestTrimPodKeepsWhatTheReconcilerReads(t *testing.T) {
	now := metav1.Now()
	full := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "w-0",
			Namespace:         "default",
			Labels:            map[string]string{SliceNameLabel: "s"},
			Annotations:       map[string]string{JobCompletionIndexAnnotation: "0"},
			DeletionTimestamp: &now,
			OwnerReferences:   []metav1.OwnerReference{{Name: "owner"}},
		},
		Spec: corev1.PodSpec{
			SchedulerName:   interfaces.SliceSchedulerName,
			NodeName:        "node-1",
			SchedulingGates: []corev1.PodSchedulingGate{{Name: "gate"}},
			Containers: []corev1.Container{{
				Name: "c",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{"google.com/tpu": resource.MustParse("4")},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
	}

	out, err := TrimPod(full)
	if err != nil {
		t.Fatalf("TrimPod() error: %v", err)
	}
	got := out.(*corev1.Pod)
	want := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              full.Name,
			Namespace:         full.Namespace,
			Labels:            full.Labels,
			Annotations:       full.Annotations,
			DeletionTimestamp: full.DeletionTimestamp,
		},
		Spec: corev1.PodSpec{
			SchedulerName:   full.Spec.SchedulerName,
			NodeName:        full.Spec.NodeName,
			SchedulingGates: full.Spec.SchedulingGates,
		},
		Status: corev1.PodStatus{Phase: full.Status.Phase},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("TrimPod() =\n%+v\nwant\n%+v", got, want)
	}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	if out, err := TrimPod(node); err != nil || out != node {
		t.Errorf("TrimPod(node) = %v, %v; want the node unchanged", out, err)
	}
}

// successorSlice is a generation of slice "s". The workload's Slice is
// recreated under the same name, so only the UID tells generations apart.
func successorSlice(uid types.UID, created time.Time, partitionIds ...string) *slicev1beta1.Slice {
	slice := placedSlice("s")
	slice.UID = uid
	slice.CreationTimestamp = metav1.NewTime(created)
	slice.Spec.PartitionIds = partitionIds
	return slice
}

// stampedPod is a pod of slice "s" bound to nodeName for the Slice with UID boundFor.
func stampedPod(name, nodeName string, boundFor types.UID) *corev1.Pod {
	pod := drainPod(name, "s", nodeName, corev1.PodRunning)
	if boundFor != "" {
		pod.Annotations = map[string]string{SliceUIDAnnotation: string(boundFor)}
	}
	return pod
}

func TestHandleScheduling_StampsTheSliceUID(t *testing.T) {
	env := newDrainTestEnv(t, nil, successorSlice("uid-1", time.Now(), "part-1"),
		drainPod("w-0", "s", "", corev1.PodPending))
	if _, err := env.reconcile(t, "s"); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if env.sub.lastBinding == nil {
		t.Fatal("no binding created")
	}
	if got := env.sub.lastBinding.Annotations[SliceUIDAnnotation]; got != "uid-1" {
		t.Errorf("binding %s = %q, want uid-1", SliceUIDAnnotation, got)
	}
}

func TestHandleNoPartitions_SameNameSuccessor(t *testing.T) {
	tests := []struct {
		name         string
		created      time.Time
		boundFor     types.UID
		wantEviction bool
	}{
		{
			name:     "pod bound for the previous slice is spared while the successor is new",
			created:  time.Now(),
			boundFor: "uid-old",
		},
		{
			name:         "pod bound for the previous slice is evicted once the grace period is over",
			created:      time.Now().Add(-2 * DefaultSuccessorGracePeriod),
			boundFor:     "uid-old",
			wantEviction: true,
		},
		{
			name:         "pod bound for this slice is evicted at once",
			created:      time.Now(),
			boundFor:     "uid-new",
			wantEviction: true,
		},
		{
			name:         "pod bound before the UID was recorded is evicted at once",
			created:      time.Now(),
			wantEviction: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newDrainTestEnv(t, nil, successorSlice("uid-new", tt.created),
				stampedPod("w-0", "node-1", tt.boundFor))
			res, err := env.reconcile(t, "s")
			if err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}
			if got := env.sub.lastEviction != nil; got != tt.wantEviction {
				t.Errorf("eviction = %v, want %v", got, tt.wantEviction)
			}
			if !tt.wantEviction && (res.RequeueAfter <= 0 || res.RequeueAfter > DefaultSuccessorGracePeriod) {
				t.Errorf("RequeueAfter = %v, want a requeue within %v to revisit the spared pod",
					res.RequeueAfter, DefaultSuccessorGracePeriod)
			}
		})
	}
}

// TestReconcile_SameNameSuccessorPlaced follows a spared leader once the
// successor Slice is placed. On the predecessor's partition it keeps its host;
// on another partition it is moved by a single node-mismatch eviction, which the
// grace period can't avoid because the scheduler doesn't know about the pod.
func TestReconcile_SameNameSuccessorPlaced(t *testing.T) {
	tests := []struct {
		name          string
		leaderNode    string
		wantEvictions []string
	}{
		{name: "placed on the same partition", leaderNode: "node-1"},
		{name: "placed on another partition", leaderNode: "node-9", wantEvictions: []string{"lws-0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newDrainTestEnv(t, nil, successorSlice("uid-new", time.Now()),
				stampedPod("lws-0", tt.leaderNode, "uid-old"),
				drainPod("lws-1", "s", "", corev1.PodPending))

			// Unplaced successor: the leader is spared and nothing is bound.
			if _, err := env.reconcile(t, "s"); err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}
			if env.sub.lastEviction != nil || len(env.sub.bound) != 0 {
				t.Fatalf("unplaced successor: eviction = %v, bound = %v; want neither", env.sub.lastEviction, env.sub.bound)
			}

			// The scheduler places the successor on part-1 (node-1, node-2).
			slice := &slicev1beta1.Slice{}
			if err := env.client.Get(context.Background(), types.NamespacedName{Name: "s"}, slice); err != nil {
				t.Fatalf("failed to get slice: %v", err)
			}
			slice.Spec.PartitionIds = []string{"part-1"}
			if err := env.client.Update(context.Background(), slice); err != nil {
				t.Fatalf("failed to place slice: %v", err)
			}
			if _, err := env.reconcile(t, "s"); err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}

			var evicted []string
			if env.sub.lastEviction != nil {
				evicted = []string{env.sub.lastEviction.Name}
			}
			if !slices.Equal(evicted, tt.wantEvictions) {
				t.Errorf("evicted = %v, want %v", evicted, tt.wantEvictions)
			}
			if !slices.Equal(env.sub.bound, []string{"lws-1"}) {
				t.Errorf("bound = %v, want [lws-1]", env.sub.bound)
			}
		})
	}
}

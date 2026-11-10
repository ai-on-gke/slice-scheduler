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

package pod_reconciler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	JobCompletionIndexAnnotation    = "batch.kubernetes.io/job-completion-index"
	WorkerIdAnnotation              = "tpu.google.com/worker-id"
	PodIndexLabel                   = "apps.kubernetes.io/pod-index"
	LWSWorkerIndexLabel             = "leaderworkerset.sigs.k8s.io/worker-index"
	LWSSubgroupSizeAnnotation       = "leaderworkerset.sigs.k8s.io/subgroup-size"
	LWSSubgroupPolicyTypeAnnotation = "leaderworkerset.sigs.k8s.io/subgroup-policy-type"
	LWSLeaderExcludedPolicy         = "LeaderExcluded"
)

// appendHostNamesRecursive recursively collects hostnames from a partition node and its children.
func appendHostNamesRecursive(node *watcher.PartitionNode, hostNames *[]string) {
	if node == nil {
		return
	}
	// If it's a leaf node (no children), collect its hostnames.
	if len(node.Children) == 0 {
		*hostNames = append(*hostNames, node.Info.NodeNames...)
	}
	for _, child := range node.Children {
		appendHostNamesRecursive(child, hostNames)
	}
}

// parsePodIndex extracts the 0-indexed ordinal rank of a pod within its TPU Slice.
// Hierarchy resolution order:
// 1. Explicit Job / JobSet index annotations (batch.kubernetes.io/job-completion-index)
// 2. Explicit TPU worker annotations (tpu.google.com/worker-id)
// 3. LeaderWorkerSet worker index label (leaderworkerset.sigs.k8s.io/worker-index)
// 4. StatefulSet ordinal labels (apps.kubernetes.io/pod-index)
// 5. Fallback string derivation parsing trailing hyphenated numeric substring from pod.Name
//
// LWS is checked before the StatefulSet ordinal because LWS stamps both labels on
// its pods, and only the worker-index carries the pod's rank within its own group.
// The StatefulSet ordinal is group-relative and does not identify a host within a
// slice, so preferring it silently mis-assigns every LWS workload past the first
// group.
func parsePodIndex(pod *corev1.Pod) (int, error) {
	if pod.Annotations != nil {
		if indexStr, ok := pod.Annotations[JobCompletionIndexAnnotation]; ok {
			return strconv.Atoi(indexStr)
		}
		if indexStr, ok := pod.Annotations[WorkerIdAnnotation]; ok {
			return strconv.Atoi(indexStr)
		}
	}

	if pod.Labels != nil {
		if indexStr, ok := pod.Labels[LWSWorkerIndexLabel]; ok {
			workerIndex, err := strconv.Atoi(indexStr)
			if err != nil {
				return 0, err
			}
			// When LWS partitions a group into subgroups, worker-index stays
			// group-global while the slice only spans one subgroup, so fold it
			// back into subgroup-local space. Under the default LeaderWorker
			// policy the leader is worker 0 of subgroup 0, so workers fold
			// directly. Under LeaderExcluded the leader sits outside every
			// subgroup, so workers 1..n map onto hosts 0..n-1 and the leader
			// itself takes index 0.
			if pod.Annotations != nil {
				subgroupSizeStr := pod.Annotations[LWSSubgroupSizeAnnotation]
				if subgroupSizeStr != "" {
					if subgroupSize, err := strconv.Atoi(subgroupSizeStr); err == nil && subgroupSize > 0 {
						if pod.Annotations[LWSSubgroupPolicyTypeAnnotation] == LWSLeaderExcludedPolicy {
							if workerIndex == 0 {
								return 0, nil
							}
							return (workerIndex - 1) % subgroupSize, nil
						}
						return workerIndex % subgroupSize, nil
					}
				}
			}
			return workerIndex, nil
		}
		if indexStr, ok := pod.Labels[PodIndexLabel]; ok {
			return strconv.Atoi(indexStr)
		}
	}

	podName := pod.Name
	parts := strings.Split(podName, "-")
	if len(parts) >= 2 {
		ordinalStr := parts[len(parts)-1]
		if index, err := strconv.Atoi(ordinalStr); err == nil {
			return index, nil
		}
	}

	return 0, fmt.Errorf("could not determine pod index for %s, expected one of: "+
		"'batch.kubernetes.io/job-completion-index' annotation, "+
		"'tpu.google.com/worker-id' annotation, "+
		"'apps.kubernetes.io/pod-index' label, "+
		"'leaderworkerset.sigs.k8s.io/worker-index' label, "+
		"or <name>-<ordinal> naming format", podName)
}

// TrimPod is the transform the manager's pod informer applies (cmd/main.go). It
// keeps only the fields the pod reconciler reads, to bound cache memory. A
// field it drops reads as empty in the running binary even though tests built
// from full pods still see it, so every pod field the reconciler reads must be
// copied here; TestTrimPodKeepsWhatTheReconcilerReads checks the list.
func TrimPod(obj interface{}) (interface{}, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              pod.Name,
			Namespace:         pod.Namespace,
			Labels:            pod.Labels,
			Annotations:       pod.Annotations,
			DeletionTimestamp: pod.DeletionTimestamp,
		},
		Spec: corev1.PodSpec{
			SchedulerName: pod.Spec.SchedulerName,
			NodeName:      pod.Spec.NodeName,
			// handleScheduling must not bind a gated pod.
			SchedulingGates: pod.Spec.SchedulingGates,
		},
		// isPodTerminal reads the phase.
		Status: corev1.PodStatus{
			Phase: pod.Status.Phase,
		},
	}, nil
}

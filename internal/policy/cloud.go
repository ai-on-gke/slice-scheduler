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

package policy

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// ProvisionedPartitionAnnotation is set by the slice-controller while a
	// sub-slice is provisioned in GCE and removed only after deactivation completes.
	ProvisionedPartitionAnnotation = "accelerator.gke.io/provisioned-partition"

	// RetryAnnotation controls whether the slice-controller should retry
	// SliceCreationFailed rather than letting the scheduler reschedule.
	RetryAnnotation = "slice.gke.io/retry-on-failure"

	// EnableRetryValue is the annotation value that enables retry-on-failure.
	EnableRetryValue = "true"

	// PartitionStateLabelPrefix is the prefix for node health labels set by
	// the slice-controller's health monitor.
	PartitionStateLabelPrefix = "cloud.google.com/gke-tpu-partition-"

	// PartitionStateLabelSuffix is the suffix appended after the topology
	// portion of the partition state label.
	PartitionStateLabelSuffix = "-state"

	// RetryDelayNodeFetch is the delay before retrying when the target node
	// cannot be fetched from the API server.
	RetryDelayNodeFetch = 2 * time.Second

	// RetryDelayPartitionWait is the delay before retrying when the node is
	// missing the partition-state health label.
	RetryDelayPartitionWait = 2 * time.Second

	// DeactivationRequeueDelay is the requeue delay when a slice is skipped
	// because deactivation is in progress.
	DeactivationRequeueDelay = 5 * time.Second

	// PartitionStateHealthy is the label value indicating a healthy partition.
	PartitionStateHealthy = "HEALTHY"

	// PartitionStateDegraded is the label value indicating a degraded but
	// functional partition.
	PartitionStateDegraded = "DEGRADED"
)

// Slice Ready condition reason values used by the cloud slice-controller.
const (
	ReasonFailed = "FAILED"
	// ReasonMMIGNotFound and ReasonPreflightError are defensive guards.
	// Currently the slice-controller maps these to SliceCreationFailed (as the
	// reason) with the specific error in the message field. These constants are
	// retained in case the controller changes to use them as standalone reasons.
	ReasonMMIGNotFound           = "MMIGNotFound"
	ReasonPreflightError         = "PreflightError"
	ReasonSliceCreationFailed    = "SliceCreationFailed"
	ReasonSliceHealthProbeFailed = "SliceHealthProbeFailed"
	ReasonActive                 = "ACTIVE"
	ReasonActiveDegraded         = "ACTIVE_DEGRADED"
)

// CloudPolicy implements SlicePolicy for GKE/Cloud environments.
// It handles GCE-specific annotations, deactivation guards, and node
// partition-state health labels.
type CloudPolicy struct{}

var _ SlicePolicy = (*CloudPolicy)(nil)

// NewCloudPolicy creates a new CloudPolicy for GKE/Cloud environments.
func NewCloudPolicy() *CloudPolicy {
	return &CloudPolicy{}
}

// NeedsRescheduling returns true if the Slice's Ready condition indicates
// a failure state that requires rescheduling to a new partition.
//
// The following Ready condition states trigger rescheduling:
//   - FAILED (ConditionFalse): GCE reported a hard failure.
//   - MMIGNotFound (ConditionFalse): The managed instance group was deleted.
//   - PreflightError (ConditionFalse): Pre-creation validation failed.
//   - SliceCreationFailed (ConditionFalse): GCE create call failed.
//   - SliceHealthProbeFailed (ConditionUnknown): Health probe lost contact.
func (c *CloudPolicy) NeedsRescheduling(slice *slicev1beta1.Slice) bool {
	cond := FindReadyCondition(slice)
	if cond == nil {
		return false
	}

	switch {
	case cond.Status == metav1.ConditionFalse:
		switch cond.Reason {
		case ReasonFailed, ReasonMMIGNotFound, ReasonPreflightError, ReasonSliceCreationFailed:
			return true
		}
	case cond.Status == metav1.ConditionUnknown:
		if cond.Reason == ReasonSliceHealthProbeFailed {
			return true
		}
	}
	return false
}

// ShouldBackoffOnFailure returns true when the scheduler should defer to
// the slice-controller's retry mechanism rather than rescheduling immediately.
//
// Precondition: NeedsRescheduling(slice) must be true. This method is only
// called after NeedsRescheduling has already verified the condition status,
// so we only switch on Reason here.
//
// Backoff cases:
//   - SliceCreationFailed with retry annotation enabled: the controller will
//     retry the GCE create call on the same partition.
//   - SliceHealthProbeFailed: transient probe failure; the controller will
//     retry health checks.
func (c *CloudPolicy) ShouldBackoffOnFailure(slice *slicev1beta1.Slice) bool {
	cond := FindReadyCondition(slice)
	if cond == nil {
		return false
	}

	switch cond.Reason {
	case ReasonSliceCreationFailed:
		return slice.Annotations[RetryAnnotation] == EnableRetryValue
	case ReasonSliceHealthProbeFailed:
		return true
	}
	return false
}

// ShouldEvictPod returns true if pods running on this slice should be evicted
// due to the slice's failure state.
//
// The partitionBroken parameter indicates whether any of the slice's
// partitions are broken (missing from cache or unhealthy).
//
// Eviction rules:
//   - FAILED, MMIGNotFound, PreflightError: always evict.
//   - SliceCreationFailed with retry disabled: evict.
//   - SliceCreationFailed with retry enabled: evict only if partitions are broken.
//     This absorbs the partition-health check so callers don't need cloud-specific knowledge.
//   - SliceHealthProbeFailed: never evict (transient).
func (c *CloudPolicy) ShouldEvictPod(slice *slicev1beta1.Slice, partitionBroken bool) bool {
	// Check for preemption eviction annotation — the scheduler annotates
	// victims for graceful eviction before clearing partitions.
	if slice.Annotations != nil && slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr {
		return true
	}
	sliceState := GetSliceState(slice)
	switch sliceState {
	case ReasonFailed, ReasonMMIGNotFound, ReasonPreflightError:
		return true
	case ReasonSliceCreationFailed:
		if slice.Annotations[RetryAnnotation] == EnableRetryValue {
			// When retry is enabled, only evict if partitions are broken.
			return partitionBroken
		}
		return true
	case ReasonSliceHealthProbeFailed:
		return false
	}
	return false
}

// IsSliceReady returns true if the slice state indicates the slice is healthy
// and ready for pod binding.
func (c *CloudPolicy) IsSliceReady(sliceState string) bool {
	return sliceState == ReasonActive || sliceState == ReasonActiveDegraded
}

// ShouldSkipReconcile returns true if the slice should be skipped during
// reconciliation because an N→0 deactivation is still in progress on GKE.
//
// A bound sub-slice carries accelerator.gke.io/provisioned-partition for its
// entire active lifetime; deactivation only begins once Spec.PartitionIds has
// been cleared to empty. During N→0 teardown, slice-controller removes
// provisioned-partition before updating Ready=SliceNotCreated and advancing
// Status.ObservedGeneration to match Generation, so both signals are checked
// while Spec.PartitionIds is empty.
func (c *CloudPolicy) ShouldSkipReconcile(slice *slicev1beta1.Slice) (bool, string, time.Duration) {
	if len(slice.Spec.PartitionIds) == 0 {
		if slice.Annotations[ProvisionedPartitionAnnotation] != "" {
			return true, "deactivation in progress (provisioned-partition annotation present, no partitions assigned)", DeactivationRequeueDelay
		}
		if slice.Status.ObservedGeneration > 0 && slice.Generation > slice.Status.ObservedGeneration {
			return true, "deactivation in progress (waiting for slice-controller to finish N->0 status update)", DeactivationRequeueDelay
		}
	}
	return false, "", 0
}

// IsNodeReadyForBinding checks that the target node has the expected
// cloud.google.com/gke-tpu-topology, cloud.google.com/gke-tpu-slice, and
// partition-state health labels (cloud.google.com/gke-tpu-partition-<topology>-state=HEALTHY|DEGRADED)
// on nodes after provisioning completes.
//
// For superslice topologies (e.g. 4x4x8), the health label uses the per-cube
// topology (4x4x4) rather than the full slice topology.
func (c *CloudPolicy) IsNodeReadyForBinding(ctx context.Context, k8sClient client.Reader,
	nodeName string, topology string, sliceName string) (bool, time.Duration, error) {

	if topology == "" {
		return true, 0, nil
	}

	node := &corev1.Node{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return false, RetryDelayNodeFetch, fmt.Errorf("unable to fetch node %s: %w", nodeName, err)
	}

	healthTopology := healthLabelTopology(topology)

	// Verify gke-tpu-topology label (matches full topology or per-cube topology for superslices)
	nodeTopology, hasTopology := node.Labels["cloud.google.com/gke-tpu-topology"]
	if !hasTopology || (nodeTopology != topology && nodeTopology != healthTopology) {
		return false, RetryDelayPartitionWait, nil
	}

	// Verify gke-tpu-slice label
	if sliceName != "" {
		nodeSlice, hasSliceName := node.Labels["cloud.google.com/gke-tpu-slice"]
		if !hasSliceName || nodeSlice != sliceName {
			return false, RetryDelayPartitionWait, nil
		}
	}

	// Verify partition state health label
	label := PartitionStateLabelPrefix + healthTopology + PartitionStateLabelSuffix
	stateValue, hasLabel := node.Labels[label]
	if !hasLabel || (stateValue != PartitionStateHealthy && stateValue != PartitionStateDegraded) {
		return false, RetryDelayPartitionWait, nil
	}

	return true, 0, nil
}

// healthLabelTopology returns the topology string used in health labels.
// The slice-controller's health monitor sets node labels at the cube level:
//   - For topologies <= 4x4x4 (e.g. 2x2x1, 4x4x4): labels use the exact topology.
//   - For superslices > 4x4x4 (e.g. 4x4x8): labels use "4x4x4" (per-cube).
func healthLabelTopology(topology string) string {
	const cubeTopology = "4x4x4"
	parts := strings.SplitN(topology, "x", 3)
	if len(parts) != 3 {
		return topology
	}
	for _, p := range parts {
		d, err := strconv.Atoi(p)
		if err != nil {
			return topology
		}
		if d > 4 {
			return cubeTopology
		}
	}
	return topology
}

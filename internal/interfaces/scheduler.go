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

package interfaces

import (
	"context"
	"fmt"
	"sync"

	goslices "slices"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// --- Constants ---

const (
	SliceSchedulerName  = "slice-scheduler"
	EvictPodsAnnotation = "cloud.google.com/evict-pods"

	// Boolean string values
	TrueStr  = "true"
	FalseStr = "false"

	// Degradation tolerance modes (from workload annotation)
	DegradationToleranceNone = "none"
	DegradationToleranceAll  = "all"

	// Degradation modes (internal representation)
	DegradationModeNormal   = "NORMAL"
	DegradationModeDegraded = "DEGRADED"

	// Partition states (matches Cloud CRD PartitionStateType)
	PartitionStateDegraded = "Degraded" // NOTE: title-case for Cloud CRD, NOT "DEGRADED"

	// Taint keys written by the partition-tree controller itself, as opposed to
	// node taints it copies onto leaf partitions.
	TaintKeyPartitionState = "cloud.google.com/partition-state"
	TaintKeyNodesNotFound  = "cloud.google.com/nodes-not-found"
)

var (
	// BlockedTaints is the list of taint keys the scheduler evicts running slices for, when
	// the taint is NoExecute. Placement doesn't use it: HasBlockedTaint blocks on any key.
	// The node.kubernetes.io keys reach the PartitionTree only when the
	// partition-tree controller runs with node taint propagation enabled.
	BlockedTaints = []string{
		corev1.TaintNodeNotReady,
		corev1.TaintNodeUnreachable,
		corev1.TaintNodeUnschedulable,
		TaintKeyPartitionState,
		TaintKeyNodesNotFound,
	}

	// BlockedEffects is the list of taint effects that block partition scheduling.
	// PreferNoSchedule is excluded: it is a soft preference, not a hard veto.
	BlockedEffects = []partitiontreev1.TaintEffect{
		partitiontreev1.TaintEffectNoSchedule,
		partitiontreev1.TaintEffectNoExecute,
	}
)

// --- Domain Models ---

// SliceInfo holds information about the slice using a partition.
type SliceInfo struct {
	Name string
	// UID tells apart Slices that reuse a name: a Slice can be deleted and recreated
	// under the same name before the scheduler sees the delete. Empty when the SliceInfo
	// wasn't built from a Slice object.
	UID          types.UID
	Priority     int
	Topology     string
	Type         string
	PartitionIds []string
	ManagedBy    string
}

// SameSlice reports whether s and other describe the same Slice object: they share a
// name, and their UIDs match if both are known.
func (s *SliceInfo) SameSlice(other *SliceInfo) bool {
	if s == nil || other == nil || s.Name != other.Name {
		return false
	}
	return s.UID == "" || other.UID == "" || s.UID == other.UID
}

// DeepCopy returns a deep copy of the SliceInfo.
func (s *SliceInfo) DeepCopy() *SliceInfo {
	if s == nil {
		return nil
	}
	out := new(SliceInfo)
	*out = *s
	if s.PartitionIds != nil {
		out.PartitionIds = make([]string, len(s.PartitionIds))
		copy(out.PartitionIds, s.PartitionIds)
	}
	return out
}

// OwnsSlice reports whether the slice-scheduler instance for tpuType manages a slice with
// the given managed-by value and TPU type. It is the single ownership test shared by the
// scheduler and the pod reconciler: one instance runs per TPU type, and every instance
// sees every Slice.
func OwnsSlice(managedBy, sliceType, tpuType string) bool {
	return managedBy == SliceSchedulerName && sliceType == tpuType
}

// DegradationState represents the degradation status of a partition.
type DegradationState struct {
	Mode             string // "NORMAL", "DEGRADED"
	ToleratedOcsName string
}

// DeepCopy returns a deep copy of the DegradationState.
func (d *DegradationState) DeepCopy() *DegradationState {
	if d == nil {
		return nil
	}
	out := new(DegradationState)
	*out = *d
	return out
}

// PartitionInfo represents a partition and its status in a generic way.
type PartitionInfo struct {
	ID               string
	Topology         string
	IsAvailable      bool // True if the partition itself is available (e.g. HEALTHY/EMPTY)
	IsOccupied       bool // True if the partition is physically occupied
	IsBroken         bool // True if the partition or its nodes are unhealthy/broken
	TestingNotReady  bool // True if the partition or its nodes are marked NotReady for testing
	Taints           []*partitiontreev1.PartitionTaint
	InUseBy          *SliceInfo
	PodName          string
	State            string // Raw CRD state for domain fallback
	Children         []*PartitionInfo
	DegradationState *DegradationState
}

// HasBlockedTaint returns true if any taint on PartitionInfo has an effect in BlockedEffects,
// whatever its key. The scheduler doesn't check tolerations, so a node taint that workloads
// tolerate has to be kept off the PartitionTree, with the partition-tree controller's
// taints.nodePropagation.excludedPrefixes.
func (p *PartitionInfo) HasBlockedTaint() bool {
	if p == nil {
		return false
	}
	for _, t := range p.Taints {
		if t != nil && goslices.Contains(BlockedEffects, t.Effect) {
			return true
		}
	}
	return false
}

// HasNoExecuteBlockedTaint reports whether any taint on PartitionInfo is IsNoExecuteBlocked.
func (p *PartitionInfo) HasNoExecuteBlockedTaint() bool {
	if p == nil {
		return false
	}
	return goslices.ContainsFunc(p.Taints, IsNoExecuteBlocked)
}

// IsNoExecuteBlocked reports whether a taint is one the scheduler evicts running work for:
// NoExecute effect with a key in BlockedTaints. The key check keeps this a strict subset of
// HasBlockedTaint, so the scheduler never evicts a slice off a partition that TaintFilter
// would immediately place it back onto.
func IsNoExecuteBlocked(t *partitiontreev1.PartitionTaint) bool {
	return t != nil && t.Effect == partitiontreev1.TaintEffectNoExecute && goslices.Contains(BlockedTaints, t.Key)
}

// DeepCopy returns a deep copy of the PartitionInfo.
func (p *PartitionInfo) DeepCopy() *PartitionInfo {
	if p == nil {
		return nil
	}
	out := new(PartitionInfo)
	*out = *p
	out.InUseBy = p.InUseBy.DeepCopy()
	// *out = *p copies only the slice header, and its elements point into the watcher cache.
	// Copy the taint values so a snapshot cannot observe a later cache mutation.
	if p.Taints != nil {
		out.Taints = make([]*partitiontreev1.PartitionTaint, len(p.Taints))
		for i, t := range p.Taints {
			out.Taints[i] = t.DeepCopy()
		}
	}
	if p.Children != nil {
		out.Children = make([]*PartitionInfo, len(p.Children))
		for i, v := range p.Children {
			out.Children[i] = v.DeepCopy()
		}
	}
	out.DegradationState = p.DegradationState.DeepCopy()
	return out
}

// SliceRequirement encapsulates the needs of an individual slice within a group.
type SliceRequirement struct {
	SliceName            string
	Type                 string   // e.g. "tpu7x"
	Topology             string   // e.g. "4x4x4"
	DegradationTolerance string   // "none", "all"
	CurrentPartitionIDs  []string // currently assigned partitions
}

// DeepCopy returns a deep copy of the SliceRequirement.
func (s *SliceRequirement) DeepCopy() *SliceRequirement {
	if s == nil {
		return nil
	}
	out := new(SliceRequirement)
	*out = *s
	if s.CurrentPartitionIDs != nil {
		out.CurrentPartitionIDs = make([]string, len(s.CurrentPartitionIDs))
		copy(out.CurrentPartitionIDs, s.CurrentPartitionIDs)
	}
	return out
}

// SliceGroup represents a group of slices that should be scheduled together.
type SliceGroup struct {
	Name         string                // From slice-group-name annotation
	ExpectedSize int                   // From num-slices annotation
	MinSize      int                   // Minimum size for scheduling (defaults to ExpectedSize)
	Slices       map[string]*SliceInfo // Track slices by their name
	Priority     int32                 // Priority for the group
}

// IsReady returns true if the group has reached its expected size.
func (sg *SliceGroup) IsReady() bool {
	return len(sg.Slices) >= sg.ExpectedSize
}

// GroupAllocationRequest represents the requirements for a slice allocation.
type GroupAllocationRequest struct {
	GroupName string
	Priority  int32
	MinSize   int
	Slices    []SliceRequirement
}

// DeepCopy returns a deep copy of the GroupAllocationRequest.
func (g *GroupAllocationRequest) DeepCopy() *GroupAllocationRequest {
	if g == nil {
		return nil
	}
	out := new(GroupAllocationRequest)
	*out = *g
	if g.Slices != nil {
		out.Slices = make([]SliceRequirement, len(g.Slices))
		for i, v := range g.Slices {
			out.Slices[i] = *v.DeepCopy()
		}
	}
	return out
}

// GroupAllocationResult represents the result of a scheduling attempt.
type GroupAllocationResult struct {
	IsSchedulable bool

	// Candidate is the chosen placement. The orchestrator uses this for reservation.
	Candidate *PlacementCandidate

	FailureReason string
}

// ClusterState represents a read-only snapshot of available resources.
type ClusterState interface {
	// GetPartitionTrees returns the root partitions for all TPU pods in the cluster.
	GetPartitionTrees() []*PartitionInfo
	// GetPartitionInfo returns the PartitionInfo associated with the specified partition IDs.
	GetPartitionInfo(ids []string) []*PartitionInfo
}

// NewConflictError creates a new error representing a reservation conflict.
func NewConflictError(sliceName string, partitions []string, existingSlice string) error {
	return fmt.Errorf("partition was taken during scheduling, sliceName: %s, partitionIds: %v, existingSlice: %s", sliceName, partitions, existingSlice)
}

// --- Component 1: The Watcher ---

// ClusterWatcher maintains a real-time, read-only cache of the cluster state and handles optimistic locking.
type ClusterWatcher interface {
	// GetState returns a thread-safe, read-only snapshot of the cluster
	GetState() ClusterState

	// Reserve optimistically marks resources in the local cache.
	// It should be transactional: if reserving any slice fails, it rolls back (releases)
	// any slices successfully reserved during this call.
	Reserve(ctx context.Context, slices []*SliceInfo) error
	// Unreserve releases reservations for a list of slices.
	Unreserve(ctx context.Context, slices []*SliceInfo)
}

// --- Component 2: The Core Library & Plugins ---

// PlacementOption represents a selected group of physical partitions (cubes)
// and their associated victims inside a single pod, before being mapped to
// a PlacementCandidate.
type PlacementOption struct {
	Partitions []*PartitionInfo
	Victims    []*SliceInfo
}

// PlacementCandidate represents a potential assignment (free or via preemption).
type PlacementCandidate struct {
	// Map of SliceName -> List of assigned partition IDs
	Assignments map[string][]*PartitionInfo

	// The complete list of victim slices to evict to satisfy the entire group
	Victims []*SliceInfo
}

// DeepCopy returns a deep copy of the PlacementCandidate.
func (p *PlacementCandidate) DeepCopy() *PlacementCandidate {
	if p == nil {
		return nil
	}
	out := new(PlacementCandidate)
	if p.Assignments != nil {
		out.Assignments = make(map[string][]*PartitionInfo)
		for k, v := range p.Assignments {
			if v != nil {
				nodes := make([]*PartitionInfo, len(v))
				for i, n := range v {
					nodes[i] = n.DeepCopy()
				}
				out.Assignments[k] = nodes
			}
		}
	}
	if p.Victims != nil {
		out.Victims = make([]*SliceInfo, len(p.Victims))
		for i, v := range p.Victims {
			out.Victims[i] = v.DeepCopy()
		}
	}
	return out
}

// FilterPlugin evaluates a node during tree traversal for early pruning.
type FilterPlugin interface {
	Name() string
	Filter(ctx context.Context, req GroupAllocationRequest, node *PartitionInfo) bool
}

// Score represents a generic score returned by a ScorePlugin.
type Score interface {
	Less(other Score) bool
}

// ScorePlugin ranks final candidates.
type ScorePlugin interface {
	Name() string
	Score(ctx context.Context, req GroupAllocationRequest, candidate *PlacementCandidate, state ClusterState) Score
}

// PlacementGenerator encapsulates TPU topology traversal logic.
type PlacementGenerator interface {
	Name() string
	Generate(ctx context.Context, req GroupAllocationRequest, state ClusterState, eligiblePartitions map[string]struct{}) []*PlacementCandidate
}

// SchedulerLibrary is the entry point for the core logic.
type SchedulerLibrary interface {
	// Name returns the unique identifier for this implementation (e.g., "Default").
	Name() string
	// Schedule calculates the allocation based on the request and current state.
	Schedule(ctx context.Context, req GroupAllocationRequest, state ClusterState) GroupAllocationResult
}

// Registry stores multiple SchedulerLibrary implementations so the controller can pick one.
type Registry struct {
	mu         sync.RWMutex
	schedulers map[string]SchedulerLibrary
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	return &Registry{
		schedulers: make(map[string]SchedulerLibrary),
	}
}

// Register adds a SchedulerLibrary to the registry.
func (r *Registry) Register(s SchedulerLibrary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schedulers[s.Name()] = s
}

// Get retrieves a SchedulerLibrary from the registry by name.
func (r *Registry) Get(name string) (SchedulerLibrary, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.schedulers[name]
	if !ok {
		return nil, fmt.Errorf("scheduler implementation %q not found", name)
	}
	return s, nil
}

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

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/util"
)

// defaultDegradationDomain is the placeholder OCS domain used by the Cloud
// adapter. Cloud's CRD does not carry OCS link names, so we use a sentinel
// value to satisfy the scheduler's internal DegradationState contract.
const defaultDegradationDomain = "DEFAULT_DEGRADATION_DOMAIN"

// PartitionWatcher implements interfaces.ClusterWatcher and wraps PartitionCache.
type PartitionWatcher struct {
	cache *PartitionCache
}

// NewPartitionWatcher creates a new PartitionWatcher.
func NewPartitionWatcher(cache *PartitionCache) *PartitionWatcher {
	return &PartitionWatcher{
		cache: cache,
	}
}

// GetState returns a read-only snapshot of the cluster state.
//
// The trees and the id index are built together under a single read lock, so
// every accessor on the returned ClusterState describes the same instant and
// hands out the same *PartitionInfo for a given partition. Converting lazily,
// or per call, lets one scheduling cycle observe two different versions of the
// cluster.
func (pw *PartitionWatcher) GetState() interfaces.ClusterState {
	pw.cache.mu.RLock()
	defer pw.cache.mu.RUnlock()

	trees := make([]*interfaces.PartitionInfo, 0, len(pw.cache.partitionTreeCache))
	infoMap := make(map[string]*interfaces.PartitionInfo)

	var convertAndMap func(node *PartitionNode, parentInUse *interfaces.SliceInfo) *interfaces.PartitionInfo
	convertAndMap = func(node *PartitionNode, parentInUse *interfaces.SliceInfo) *interfaces.PartitionInfo {
		currentInUse := node.InUseBy
		if currentInUse == nil {
			currentInUse = parentInUse
		}

		info := &interfaces.PartitionInfo{
			ID:              node.Info.PartitionID,
			Topology:        util.ToTopology(node.Info.Topology),
			IsAvailable:     util.IsPartitionAvailable(node.Info),
			IsOccupied:      util.IsPartitionOccupied(node.Info),
			IsBroken:        node.isBroken(pw.cache.enableNodeReadiness),
			TestingNotReady: node.isTestingNotReady(),
			InUseBy:         currentInUse,
			PodName:         node.PodName,
			State:           string(node.Info.State),
		}
		if pw.cache.enableTaintFiltering && len(node.Taints) > 0 {
			info.Taints = make([]*partitiontreev1.PartitionTaint, len(node.Taints))
			for i := range node.Taints {
				info.Taints[i] = node.Taints[i].DeepCopy()
			}
		}

		// Adapter: Cloud's CRD uses State enum ("Degraded"), not a separate
		// DegradationState struct. Translate to the scheduler's internal representation.
		if node.Info.State == partitiontreev1.PartitionStateDegraded {
			info.DegradationState = &interfaces.DegradationState{
				Mode:             interfaces.DegradationModeDegraded,
				ToleratedOcsName: defaultDegradationDomain,
			}
		}

		infoMap[info.ID] = info

		if len(node.Children) > 0 {
			info.Children = make([]*interfaces.PartitionInfo, len(node.Children))
			for i, child := range node.Children {
				info.Children[i] = convertAndMap(child, currentInUse)
			}
		}
		return info
	}

	for _, root := range pw.cache.partitionTreeCache {
		trees = append(trees, convertAndMap(root, nil))
	}

	return &partitionState{
		trees:   trees,
		infoMap: infoMap,
	}
}

// Reserve marks partitions as in-use for a list of slices.
func (pw *PartitionWatcher) Reserve(ctx context.Context, slices []*interfaces.SliceInfo) error {
	if len(slices) == 0 {
		return nil
	}

	// Reserve all slices in the group
	reservedSlices := make([]*interfaces.SliceInfo, 0, len(slices))
	for _, sInfo := range slices {
		if ok, existing := pw.cache.ReservePartition(ctx, sInfo); !ok {
			// Rollback: Unreserve what we just reserved
			pw.cache.ReleasePartitions(ctx, reservedSlices)
			return interfaces.NewConflictError(sInfo.Name, sInfo.PartitionIds, existing)
		}
		reservedSlices = append(reservedSlices, sInfo)
	}

	return nil
}

// Unreserve releases reservations for a list of slices.
func (pw *PartitionWatcher) Unreserve(ctx context.Context, slices []*interfaces.SliceInfo) {
	if len(slices) > 0 {
		pw.cache.ReleasePartitions(ctx, slices)
	}
}

// partitionState implements interfaces.ClusterState.
type partitionState struct {
	trees   []*interfaces.PartitionInfo
	infoMap map[string]*interfaces.PartitionInfo
}

// GetPartitionTrees returns the partition trees captured in this snapshot.
func (ps *partitionState) GetPartitionTrees() []*interfaces.PartitionInfo {
	return ps.trees
}

// GetPartitionInfo resolves ids against the snapshot taken in GetState. The
// returned pointers are the same nodes reachable from GetPartitionTrees, so a
// partition whose ancestor holds a reservation reports that ancestor's
// InUseBy. Unknown ids are skipped.
func (ps *partitionState) GetPartitionInfo(ids []string) []*interfaces.PartitionInfo {
	result := make([]*interfaces.PartitionInfo, 0, len(ids))
	for _, id := range ids {
		if info, ok := ps.infoMap[id]; ok {
			result = append(result, info)
		}
	}
	return result
}

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

package plugins

import (
	"context"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

// GetEligiblePartitions pre-evaluates filters for a specific request and returns a map of eligible partition IDs.
func GetEligiblePartitions(ctx context.Context, req interfaces.GroupAllocationRequest, state interfaces.ClusterState, filters []interfaces.FilterPlugin) map[string]struct{} {
	eligible := make(map[string]struct{})
	if state == nil {
		return eligible
	}
	for _, tree := range state.GetPartitionTrees() {
		collectEligibleRecursive(ctx, req, tree, filters, eligible)
	}
	return eligible
}

func collectEligibleRecursive(ctx context.Context, req interfaces.GroupAllocationRequest, node *interfaces.PartitionInfo, filters []interfaces.FilterPlugin, eligible map[string]struct{}) {
	// Check if this node passes all filters
	nodePasses := true
	for _, f := range filters {
		if !f.Filter(ctx, req, node) {
			nodePasses = false
			break
		}
	}

	if nodePasses {
		eligible[node.ID] = struct{}{}
	}

	// Recursively apply to children
	for _, child := range node.Children {
		collectEligibleRecursive(ctx, req, child, filters, eligible)
	}
}

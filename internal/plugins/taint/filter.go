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

// Package taint filters out partitions carrying a blocked node taint.
package taint

import (
	"context"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
)

// TaintFilter prunes nodes that have blocked taints applied.
type TaintFilter struct{}

func (f *TaintFilter) Name() string { return "TaintFilter" }

func (f *TaintFilter) Filter(ctx context.Context, req interfaces.GroupAllocationRequest, node *interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	return !node.HasBlockedTaint()
}

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

package pq

import (
	"container/heap"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPriorityQueue_Dedup(t *testing.T) {
	pq := NewPriorityQueue()

	// Push a slice with priority 10
	heap.Push(pq, &Item{GroupId: "slice-1", Priority: 10})

	// Push the same slice with a lower priority
	heap.Push(pq, &Item{GroupId: "slice-1", Priority: 5})

	if pq.Len() != 1 {
		t.Errorf("Expected queue length to be 1, but got %d", pq.Len())
	}

	item := heap.Pop(pq).(*Item)
	if item.GroupId != "slice-1" || item.Priority != 5 {
		t.Errorf("Expected to pop slice-1 with priority 5, but got %s with priority %d", item.GroupId, item.Priority)
	}
}

func TestPriorityQueue_Order(t *testing.T) {
	pq := NewPriorityQueue()

	items := []*Item{
		{GroupId: "slice-low", Priority: 10},
		{GroupId: "slice-high", Priority: 100},
		{GroupId: "slice-medium", Priority: 50},
	}

	for _, item := range items {
		heap.Push(pq, item)
	}

	expectedOrder := []string{"slice-high", "slice-medium", "slice-low"}
	processedOrder := make([]string, 0, len(items))

	for pq.Len() > 0 {
		item := heap.Pop(pq).(*Item)
		processedOrder = append(processedOrder, item.GroupId)
	}

	if diff := cmp.Diff(expectedOrder, processedOrder); diff != "" {
		t.Errorf("Slices in queue have wrong order (-want +got):\n%s", diff)
	}
}

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
	"time"
)

// Item represents an item in the priority queue.
type Item struct {
	GroupId          string
	Topology         string
	Priority         int
	Index            int // The index of the item in the heap.
	EnqueueTimeStamp time.Time
}

// PriorityQueue implements heap.Interface and holds PQItems.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Priority > pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push adds an item to the priority queue. If an item with the same GroupId
// already exists, it will update the priority if the new priority is higher.
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Item)

	// Check if the item already exists.
	for _, existingItem := range *pq {
		if existingItem.GroupId == item.GroupId {
			// Update the priority and fix the heap.
			existingItem.Priority = item.Priority
			heap.Fix(pq, existingItem.Index)
			return
		}
	}

	// If the item does not exist, add it to the queue.
	n := len(*pq)
	item.Index = n
	*pq = append(*pq, item)
}

// Pop removes and returns the highest priority item from the queue.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// NewPriorityQueue creates a new priority queue.
func NewPriorityQueue() *PriorityQueue {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &pq
}

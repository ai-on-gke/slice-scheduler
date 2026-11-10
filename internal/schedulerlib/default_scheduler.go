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

package schedulerlib

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/metrics"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	TPU7x = "tpu7x"
	TPU6e = "v6e"
)

// DefaultScheduler is the primary implementation of the modular scheduling framework.
// It orchestrates the scheduling pipeline by invoking generators, filters, and scorers.
type DefaultScheduler struct {
	generators []interfaces.PlacementGenerator
	filters    []interfaces.FilterPlugin
	scorers    []interfaces.ScorePlugin
}

func NewDefaultScheduler(
	generators []interfaces.PlacementGenerator,
	filters []interfaces.FilterPlugin,
	scorers []interfaces.ScorePlugin,
) *DefaultScheduler {
	return &DefaultScheduler{
		generators: generators,
		filters:    filters,
		scorers:    scorers,
	}
}

func (s *DefaultScheduler) Name() string {
	return "ModularDefault"
}

// FilterPhase pre-evaluates all filters for this request and builds a map of eligible partitions.
func (s *DefaultScheduler) FilterPhase(ctx context.Context, req interfaces.GroupAllocationRequest, state interfaces.ClusterState) map[string]struct{} {
	return plugins.GetEligiblePartitions(ctx, req, state, s.filters)
}

// GeneratePhase generates placement candidates using all configured generators.
func (s *DefaultScheduler) GeneratePhase(ctx context.Context, req interfaces.GroupAllocationRequest, state interfaces.ClusterState, eligiblePartitions map[string]struct{}) []*interfaces.PlacementCandidate {
	var validCandidates []*interfaces.PlacementCandidate
	for _, gen := range s.generators {
		candidates := gen.Generate(ctx, req, state, eligiblePartitions)
		validCandidates = append(validCandidates, candidates...)
	}
	return validCandidates
}

// ScorePhase executes lexicographical scoring to find the best placement candidate.
func (s *DefaultScheduler) ScorePhase(ctx context.Context, req interfaces.GroupAllocationRequest, state interfaces.ClusterState, validCandidates []*interfaces.PlacementCandidate) *interfaces.PlacementCandidate {
	scores := make([][]interfaces.Score, len(validCandidates))
	for i := range scores {
		scores[i] = make([]interfaces.Score, len(s.scorers))
	}

	// Limit scorer execution concurrency to GOMAXPROCS (respecting container cgroup CPU quotas)
	maxConcurrency := runtime.GOMAXPROCS(0)
	if maxConcurrency < 4 {
		maxConcurrency = 4 // Ensure a sensible minimum for parallel processing
	}
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for idx, candidate := range validCandidates {
		wg.Add(1)
		sem <- struct{}{} // Acquire slot
		go func(cIdx int, cand *interfaces.PlacementCandidate) {
			defer func() {
				if r := recover(); r != nil {
					log.FromContext(ctx).Error(nil, "Panic recovered in scorer goroutine",
						"candidateIndex", cIdx, "panic", r)
				}
				<-sem // Release slot
				wg.Done()
			}()
			for sIdx, scorer := range s.scorers {
				scores[cIdx][sIdx] = scorer.Score(ctx, req, cand, state)
			}
		}(idx, candidate)
	}
	wg.Wait()
	var bestCandidate *interfaces.PlacementCandidate
	var bestScores []interfaces.Score

	for idx, candidate := range validCandidates {
		candidateScores := scores[idx]

		if bestCandidate == nil {
			bestCandidate = candidate
			bestScores = candidateScores
			continue
		}

		// Compare scores lexicographically to enforce a strict hierarchy
		isBetter := false
		for i := 0; i < len(candidateScores); i++ {
			if candidateScores[i].Less(bestScores[i]) {
				isBetter = true
				break
			}
			if bestScores[i].Less(candidateScores[i]) {
				break // Strictly worse at this tier, stop comparing
			}
		}

		if isBetter {
			bestCandidate = candidate
			bestScores = candidateScores
		}
	}
	return bestCandidate
}

func (s *DefaultScheduler) Schedule(ctx context.Context, req interfaces.GroupAllocationRequest, state interfaces.ClusterState) interfaces.GroupAllocationResult {
	if len(req.Slices) == 0 {
		return interfaces.GroupAllocationResult{
			IsSchedulable: false,
			FailureReason: "No slices in group",
		}
	}
	sliceReq := req.Slices[0]
	logf := log.FromContext(ctx, "sliceName", sliceReq.SliceName, "groupName", req.GroupName)
	logf.Info("Starting modular scheduling cycle", "topology", sliceReq.Topology)

	// 1. Filter Phase
	filterStart := time.Now()
	eligiblePartitions := s.FilterPhase(ctx, req, state)
	metrics.SliceSchedulerStepDurationSeconds.WithLabelValues(sliceReq.Type, sliceReq.Topology, "filter").Observe(time.Since(filterStart).Seconds())

	// 2. Generate Phase
	genStart := time.Now()
	validCandidates := s.GeneratePhase(ctx, req, state, eligiblePartitions)
	metrics.SliceSchedulerStepDurationSeconds.WithLabelValues(sliceReq.Type, sliceReq.Topology, "generate").Observe(time.Since(genStart).Seconds())
	if len(validCandidates) == 0 {
		return interfaces.GroupAllocationResult{
			IsSchedulable: false,
			FailureReason: "No valid placement candidates found after generation and filtering",
		}
	}

	// 3. Score Phase
	scoreStart := time.Now()
	bestCandidate := s.ScorePhase(ctx, req, state, validCandidates)
	metrics.SliceSchedulerStepDurationSeconds.WithLabelValues(sliceReq.Type, sliceReq.Topology, "score").Observe(time.Since(scoreStart).Seconds())

	// 4. Return Result
	result := interfaces.GroupAllocationResult{
		IsSchedulable: true,
		Candidate:     bestCandidate,
	}

	return result
}

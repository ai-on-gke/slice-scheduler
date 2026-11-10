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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// queueLatencyBuckets covers a queue wait from a few milliseconds, an idle
// queue popped straight away, up to 30 minutes. A single worker drains the
// queue, so under a backlog a group waits behind every group ahead of it, and
// DefBuckets, which stop at 10 seconds, would put every such wait in +Inf. The
// boundaries at 60 seconds and above keep quantiles meaningful for the queue
// wait alert in docs/user_playbook.md.
var queueLatencyBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800,
}

var (
	SliceScheduleAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slice_schedule_attempts_total",
			Help: "Total number of slice scheduling attempts.",
		},
		[]string{"tpu_type", "topology", "is_reschedule"},
	)

	SliceScheduleErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slice_schedule_errors_total",
			Help: "Total number of failed slice scheduling attempts.",
		},
		[]string{"tpu_type", "topology", "reason"},
	)

	SliceScheduleSuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slice_schedule_success_total",
			Help: "Total number of successful slice scheduling attempts.",
		},
		[]string{"tpu_type", "topology", "tpu_pod"},
	)

	SliceScheduleDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "slice_schedule_duration_seconds",
			Help:    "Time taken to schedule a slice.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"tpu_type", "topology", "success"},
	)

	SlicePreemptionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slice_preemptions_total",
			Help: "Total number of times a slice caused one or more other slice(s) to be preempted.",
		},
		[]string{"tpu_type", "topology", "tpu_pod"},
	)

	PreemptedSlicesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "preempted_slices_total",
			Help: "Total number of slices that have been preempted.",
		},
		[]string{"tpu_type", "topology", "tpu_pod"},
	)

	SliceQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "slice_queue_depth",
			Help: "Number of slices currently waiting in the priority queue.",
		},
		[]string{"tpu_type", "topology"},
	)

	ReconciliationErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slice_scheduler_reconciliation_errors_total",
			Help: "Total number of slice scheduler reconciliation errors",
		},
		[]string{"error_type"},
	)

	SliceSchedulerQueueLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "slice_scheduler_queue_latency_seconds",
			Help:    "Time a group spends in the scheduling queue.",
			Buckets: queueLatencyBuckets,
		},
		[]string{"tpu_type", "topology"},
	)

	SliceSchedulerStepDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "slice_scheduler_step_duration_seconds",
			Help:    "Time taken by individual steps of the scheduling algorithm.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"tpu_type", "topology", "step"},
	)
)

func init() {
	metrics.Registry.MustRegister(SliceScheduleAttemptsTotal)
	metrics.Registry.MustRegister(SliceScheduleErrorsTotal)
	metrics.Registry.MustRegister(SliceScheduleSuccessTotal)
	metrics.Registry.MustRegister(SliceScheduleDurationSeconds)
	metrics.Registry.MustRegister(SlicePreemptionsTotal)
	metrics.Registry.MustRegister(PreemptedSlicesTotal)
	metrics.Registry.MustRegister(SliceQueueDepth)
	metrics.Registry.MustRegister(ReconciliationErrorsTotal)

	metrics.Registry.MustRegister(SliceSchedulerQueueLatencySeconds)
	metrics.Registry.MustRegister(SliceSchedulerStepDurationSeconds)
}

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
	"testing"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// makeSlice creates a Slice with the given Ready condition and annotations.
// If reason is empty, no condition is added.
func makeSlice(reason string, status metav1.ConditionStatus, annotations map[string]string) *slicev1beta1.Slice {
	s := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-slice",
			Annotations: annotations,
		},
		Spec: slicev1beta1.SliceSpec{
			Type:     "v6e",
			Topology: "2x2x1",
		},
	}
	if reason != "" {
		s.Status.Conditions = []metav1.Condition{
			{
				Type:   ConditionTypeReady,
				Status: status,
				Reason: reason,
			},
		}
	}
	return s
}

// ---------------------------------------------------------------------------
// FindReadyCondition / GetSliceState
// ---------------------------------------------------------------------------

func TestFindReadyCondition(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		wantNil    bool
		wantReason string
	}{
		{
			name:       "no conditions",
			conditions: nil,
			wantNil:    true,
		},
		{
			name: "Ready condition present",
			conditions: []metav1.Condition{
				{Type: ConditionTypeReady, Reason: "ACTIVE", Status: metav1.ConditionTrue},
			},
			wantReason: "ACTIVE",
		},
		{
			name: "only non-Ready conditions",
			conditions: []metav1.Condition{
				{Type: "Provisioned", Reason: "Done", Status: metav1.ConditionTrue},
			},
			wantNil: true,
		},
		{
			name: "multiple conditions including Ready",
			conditions: []metav1.Condition{
				{Type: "Provisioned", Reason: "Done", Status: metav1.ConditionTrue},
				{Type: ConditionTypeReady, Reason: "FAILED", Status: metav1.ConditionFalse},
			},
			wantReason: "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := &slicev1beta1.Slice{}
			slice.Status.Conditions = tt.conditions

			got := FindReadyCondition(slice)
			if tt.wantNil {
				if got != nil {
					t.Errorf("expected nil, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil condition")
			}
			if got.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tt.wantReason)
			}
		})
	}
}

func TestGetSliceState(t *testing.T) {
	tests := []struct {
		name  string
		slice *slicev1beta1.Slice
		want  string
	}{
		{
			name:  "no conditions",
			slice: &slicev1beta1.Slice{},
			want:  "",
		},
		{
			name:  "Ready with ACTIVE",
			slice: makeSlice("ACTIVE", metav1.ConditionTrue, nil),
			want:  "ACTIVE",
		},
		{
			name:  "Ready with FAILED",
			slice: makeSlice("FAILED", metav1.ConditionFalse, nil),
			want:  "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSliceState(tt.slice); got != tt.want {
				t.Errorf("GetSliceState() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CloudPolicy.NeedsRescheduling
// ---------------------------------------------------------------------------

func TestCloudPolicy_NeedsRescheduling(t *testing.T) {
	cp := &CloudPolicy{}

	tests := []struct {
		name   string
		reason string
		status metav1.ConditionStatus
		want   bool
	}{
		// ConditionFalse triggers
		{name: "FAILED/ConditionFalse", reason: ReasonFailed, status: metav1.ConditionFalse, want: true},
		{name: "MMIGNotFound/ConditionFalse", reason: ReasonMMIGNotFound, status: metav1.ConditionFalse, want: true},
		{name: "PreflightError/ConditionFalse", reason: ReasonPreflightError, status: metav1.ConditionFalse, want: true},
		{name: "SliceCreationFailed/ConditionFalse", reason: ReasonSliceCreationFailed, status: metav1.ConditionFalse, want: true},

		// ConditionUnknown trigger
		{name: "SliceHealthProbeFailed/ConditionUnknown", reason: ReasonSliceHealthProbeFailed, status: metav1.ConditionUnknown, want: true},

		// Non-triggers: wrong status for the reason
		{name: "FAILED/ConditionTrue", reason: ReasonFailed, status: metav1.ConditionTrue, want: false},
		{name: "FAILED/ConditionUnknown", reason: ReasonFailed, status: metav1.ConditionUnknown, want: false},
		{name: "SliceHealthProbeFailed/ConditionFalse", reason: ReasonSliceHealthProbeFailed, status: metav1.ConditionFalse, want: false},
		{name: "SliceCreationFailed/ConditionTrue", reason: ReasonSliceCreationFailed, status: metav1.ConditionTrue, want: false},

		// Non-failure reasons
		{name: "ACTIVE/ConditionTrue", reason: ReasonActive, status: metav1.ConditionTrue, want: false},
		{name: "ACTIVE_DEGRADED/ConditionTrue", reason: ReasonActiveDegraded, status: metav1.ConditionTrue, want: false},
		{name: "UNHEALTHY/ConditionFalse", reason: "UNHEALTHY", status: metav1.ConditionFalse, want: false},
		{name: "ACTIVATING/ConditionUnknown", reason: "ACTIVATING", status: metav1.ConditionUnknown, want: false},

		// No condition at all
		{name: "no Ready condition", reason: "", status: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := makeSlice(tt.reason, tt.status, nil)
			if got := cp.NeedsRescheduling(slice); got != tt.want {
				t.Errorf("NeedsRescheduling() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CloudPolicy.ShouldBackoffOnFailure
// ---------------------------------------------------------------------------

func TestCloudPolicy_ShouldBackoffOnFailure(t *testing.T) {
	cp := &CloudPolicy{}

	tests := []struct {
		name        string
		reason      string
		status      metav1.ConditionStatus
		annotations map[string]string
		want        bool
	}{
		{
			name:   "SliceCreationFailed without retry annotation",
			reason: ReasonSliceCreationFailed, status: metav1.ConditionFalse,
			want: false,
		},
		{
			name:   "SliceCreationFailed with retry=true",
			reason: ReasonSliceCreationFailed, status: metav1.ConditionFalse,
			annotations: map[string]string{RetryAnnotation: EnableRetryValue},
			want:        true,
		},
		{
			name:   "SliceCreationFailed with retry=false",
			reason: ReasonSliceCreationFailed, status: metav1.ConditionFalse,
			annotations: map[string]string{RetryAnnotation: "false"},
			want:        false,
		},
		{
			name:   "SliceHealthProbeFailed always backs off",
			reason: ReasonSliceHealthProbeFailed, status: metav1.ConditionUnknown,
			want: true,
		},
		{
			name:   "FAILED does not back off",
			reason: ReasonFailed, status: metav1.ConditionFalse,
			want: false,
		},
		{
			name:   "MMIGNotFound does not back off",
			reason: ReasonMMIGNotFound, status: metav1.ConditionFalse,
			want: false,
		},
		{
			name:   "PreflightError does not back off",
			reason: ReasonPreflightError, status: metav1.ConditionFalse,
			want: false,
		},
		{
			name:   "ACTIVE does not back off",
			reason: ReasonActive, status: metav1.ConditionTrue,
			want: false,
		},
		{
			name:   "no Ready condition",
			reason: "", status: "",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := makeSlice(tt.reason, tt.status, tt.annotations)
			if got := cp.ShouldBackoffOnFailure(slice); got != tt.want {
				t.Errorf("ShouldBackoffOnFailure() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CloudPolicy.ShouldEvictPod
// ---------------------------------------------------------------------------

func TestCloudPolicy_ShouldEvictPod(t *testing.T) {
	cp := &CloudPolicy{}

	tests := []struct {
		name            string
		sliceState      string
		condStatus      metav1.ConditionStatus
		annotations     map[string]string
		partitionBroken bool
		want            bool
	}{
		// Eviction annotation takes precedence over slice state
		{
			name:        "Eviction annotation triggers eviction regardless of state",
			sliceState:  ReasonActive,
			condStatus:  metav1.ConditionTrue,
			annotations: map[string]string{interfaces.EvictPodsAnnotation: interfaces.TrueStr},
			want:        true,
		},
		{
			name:        "Eviction annotation coexists with failure state",
			sliceState:  ReasonFailed,
			condStatus:  metav1.ConditionFalse,
			annotations: map[string]string{interfaces.EvictPodsAnnotation: interfaces.TrueStr},
			want:        true,
		},
		{
			name:        "Non-true annotation value does not trigger eviction",
			sliceState:  ReasonActive,
			condStatus:  metav1.ConditionTrue,
			annotations: map[string]string{interfaces.EvictPodsAnnotation: "false"},
			want:        false,
		},

		// Always evict
		{name: "FAILED", sliceState: ReasonFailed, condStatus: metav1.ConditionFalse, want: true},
		{name: "MMIGNotFound", sliceState: ReasonMMIGNotFound, condStatus: metav1.ConditionFalse, want: true},
		{name: "PreflightError", sliceState: ReasonPreflightError, condStatus: metav1.ConditionFalse, want: true},

		// SliceCreationFailed: depends on retry annotation and partition health
		{
			name:       "SliceCreationFailed without retry",
			sliceState: ReasonSliceCreationFailed,
			condStatus: metav1.ConditionFalse,
			want:       true,
		},
		{
			name:            "SliceCreationFailed with retry=true and healthy partitions",
			sliceState:      ReasonSliceCreationFailed,
			condStatus:      metav1.ConditionFalse,
			annotations:     map[string]string{RetryAnnotation: EnableRetryValue},
			partitionBroken: false,
			want:            false,
		},
		{
			name:            "SliceCreationFailed with retry=true and broken partitions",
			sliceState:      ReasonSliceCreationFailed,
			condStatus:      metav1.ConditionFalse,
			annotations:     map[string]string{RetryAnnotation: EnableRetryValue},
			partitionBroken: true,
			want:            true,
		},
		{
			name:        "SliceCreationFailed with retry=false",
			sliceState:  ReasonSliceCreationFailed,
			condStatus:  metav1.ConditionFalse,
			annotations: map[string]string{RetryAnnotation: "false"},
			want:        true,
		},

		// Never evict
		{name: "SliceHealthProbeFailed", sliceState: ReasonSliceHealthProbeFailed, condStatus: metav1.ConditionUnknown, want: false},

		// Other states
		{name: "ACTIVE", sliceState: ReasonActive, condStatus: metav1.ConditionTrue, want: false},
		{name: "ACTIVE_DEGRADED", sliceState: ReasonActiveDegraded, condStatus: metav1.ConditionTrue, want: false},
		{name: "empty state", sliceState: "", condStatus: "", want: false},
		{name: "unknown state", sliceState: "SomethingElse", condStatus: metav1.ConditionFalse, want: false},

		// Eviction annotation tests
		{
			name:        "eviction annotation triggers eviction on ACTIVE slice",
			sliceState:  ReasonActive,
			condStatus:  metav1.ConditionTrue,
			annotations: map[string]string{interfaces.EvictPodsAnnotation: interfaces.TrueStr},
			want:        true,
		},
		{
			name:        "eviction annotation triggers eviction with no condition",
			sliceState:  "",
			condStatus:  "",
			annotations: map[string]string{interfaces.EvictPodsAnnotation: interfaces.TrueStr},
			want:        true,
		},
		{
			name:        "eviction annotation false does not trigger eviction",
			sliceState:  ReasonActive,
			condStatus:  metav1.ConditionTrue,
			annotations: map[string]string{interfaces.EvictPodsAnnotation: "false"},
			want:        false,
		},
		{
			name:        "eviction annotation coexists with FAILED state",
			sliceState:  ReasonFailed,
			condStatus:  metav1.ConditionFalse,
			annotations: map[string]string{interfaces.EvictPodsAnnotation: interfaces.TrueStr},
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := makeSlice(tt.sliceState, tt.condStatus, tt.annotations)
			if got := cp.ShouldEvictPod(slice, tt.partitionBroken); got != tt.want {
				t.Errorf("ShouldEvictPod(%q, partitionBroken=%v) = %v, want %v", tt.sliceState, tt.partitionBroken, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CloudPolicy.IsSliceReady
// ---------------------------------------------------------------------------

func TestCloudPolicy_IsSliceReady(t *testing.T) {
	cp := &CloudPolicy{}

	tests := []struct {
		state string
		want  bool
	}{
		{ReasonActive, true},
		{ReasonActiveDegraded, true},
		{ReasonFailed, false},
		{ReasonSliceCreationFailed, false},
		{ReasonSliceHealthProbeFailed, false},
		{ReasonMMIGNotFound, false},
		{ReasonPreflightError, false},
		{"ACTIVATING", false},
		{"DEACTIVATING", false},
		{"INACTIVE", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run("state="+tt.state, func(t *testing.T) {
			if got := cp.IsSliceReady(tt.state); got != tt.want {
				t.Errorf("IsSliceReady(%q) = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CloudPolicy.ShouldSkipReconcile
// ---------------------------------------------------------------------------

func TestCloudPolicy_ShouldSkipReconcile(t *testing.T) {
	cp := &CloudPolicy{}

	tests := []struct {
		name               string
		partitionIDs       []string
		annotations        map[string]string
		generation         int64
		observedGeneration int64
		wantSkip           bool
		wantDelay          bool
	}{
		{
			name:     "new unbound slice without provisioned-partition annotation",
			wantSkip: false,
		},
		{
			name:         "deactivating unbound slice with non-empty provisioned-partition annotation",
			partitionIDs: []string{},
			annotations:  map[string]string{ProvisionedPartitionAnnotation: "sub-a"},
			generation:   3,
			wantSkip:     true,
			wantDelay:    true,
		},
		{
			name:               "active bound sub-slice with provisioned-partition annotation is NOT skipped",
			partitionIDs:       []string{"sub-a"},
			annotations:        map[string]string{ProvisionedPartitionAnnotation: "sub-a"},
			generation:         2,
			observedGeneration: 2,
			wantSkip:           false,
			wantDelay:          false,
		},
		{
			name:               "bound sub-slice with provisioned-partition and generation > observedGeneration is NOT skipped",
			partitionIDs:       []string{"sub-a"},
			annotations:        map[string]string{ProvisionedPartitionAnnotation: "sub-a"},
			generation:         3,
			observedGeneration: 2,
			wantSkip:           false,
			wantDelay:          false,
		},
		{
			name:         "unbound slice with empty provisioned-partition value is NOT skipped",
			partitionIDs: []string{},
			annotations:  map[string]string{ProvisionedPartitionAnnotation: ""},
			wantSkip:     false,
			wantDelay:    false,
		},
		{
			name:               "unbound slice during slice-controller N->0 race window (annotation deleted before ObservedGeneration updated)",
			partitionIDs:       []string{},
			generation:         3,
			observedGeneration: 2,
			wantSkip:           true,
			wantDelay:          true,
		},
		{
			name:               "brand-new unbound slice before slice-controller initialization (Generation=1, ObservedGeneration=0)",
			partitionIDs:       []string{},
			generation:         1,
			observedGeneration: 0,
			wantSkip:           false,
			wantDelay:          false,
		},
		{
			name:               "brand-new unbound slice after slice-controller initialization (Generation=1, ObservedGeneration=1)",
			partitionIDs:       []string{},
			generation:         1,
			observedGeneration: 1,
			wantSkip:           false,
			wantDelay:          false,
		},
		{
			name:               "unbound slice in test/env without slice-controller (Generation=3, ObservedGeneration=0)",
			partitionIDs:       []string{},
			generation:         3,
			observedGeneration: 0,
			wantSkip:           false,
			wantDelay:          false,
		},
		{
			name:               "post-deactivation unbound slice once ObservedGeneration catches up (Generation=3, ObservedGeneration=3)",
			partitionIDs:       []string{},
			generation:         3,
			observedGeneration: 3,
			wantSkip:           false,
			wantDelay:          false,
		},
		{
			name:               "deactivating full-cube slice with nil annotations and ObservedGeneration lag",
			partitionIDs:       []string{},
			annotations:        nil,
			generation:         3,
			observedGeneration: 2,
			wantSkip:           true,
			wantDelay:          true,
		},
		{
			name:               "bound multi-cube super-slice with ObservedGeneration lag is NOT skipped",
			partitionIDs:       []string{"c1", "c2"},
			generation:         2,
			observedGeneration: 1,
			wantSkip:           false,
			wantDelay:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := makeSlice("ACTIVE", metav1.ConditionTrue, tt.annotations)
			slice.Spec.PartitionIds = tt.partitionIDs
			slice.Generation = tt.generation
			slice.Status.ObservedGeneration = tt.observedGeneration
			skip, reason, requeueAfter := cp.ShouldSkipReconcile(slice)
			if skip != tt.wantSkip {
				t.Errorf("skip = %v, want %v (reason=%q)", skip, tt.wantSkip, reason)
			}
			if tt.wantSkip && reason == "" {
				t.Error("expected non-empty reason when skip=true")
			}
			if tt.wantDelay && requeueAfter != DeactivationRequeueDelay {
				t.Errorf("requeueAfter = %v, want %v", requeueAfter, DeactivationRequeueDelay)
			}
			if !tt.wantDelay && requeueAfter != 0 {
				t.Errorf("requeueAfter = %v, want 0", requeueAfter)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CloudPolicy.IsNodeReadyForBinding
// ---------------------------------------------------------------------------

func TestCloudPolicy_IsNodeReadyForBinding(t *testing.T) {
	cp := &CloudPolicy{}
	ctx := context.Background()

	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}

	tests := []struct {
		name       string
		topology   string
		nodeLabels map[string]string
		nodeExists bool
		wantReady  bool
		wantDelay  bool
	}{
		{
			name:      "empty topology always ready",
			topology:  "",
			wantReady: true,
		},
		{
			name:     "node not found",
			topology: "2x2x1",
			// no node created
			nodeExists: false,
			wantReady:  false,
			wantDelay:  true,
		},
		{
			name:     "node with HEALTHY label",
			topology: "2x2x1",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x1-state": "HEALTHY",
				"cloud.google.com/gke-tpu-topology":              "2x2x1",
				"cloud.google.com/gke-tpu-slice":                 "test-slice",
			},
			nodeExists: true,
			wantReady:  true,
		},
		{
			name:     "node with DEGRADED label",
			topology: "2x2x1",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x1-state": "DEGRADED",
				"cloud.google.com/gke-tpu-topology":              "2x2x1",
				"cloud.google.com/gke-tpu-slice":                 "test-slice",
			},
			nodeExists: true,
			wantReady:  true,
		},
		{
			name:       "node missing health label",
			topology:   "2x2x1",
			nodeLabels: map[string]string{},
			nodeExists: true,
			wantReady:  false,
			wantDelay:  true,
		},
		{
			name:     "node with wrong label value",
			topology: "2x2x1",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x1-state": "UNHEALTHY",
			},
			nodeExists: true,
			wantReady:  false,
			wantDelay:  true,
		},
		{
			name:     "superslice topology uses cube label",
			topology: "4x4x8",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-4x4x4-state": "HEALTHY",
				"cloud.google.com/gke-tpu-topology":              "4x4x4",
				"cloud.google.com/gke-tpu-slice":                 "test-slice",
			},
			nodeExists: true,
			wantReady:  true,
		},
		{
			name:     "superslice topology without cube label",
			topology: "4x4x8",
			nodeLabels: map[string]string{
				// Label uses full topology instead of cube - should not match
				"cloud.google.com/gke-tpu-partition-4x4x8-state": "HEALTHY",
			},
			nodeExists: true,
			wantReady:  false,
			wantDelay:  true,
		},
		{
			name:     "4x4x4 topology exact match",
			topology: "4x4x4",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-4x4x4-state": "HEALTHY",
				"cloud.google.com/gke-tpu-topology":              "4x4x4",
				"cloud.google.com/gke-tpu-slice":                 "test-slice",
			},
			nodeExists: true,
			wantReady:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objs []runtime.Object
			if tt.nodeExists {
				node := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "test-node",
						Labels: tt.nodeLabels,
					},
				}
				objs = append(objs, node)
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(objs...).
				Build()

			ready, retryAfter, err := cp.IsNodeReadyForBinding(ctx, fakeClient, "test-node", tt.topology, "test-slice")

			if ready != tt.wantReady {
				t.Errorf("ready = %v, want %v", ready, tt.wantReady)
			}

			if tt.wantDelay && retryAfter == 0 {
				t.Error("expected non-zero retryAfter")
			}
			if !tt.wantDelay && retryAfter != 0 {
				t.Errorf("retryAfter = %v, want 0", retryAfter)
			}

			// For node-not-found, we expect an error
			if !tt.nodeExists && tt.topology != "" && err == nil {
				t.Error("expected error for missing node")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// healthLabelTopology (internal helper)
// ---------------------------------------------------------------------------

func TestHealthLabelTopology(t *testing.T) {
	tests := []struct {
		topology string
		want     string
	}{
		{"2x2x1", "2x2x1"},
		{"4x4x4", "4x4x4"},
		{"4x4x8", "4x4x4"},
		{"8x8x8", "4x4x4"},
		{"4x8x4", "4x4x4"},
		{"1x1x1", "1x1x1"},
		{"2x4", "2x4"},         // 2D topology, returned as-is
		{"invalid", "invalid"}, // non-numeric, returned as-is
		{"", ""},               // empty
		{"axbxc", "axbxc"},     // non-numeric parts
	}

	for _, tt := range tests {
		t.Run("topology="+tt.topology, func(t *testing.T) {
			if got := healthLabelTopology(tt.topology); got != tt.want {
				t.Errorf("healthLabelTopology(%q) = %q, want %q", tt.topology, got, tt.want)
			}
		})
	}
}

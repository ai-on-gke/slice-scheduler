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

package pod_reconciler

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParsePodIndex(t *testing.T) {
	tests := []struct {
		name    string
		pod     *corev1.Pod
		want    int
		wantErr bool
	}{
		{
			name: "StatefulSet-style pod name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-0"},
			},
			want: 0,
		},
		{
			name: "StatefulSet-style pod name with multi-digits",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "my-tpu-pod-10"},
			},
			want: 10,
		},
		{
			name: "Job completion index annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "job-abc-123",
					Annotations: map[string]string{
						JobCompletionIndexAnnotation: "5",
					},
				},
			},
			want: 5,
		},
		{
			name: "Custom worker-id annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "custom-worker",
					Annotations: map[string]string{
						WorkerIdAnnotation: "3",
					},
				},
			},
			want: 3,
		},
		{
			name: "Job completion index takes precedence over name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod-10",
					Annotations: map[string]string{
						JobCompletionIndexAnnotation: "2",
					},
				},
			},
			want: 2,
		},
		{
			name: "Invalid pod name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod"},
			},
			wantErr: true,
		},
		{
			name: "Non-numeric index in name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-abc"},
			},
			wantErr: true,
		},
		{
			name: "LWS worker index label",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-worker-pod",
					Labels: map[string]string{
						LWSWorkerIndexLabel: "5",
					},
				},
			},
			want: 5,
		},
		{
			// LWS stamps both labels; only worker-index ranks the pod within its
			// own group, so it must win. This inverts the previous precedence.
			name: "LWS worker-index takes precedence over StatefulSet pod-index",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-pod-0",
					Labels: map[string]string{
						PodIndexLabel:       "2",
						LWSWorkerIndexLabel: "7",
					},
				},
			},
			want: 7,
		},
		{
			name: "LWS worker index label with non-numeric value",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-pod-bad",
					Labels: map[string]string{
						LWSWorkerIndexLabel: "abc",
					},
				},
			},
			wantErr: true,
		},
		{
			// No policy annotation means the default LeaderWorker policy:
			// subgroup 1 holds workers 4..7, so worker 5 is its second host.
			name: "LWS subgroup size folds worker-index into subgroup-local space",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "lws-pod-5",
					Labels:      map[string]string{LWSWorkerIndexLabel: "5"},
					Annotations: map[string]string{LWSSubgroupSizeAnnotation: "4"},
				},
			},
			want: 1,
		},
		{
			name: "LWS subgroup size, mid-subgroup worker",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "lws-pod-7",
					Labels:      map[string]string{LWSWorkerIndexLabel: "7"},
					Annotations: map[string]string{LWSSubgroupSizeAnnotation: "4"},
				},
			},
			want: 3,
		},
		{
			name: "LWS subgroup size of zero is ignored, worker-index used as-is",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "lws-pod-6",
					Labels:      map[string]string{LWSWorkerIndexLabel: "6"},
					Annotations: map[string]string{LWSSubgroupSizeAnnotation: "0"},
				},
			},
			want: 6,
		},
		{
			name: "LWS non-numeric subgroup size is ignored, worker-index used as-is",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "lws-pod-6",
					Labels:      map[string]string{LWSWorkerIndexLabel: "6"},
					Annotations: map[string]string{LWSSubgroupSizeAnnotation: "xyz"},
				},
			},
			want: 6,
		},
		{
			// The leader is the first host of subgroup 0 under the default
			// policy; it must not fold to a negative index.
			name: "LWS leader with subgroup size and default policy maps to host 0",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "lws-leader-0",
					Labels:      map[string]string{LWSWorkerIndexLabel: "0"},
					Annotations: map[string]string{LWSSubgroupSizeAnnotation: "4"},
				},
			},
			want: 0,
		},
		{
			name: "LWS LeaderWorker policy, leader maps to host 0",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "lws-leader-0",
					Labels: map[string]string{LWSWorkerIndexLabel: "0"},
					Annotations: map[string]string{
						LWSSubgroupSizeAnnotation:       "2",
						LWSSubgroupPolicyTypeAnnotation: "LeaderWorker",
					},
				},
			},
			want: 0,
		},
		{
			name: "LWS LeaderWorker policy, last worker of subgroup 0",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "lws-pod-1",
					Labels: map[string]string{LWSWorkerIndexLabel: "1"},
					Annotations: map[string]string{
						LWSSubgroupSizeAnnotation:       "2",
						LWSSubgroupPolicyTypeAnnotation: "LeaderWorker",
					},
				},
			},
			want: 1,
		},
		{
			name: "LWS LeaderWorker policy, first worker of subgroup 1",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "lws-pod-2",
					Labels: map[string]string{LWSWorkerIndexLabel: "2"},
					Annotations: map[string]string{
						LWSSubgroupSizeAnnotation:       "2",
						LWSSubgroupPolicyTypeAnnotation: "LeaderWorker",
					},
				},
			},
			want: 0,
		},
		{
			name: "LWS LeaderExcluded policy, leader maps to index 0",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "lws-leader-0",
					Labels: map[string]string{LWSWorkerIndexLabel: "0"},
					Annotations: map[string]string{
						LWSSubgroupSizeAnnotation:       "4",
						LWSSubgroupPolicyTypeAnnotation: LWSLeaderExcludedPolicy,
					},
				},
			},
			want: 0,
		},
		{
			name: "LWS LeaderExcluded policy, first worker maps to host 0",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "lws-pod-1",
					Labels: map[string]string{LWSWorkerIndexLabel: "1"},
					Annotations: map[string]string{
						LWSSubgroupSizeAnnotation:       "4",
						LWSSubgroupPolicyTypeAnnotation: LWSLeaderExcludedPolicy,
					},
				},
			},
			want: 0,
		},
		{
			// Subgroup 1 holds workers 5..8, so worker 5 is its first host.
			name: "LWS LeaderExcluded policy, first worker of subgroup 1",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "lws-pod-5",
					Labels: map[string]string{LWSWorkerIndexLabel: "5"},
					Annotations: map[string]string{
						LWSSubgroupSizeAnnotation:       "4",
						LWSSubgroupPolicyTypeAnnotation: LWSLeaderExcludedPolicy,
					},
				},
			},
			want: 0,
		},
		{
			name: "LWS LeaderExcluded policy, last worker of subgroup 1",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "lws-pod-8",
					Labels: map[string]string{LWSWorkerIndexLabel: "8"},
					Annotations: map[string]string{
						LWSSubgroupSizeAnnotation:       "4",
						LWSSubgroupPolicyTypeAnnotation: LWSLeaderExcludedPolicy,
					},
				},
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePodIndex(tt.pod)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePodIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parsePodIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

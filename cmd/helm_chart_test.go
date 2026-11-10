/*
Copyright 2026.

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

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHelmChartTolerationFlags(t *testing.T) {
	helmBin, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm binary not in PATH; skipping chart template test")
	}

	chartDir := filepath.Join("..", "charts", "slice-scheduler")
	cases := []struct {
		name       string
		extraArgs  []string
		valuesYAML string
		wantFlags  []string
		wantErr    string
	}{
		{
			name: "defaults from values.yaml",
			wantFlags: []string{
				`"--enable-taint-filtering=false"`,
				`"--default-not-ready-toleration-seconds=300"`,
				`"--default-unreachable-toleration-seconds=300"`,
			},
		},
		{
			name: "zero evicts immediately (not treated as empty by Helm default)",
			extraArgs: []string{
				"--set", "enableTaintFiltering=true",
				"--set", "defaultNotReadyTolerationSeconds=0",
				"--set", "defaultUnreachableTolerationSeconds=0",
			},
			wantFlags: []string{
				`"--enable-taint-filtering=true"`,
				`"--default-not-ready-toleration-seconds=0"`,
				`"--default-unreachable-toleration-seconds=0"`,
			},
		},
		{
			name: "negative tolerates forever",
			extraArgs: []string{
				"--set", "defaultNotReadyTolerationSeconds=-1",
				"--set", "defaultUnreachableTolerationSeconds=-1",
			},
			wantFlags: []string{
				`"--default-not-ready-toleration-seconds=-1"`,
				`"--default-unreachable-toleration-seconds=-1"`,
			},
		},
		{
			name: "explicit null falls back to 300",
			extraArgs: []string{
				"--set", "defaultNotReadyTolerationSeconds=null",
				"--set", "defaultUnreachableTolerationSeconds=null",
			},
			wantFlags: []string{
				`"--default-not-ready-toleration-seconds=300"`,
				`"--default-unreachable-toleration-seconds=300"`,
			},
		},
		{
			name: "values file with null falls back to 300",
			valuesYAML: "defaultNotReadyTolerationSeconds: null\n" +
				"defaultUnreachableTolerationSeconds: ~\n",
			wantFlags: []string{
				`"--default-not-ready-toleration-seconds=300"`,
				`"--default-unreachable-toleration-seconds=300"`,
			},
		},
		{
			name: "values file with >=1e6 seconds renders integer instead of scientific notation",
			valuesYAML: "defaultNotReadyTolerationSeconds: 1000000\n" +
				"defaultUnreachableTolerationSeconds: 2592000\n",
			wantFlags: []string{
				`"--default-not-ready-toleration-seconds=1000000"`,
				`"--default-unreachable-toleration-seconds=2592000"`,
			},
		},
		{
			name: "values file with zero and negative numbers renders integer",
			valuesYAML: "defaultNotReadyTolerationSeconds: 0\n" +
				"defaultUnreachableTolerationSeconds: -1\n",
			wantFlags: []string{
				`"--default-not-ready-toleration-seconds=0"`,
				`"--default-unreachable-toleration-seconds=-1"`,
			},
		},
		{
			name:       "values file with string number renders integer",
			valuesYAML: "defaultUnreachableTolerationSeconds: \"600\"\n",
			wantFlags:  []string{`"--default-unreachable-toleration-seconds=600"`},
		},
		{
			name: "max and min int64 via --set render without float precision loss",
			extraArgs: []string{
				"--set", "defaultNotReadyTolerationSeconds=9223372036854775807",
				"--set", "defaultUnreachableTolerationSeconds=-9223372036854775808",
			},
			wantFlags: []string{
				`"--default-not-ready-toleration-seconds=9223372036854775807"`,
				`"--default-unreachable-toleration-seconds=-9223372036854775808"`,
			},
		},
		{
			name:       "duration string 5m fails render instead of coercing to 0",
			valuesYAML: "defaultNotReadyTolerationSeconds: 5m\n",
			wantErr:    "defaultNotReadyTolerationSeconds must be a 64-bit integer number of seconds",
		},
		{
			name:      "empty --set value fails render instead of coercing to 0",
			extraArgs: []string{"--set", "defaultUnreachableTolerationSeconds="},
			wantErr:   "defaultUnreachableTolerationSeconds must be a 64-bit integer number of seconds",
		},
		{
			name:       "fractional float 600.9 fails render instead of silently truncating",
			valuesYAML: "defaultNotReadyTolerationSeconds: 600.9\n",
			wantErr:    "defaultNotReadyTolerationSeconds must be a 64-bit integer number of seconds",
		},
		{
			name:       "boolean value fails render instead of coercing to 1 or 0",
			valuesYAML: "defaultUnreachableTolerationSeconds: true\n",
			wantErr:    "defaultUnreachableTolerationSeconds must be a 64-bit integer number of seconds",
		},
		{
			name:       "out-of-range float 1e20 fails render instead of overflowing int64",
			valuesYAML: "defaultNotReadyTolerationSeconds: 1e20\n",
			wantErr:    "defaultNotReadyTolerationSeconds must be a 64-bit integer number of seconds",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"template", "test", chartDir}, tc.extraArgs...)
			if tc.valuesYAML != "" {
				vf := filepath.Join(t.TempDir(), "values.yaml")
				if err := os.WriteFile(vf, []byte(tc.valuesYAML), 0o600); err != nil {
					t.Fatalf("write temp values.yaml: %v", err)
				}
				args = append(args, "-f", vf)
			}
			out, err := exec.Command(helmBin, args...).CombinedOutput()
			rendered := string(out)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("helm template succeeded unexpectedly; wanted error %q, rendered:\n%s", tc.wantErr, rendered)
				}
				if !strings.Contains(rendered, tc.wantErr) {
					t.Errorf("helm template error output missing %q:\n%s", tc.wantErr, rendered)
				}
				return
			}
			if err != nil {
				t.Fatalf("helm template failed: %v\n%s", err, rendered)
			}
			for _, want := range tc.wantFlags {
				if !strings.Contains(rendered, want) {
					t.Errorf("rendered deployment missing %s:\n%s", want, rendered)
				}
			}
		})
	}
}

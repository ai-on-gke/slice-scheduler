//go:build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ai-on-gke/slice-scheduler/test/utils"
)

const namespace = "slice-scheduler-system"

var _ = Describe("controller", Ordered, func() {
	BeforeAll(func() {
		By("labeling Kind control-plane node for controller scheduling")
		cmd := exec.Command("kubectl", "label", "node", "slice-scheduler-test-e2e-control-plane",
			"cloud.google.com/gke-nodepool=default-pool", "--overwrite")
		_, _ = utils.Run(cmd)
		// Remove control-plane taint so the controller manager can be scheduled
		cmd = exec.Command("kubectl", "taint", "nodes", "slice-scheduler-test-e2e-control-plane",
			"node-role.kubernetes.io/control-plane:NoSchedule-")
		_, _ = utils.Run(cmd)

		By("installing prometheus operator")
		Expect(utils.InstallPrometheusOperator()).To(Succeed())

		By("installing the cert-manager")
		Expect(utils.InstallCertManager()).To(Succeed())

		By("creating manager namespace")
		cmd = exec.Command("kubectl", "create", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	AfterAll(func() {
		By("uninstalling the Prometheus manager bundle")
		utils.UninstallPrometheusOperator()

		By("uninstalling the cert-manager bundle")
		utils.UninstallCertManager()

		By("removing manager namespace")
		cmd := exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	Context("Operator", func() {
		It("should run successfully", func() {
			var controllerPodName string
			var err error

			// projectimage stores the name of the image used in the example
			var projectimage = "example.com/slice-scheduler:v0.0.1"

			By("building the manager(Operator) image")
			cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectimage))
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("loading the the manager(Operator) image on Kind")
			err = utils.LoadImageToKindClusterWithName(projectimage)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("installing CRDs")
			cmd = exec.Command("make", "install")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("installing PartitionTree CRD (owned by partition-tree controller, not included in make install)")
			cmd = exec.Command("kubectl", "apply", "-f", "config/crd/bases/partition.tpu.google.com_partitiontrees.yaml")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("installing JobSet CRD")
			cmd = exec.Command("kubectl", "apply", "--server-side", "-f",
				"https://github.com/kubernetes-sigs/jobset/releases/download/v0.7.0/manifests.yaml")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("patching JobSet controller to fix deprecated kube-rbac-proxy image")
			cmd = exec.Command("kubectl", "set", "image",
				"deployment/jobset-controller-manager",
				"kube-rbac-proxy=registry.k8s.io/kubebuilder/kube-rbac-proxy:v0.16.0",
				"-n", "jobset-system")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("waiting for JobSet webhook to be ready")
			cmd = exec.Command("kubectl", "wait", "--for=condition=Available",
				"deployment/jobset-controller-manager", "-n", "jobset-system", "--timeout", "120s")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("setting up PartitionTree and node labels")
			Expect(setupPartitionTree()).To(Succeed())

			By("deploying the controller-manager")
			cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectimage))
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("enabling pod reconciler for pod-to-node binding")
			cmd = exec.Command("kubectl", "patch", "deployment",
				"slice-scheduler-controller-manager", "-n", namespace,
				"--type", "json",
				"-p", `[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--enable-pod-reconciler"}]`)
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func() error {
				// Get pod name

				cmd = exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				podNames := utils.GetNonEmptyLines(string(podOutput))
				if len(podNames) != 1 {
					return fmt.Errorf("expect 1 controller pods running, but got %d", len(podNames))
				}
				controllerPodName = podNames[0]
				ExpectWithOffset(2, controllerPodName).Should(ContainSubstring("controller-manager"))

				// Validate pod status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				status, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				if string(status) != "Running" {
					return fmt.Errorf("controller pod in %s status", status)
				}
				return nil
			}
			EventuallyWithOffset(1, verifyControllerUp, time.Minute, time.Second).Should(Succeed())

		})
	})

	Context("Slice Workload Scheduling", func() {
		It("should successfully schedule slice and workload", func() {
			DeferCleanup(func() {
				By("Cleaning up resources")
				cmd := exec.Command("kubectl", "delete", "jobset", "jobset-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "slice", "slice-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "label", "nodes", "--all", "cloud.google.com/gke-tpu-slice-", "--overwrite")
				_, _ = utils.Run(cmd)
			})

			By("Applying JobSet manifest")
			cmd := exec.Command("kubectl", "apply", "-f", "test/yaml/jobset-1cube.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying JobSet pods are Pending")
			verifyPodsPending := func() error {
				cmd = exec.Command("kubectl", "get", "pods", "-l", "jobset.sigs.k8s.io/jobset-name=jobset-1cube", "-o", "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) == 0 {
					return fmt.Errorf("no pods found")
				}
				for _, p := range phases {
					if p != "Pending" {
						return fmt.Errorf("expected Pending pod, got: %s", p)
					}
				}
				return nil
			}
			Eventually(verifyPodsPending, time.Minute, time.Second).Should(Succeed())

			By("Applying Slice manifest")
			cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Slice CR is created and scheduled")
			verifySliceScheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) == "" || string(out) == "[]" {
					return fmt.Errorf("slice partitionIds are empty")
				}
				return nil
			}
			Eventually(verifySliceScheduled, time.Minute, time.Second).Should(Succeed())

			By("Labeling KWOK nodes with slice name (simulates GKE slice-provisioner)")
			cmd = exec.Command("kubectl", "label", "nodes", "-l", "type=kwok",
				"cloud.google.com/gke-tpu-slice=slice-1cube", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying JobSet pods are Running")
			verifyPodsRunning := func() error {
				cmd = exec.Command("kubectl", "get", "pods", "-l", "jobset.sigs.k8s.io/jobset-name=jobset-1cube", "-o", "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) == 0 {
					return fmt.Errorf("no pods found")
				}
				for _, p := range phases {
					if p != "Running" {
						return fmt.Errorf("expected Running pod, got: %s", p)
					}
				}
				return nil
			}
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	Context("Slice Recovery and Recreation", func() {
		It("should recover from FAILED state (Scenario A)", func() {
			DeferCleanup(func() {
				By("Cleaning up resources")
				cmd := exec.Command("kubectl", "delete", "jobset", "jobset-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "slice", "slice-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "label", "nodes", "--all", "cloud.google.com/gke-tpu-slice-", "--overwrite")
				_, _ = utils.Run(cmd)
			})

			By("Applying Slice manifest")
			cmd := exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Slice CR is created and scheduled")
			verifySliceScheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) == "" || string(out) == "[]" {
					return fmt.Errorf("slice partitionIds are empty")
				}
				return nil
			}
			Eventually(verifySliceScheduled, time.Minute, time.Second).Should(Succeed())

			By("Applying JobSet manifest")
			cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/jobset-1cube.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Labeling KWOK nodes with slice name (simulates GKE slice-provisioner)")
			cmd = exec.Command("kubectl", "label", "nodes", "-l", "type=kwok",
				"cloud.google.com/gke-tpu-slice=slice-1cube", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying JobSet pods are Running")
			verifyPodsRunning := func() error {
				cmd = exec.Command("kubectl", "get", "pods", "-l", "jobset.sigs.k8s.io/jobset-name=jobset-1cube", "-o", "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) == 0 {
					return fmt.Errorf("no pods found")
				}
				for _, p := range phases {
					if p != "Running" {
						return fmt.Errorf("expected Running pod, got: %s", p)
					}
				}
				return nil
			}
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())

			cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
			initialPartitionsOut, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			initialPartitions := string(initialPartitionsOut)

			By("Simulating Slice failure by patching status to FAILED")
			failedStatusPatch := `{"status":{"conditions":[{"type":"Ready","status":"False",` +
				`"reason":"FAILED","message":"Simulated failure",` +
				`"lastTransitionTime":"2026-01-01T00:00:00Z"}]}}`
			cmd = exec.Command("kubectl", "patch", "slice", "slice-1cube",
				"--subresource", "status", "--type", "merge", "-p", failedStatusPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying PodReconciler unbinds the FAILED Slice (N->0)")
			Eventually(func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) != "" && string(out) != "[]" {
					return fmt.Errorf("expected empty partitionIds after FAILED unbind, got %s", string(out))
				}
				return nil
			}, 30*time.Second, time.Second).Should(Succeed())

			By("Simulating slice-controller completing N->0 teardown (Ready=SliceNotCreated)")
			resetStatusPatch := `{"status":{"conditions":[{"type":"Ready","status":"Unknown",` +
				`"reason":"SliceNotCreated","message":"Deactivated",` +
				`"lastTransitionTime":"2026-01-01T00:00:10Z"}]}}`
			cmd = exec.Command("kubectl", "patch", "slice", "slice-1cube",
				"--subresource", "status", "--type", "merge", "-p", resetStatusPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying scheduler reschedules the Slice (0->M)")
			verifySliceRescheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				partitions := string(out)
				if partitions == "" || partitions == "[]" {
					return fmt.Errorf("slice partitionIds are empty after rescheduling")
				}
				_ = initialPartitions // Partitions may be same or different depending on availability
				return nil
			}
			Eventually(verifySliceRescheduled, time.Minute, time.Second).Should(Succeed())

			By("Verifying associated JobSet pods are Running again")
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should recover from manual Slice deletion (Scenario B)", func() {
			DeferCleanup(func() {
				By("Cleaning up resources")
				cmd := exec.Command("kubectl", "delete", "jobset", "jobset-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "slice", "slice-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "label", "nodes", "--all", "cloud.google.com/gke-tpu-slice-", "--overwrite")
				_, _ = utils.Run(cmd)
			})

			By("Applying Slice manifest")
			cmd := exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Slice CR is created and scheduled")
			verifySliceScheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) == "" || string(out) == "[]" {
					return fmt.Errorf("slice partitionIds are empty")
				}
				return nil
			}
			Eventually(verifySliceScheduled, time.Minute, time.Second).Should(Succeed())

			By("Applying JobSet manifest")
			cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/jobset-1cube.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Labeling KWOK nodes with slice name (simulates GKE slice-provisioner)")
			cmd = exec.Command("kubectl", "label", "nodes", "-l", "type=kwok",
				"cloud.google.com/gke-tpu-slice=slice-1cube", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying JobSet pods are Running")
			verifyPodsRunning := func() error {
				cmd = exec.Command("kubectl", "get", "pods", "-l", "jobset.sigs.k8s.io/jobset-name=jobset-1cube", "-o", "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) == 0 {
					return fmt.Errorf("no pods found")
				}
				for _, p := range phases {
					if p != "Running" {
						return fmt.Errorf("expected Running pod, got: %s", p)
					}
				}
				return nil
			}
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("Manually deleting the Slice object")
			cmd = exec.Command("kubectl", "delete", "slice", "slice-1cube")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the Slice object is deleted")
			verifySliceDeleted := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube")
				_, err := utils.Run(cmd)
				if err == nil {
					return fmt.Errorf("slice still exists")
				}
				return nil
			}
			Eventually(verifySliceDeleted, 30*time.Second, time.Second).Should(Succeed())

			By("Re-applying the Slice manifest to trigger re-scheduling")
			cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Slice CR is re-scheduled")
			verifySliceRescheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) == "" || string(out) == "[]" {
					return fmt.Errorf("slice partitionIds are empty")
				}
				return nil
			}
			Eventually(verifySliceRescheduled, time.Minute, time.Second).Should(Succeed())

			By("Labeling KWOK nodes with slice name (simulates GKE slice-provisioner)")
			cmd = exec.Command("kubectl", "label", "nodes", "-l", "type=kwok",
				"cloud.google.com/gke-tpu-slice=slice-1cube", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying associated JobSet pods are Running again")
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	Context("Subslicing Taint Eviction and Deactivation Gate", func() {
		waitControllerReady := func() {
			cmd := exec.Command("kubectl", "rollout", "status", "deployment/slice-scheduler-controller-manager",
				"-n", namespace, "--timeout", "90s")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			podPhaseTmpl := "go-template={{ range .items }}" +
				"{{ if not .metadata.deletionTimestamp }}{{ .status.phase }}{{ \"\\n\" }}{{ end }}" +
				"{{ end }}"
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pods", "-l", "control-plane=controller-manager",
					"-n", namespace, "-o", podPhaseTmpl)
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) != 1 || phases[0] != "Running" {
					return fmt.Errorf("expected 1 Running controller pod, got %v", phases)
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())
		}

		var origArgs []string
		restoreSubsliceEnv := func(restoreArgs bool) {
			for _, p := range []string{"subslice-worker-0", "gang-sub-0-worker-0", "gang-sub-1-worker-0"} {
				cmd := exec.Command("kubectl", "delete", "pod", p,
					"--ignore-not-found", "--force", "--grace-period=0")
				_, _ = utils.Run(cmd)
			}
			for _, sl := range []string{"slice-subslice", "gang-sub-0", "gang-sub-1"} {
				cmd := exec.Command("kubectl", "delete", "slice", sl, "--ignore-not-found")
				_, _ = utils.Run(cmd)
			}
			for i := 1; i <= 12; i++ {
				nodeName := fmt.Sprintf("kwok-node-%d", i)
				cmd := exec.Command("kubectl", "label", "node", nodeName,
					"cloud.google.com/gke-tpu-topology=4x4x4",
					"cloud.google.com/gke-tpu-slice-",
					"cloud.google.com/gke-tpu-partition-2x2x4-state-",
					"--overwrite")
				_, _ = utils.Run(cmd)
			}
			var allNodes []string
			for i := 1; i <= 16; i++ {
				allNodes = append(allNodes, fmt.Sprintf(`"kwok-node-%d"`, i))
			}
			restorePT := fmt.Sprintf(
				`{"status":{"root":{"partitionId":"np-t1","topology":"4x4x4",`+
					`"state":"Healthy","taints":null,"childPartitions":null,"nodeNames":[%s]}}}`,
				strings.Join(allNodes, ","))
			cmd := exec.Command("kubectl", "patch", "partitiontree", "pt-node-pool-1",
				"--subresource", "status", "--type", "merge", "-p", restorePT)
			_, _ = utils.Run(cmd)

			if restoreArgs && len(origArgs) > 0 {
				if _, err := utils.Run(exec.Command("kubectl", "get", "deployment",
					"slice-scheduler-controller-manager", "-n", namespace)); err == nil {
					argsJSON, mErr := json.Marshal(origArgs)
					Expect(mErr).NotTo(HaveOccurred())
					restorePatch := fmt.Sprintf(
						`[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":%s}]`,
						string(argsJSON))
					cmd = exec.Command("kubectl", "patch", "deployment", "slice-scheduler-controller-manager",
						"-n", namespace, "--type", "json", "-p", restorePatch)
					_, _ = utils.Run(cmd)
					waitControllerReady()
				}
			}
		}

		BeforeAll(func() {
			By("Snapshotting existing controller-manager args and adding taint-filtering flags")
			rawArgs, err := utils.Run(exec.Command("kubectl", "get", "deployment",
				"slice-scheduler-controller-manager", "-n", namespace,
				"-o", "jsonpath={.spec.template.spec.containers[0].args}"))
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(rawArgs, &origArgs)).To(Succeed())
			Expect(origArgs).To(ContainElement("--metrics-bind-address=:8443"))

			newArgs := append(slices.Clone(origArgs),
				"--enable-taint-filtering",
				"--reschedule-unhealthy-delay=2s",
			)
			argsJSON, err := json.Marshal(newArgs)
			Expect(err).NotTo(HaveOccurred())
			taintArgsPatch := fmt.Sprintf(
				`[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":%s}]`,
				string(argsJSON))
			cmd := exec.Command("kubectl", "patch", "deployment", "slice-scheduler-controller-manager",
				"-n", namespace, "--type", "json", "-p", taintArgsPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			waitControllerReady()
		})

		AfterAll(func() {
			By("Restoring default controller-manager flags and PartitionTree after subslicing suite")
			restoreSubsliceEnv(true)
		})

		It("should evict an active sub-slice on NoExecute and gate 0->M until deactivated", func() {
			DeferCleanup(func() {
				By("Cleaning up single-subslice test resources")
				restoreSubsliceEnv(false)
			})

			By("Configuring PartitionTree with 2x2x4 child sub-slices sub-a (Healthy) and sub-b (NoSchedule)")
			initialSubslicePT := `{"status":{"acceleratorType":"tpu7x","root":{` +
				`"partitionId":"np-t1","topology":"4x4x4","state":"Healthy","nodeNames":null,` +
				`"childPartitions":[{"partitionId":"sub-a","topology":"2x2x4","state":"Healthy",` +
				`"nodeNames":["kwok-node-1","kwok-node-2","kwok-node-3","kwok-node-4"]},` +
				`{"partitionId":"sub-b","topology":"2x2x4","state":"Healthy",` +
				`"taints":[{"key":"cloud.google.com/partition-state","value":"MAINTENANCE","effect":"NoSchedule"}],` +
				`"nodeNames":["kwok-node-5","kwok-node-6","kwok-node-7","kwok-node-8"]}]}}}`
			cmd := exec.Command("kubectl", "patch", "partitiontree", "pt-node-pool-1",
				"--subresource", "status", "--type", "merge", "-p", initialSubslicePT)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Creating 2x2x4 Slice CR slice-subslice")
			sliceYAML := `{
				"apiVersion": "accelerator.gke.io/v1beta1",
				"kind": "Slice",
				"metadata": {
					"name": "slice-subslice",
					"annotations": {
						"cloud.google.com/managed-by": "slice-scheduler"
					}
				},
				"spec": {
					"type": "tpu7x",
					"topology": "2x2x4",
					"partitionIds": []
				}
			}`
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(sliceYAML)
			dir, _ := utils.GetProjectDir()
			cmd.Dir = dir
			out, err := cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(out))

			By("Verifying slice-subslice is scheduled onto sub-a")
			Eventually(func() (string, error) {
				cmd := exec.Command("kubectl", "get", "slice", "slice-subslice", "-o", "jsonpath={.spec.partitionIds}")
				b, err := utils.Run(cmd)
				return string(b), err
			}, 30*time.Second, time.Second).Should(Equal(`["sub-a"]`))

			By("Simulating slice-controller ACTIVATE on sub-a (provisioned-partition=sub-a, observedGeneration)")
			cmd = exec.Command("kubectl", "annotate", "slice", "slice-subslice",
				"accelerator.gke.io/provisioned-partition=sub-a", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "get", "slice", "slice-subslice", "-o", "jsonpath={.metadata.generation}")
			genBytes, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			activeGen := strings.TrimSpace(string(genBytes))

			activeStatusPatch := fmt.Sprintf(
				`{"status":{"observedGeneration":%s,"conditions":[{"type":"Ready","status":"True",`+
					`"reason":"ACTIVE","message":"Sub-slice sub-a active",`+
					`"lastTransitionTime":"2026-01-01T00:00:00Z"}]}}`,
				activeGen)
			cmd = exec.Command("kubectl", "patch", "slice", "slice-subslice",
				"--subresource", "status", "--type", "merge", "-p", activeStatusPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			for i := 1; i <= 4; i++ {
				nodeName := fmt.Sprintf("kwok-node-%d", i)
				cmd = exec.Command("kubectl", "label", "node", nodeName,
					"cloud.google.com/gke-tpu-topology=2x2x4",
					"cloud.google.com/gke-tpu-slice=slice-subslice",
					"cloud.google.com/gke-tpu-partition-2x2x4-state=HEALTHY",
					"--overwrite")
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			}

			By("Creating a slice-scheduler managed Pod and verifying PodReconciler binds it to kwok-node-1")
			podJSON := `{
				"apiVersion": "v1",
				"kind": "Pod",
				"metadata": {
					"name": "subslice-worker-0",
					"namespace": "default",
					"labels": {
						"cloud.google.com/gke-tpu-slice": "slice-subslice"
					},
					"annotations": {
						"batch.kubernetes.io/job-completion-index": "0"
					}
				},
				"spec": {
					"schedulerName": "slice-scheduler",
					"restartPolicy": "Never",
					"containers": [
						{
							"name": "worker",
							"image": "busybox:1.36",
							"command": ["sleep", "3600"]
						}
					]
				}
			}`
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(podJSON)
			cmd.Dir = dir
			out, err = cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(out))

			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pod", "subslice-worker-0", "-n", "default",
					"-o", "jsonpath={.spec.nodeName}:{.status.phase}")
				b, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(b) != "kwok-node-1:Running" {
					return fmt.Errorf("expected kwok-node-1:Running, got %s", string(b))
				}
				return nil
			}, 30*time.Second, time.Second).Should(Succeed())

			By("Applying NoExecute taint on sub-a and clearing NoSchedule taint on sibling sub-b")
			taintedPT := `{"status":{"acceleratorType":"tpu7x","root":{` +
				`"partitionId":"np-t1","topology":"4x4x4","state":"Healthy","nodeNames":null,` +
				`"childPartitions":[{"partitionId":"sub-a","topology":"2x2x4","state":"Unhealthy",` +
				`"taints":[{"key":"cloud.google.com/partition-state","value":"UNHEALTHY","effect":"NoExecute"}],` +
				`"nodeNames":["kwok-node-1","kwok-node-2","kwok-node-3","kwok-node-4"]},` +
				`{"partitionId":"sub-b","topology":"2x2x4","state":"Healthy","taints":[],` +
				`"nodeNames":["kwok-node-5","kwok-node-6","kwok-node-7","kwok-node-8"]}]}}}`
			cmd = exec.Command("kubectl", "patch", "partitiontree", "pt-node-pool-1",
				"--subresource", "status", "--type", "merge", "-p", taintedPT)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying active sub-slice is marked, drained, and spec.partitionIds cleared")
			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "slice", "slice-subslice", "-o", "jsonpath={.spec.partitionIds}")
				b, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(b) != "" && string(b) != "[]" {
					return fmt.Errorf("expected empty partitionIds after NoExecute eviction, still got %s", string(b))
				}
				return nil
			}, 25*time.Second, time.Second).Should(Succeed())

			By("Verifying Gate Phase 1: slice NOT bound to sub-b while provisioned-partition=sub-a remains")
			Consistently(func() (string, error) {
				cmd := exec.Command("kubectl", "get", "slice", "slice-subslice", "-o", "jsonpath={.spec.partitionIds}")
				b, err := utils.Run(cmd)
				if err != nil {
					return "", err
				}
				if string(b) == "" {
					return "[]", nil
				}
				return string(b), nil
			}, 8*time.Second, time.Second).Should(Equal("[]"))

			By("Verifying Gate Phase 2 (Race Window A): observedGeneration lag still gates 0->M scheduling")
			cmd = exec.Command("kubectl", "annotate", "slice", "slice-subslice",
				"accelerator.gke.io/provisioned-partition-")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			Consistently(func() (string, error) {
				cmd := exec.Command("kubectl", "get", "slice", "slice-subslice", "-o", "jsonpath={.spec.partitionIds}")
				b, err := utils.Run(cmd)
				if err != nil {
					return "", err
				}
				if string(b) == "" {
					return "[]", nil
				}
				return string(b), nil
			}, 7*time.Second, time.Second).Should(Equal("[]"))

			By("Simulating slice-controller completing N->0 deactivation (observedGeneration == generation)")
			cmd = exec.Command("kubectl", "get", "slice", "slice-subslice", "-o", "jsonpath={.metadata.generation}")
			deactGenBytes, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			deactGen := strings.TrimSpace(string(deactGenBytes))

			deactDonePatch := fmt.Sprintf(
				`{"status":{"observedGeneration":%s,"conditions":[{"type":"Ready","status":"Unknown",`+
					`"reason":"SliceNotCreated","message":"Deactivated",`+
					`"lastTransitionTime":"2026-01-01T00:01:00Z"}]}}`,
				deactGen)
			cmd = exec.Command("kubectl", "patch", "slice", "slice-subslice",
				"--subresource", "status", "--type", "merge", "-p", deactDonePatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying slice-scheduler immediately schedules slice-subslice onto healthy sibling sub-b")
			Eventually(func() (string, error) {
				cmd := exec.Command("kubectl", "get", "slice", "slice-subslice", "-o", "jsonpath={.spec.partitionIds}")
				b, err := utils.Run(cmd)
				return string(b), err
			}, 25*time.Second, time.Second).Should(Equal(`["sub-b"]`))

			By("Simulating slice-controller ACTIVATE on sub-b and verifying replacement Pod binds to kwok-node-5")
			cmd = exec.Command("kubectl", "annotate", "slice", "slice-subslice",
				"accelerator.gke.io/provisioned-partition=sub-b", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "get", "slice", "slice-subslice", "-o", "jsonpath={.metadata.generation}")
			subBGenBytes, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			subBGen := strings.TrimSpace(string(subBGenBytes))

			subBActivePatch := fmt.Sprintf(
				`{"status":{"observedGeneration":%s,"conditions":[{"type":"Ready","status":"True",`+
					`"reason":"ACTIVE","message":"Sub-slice sub-b active",`+
					`"lastTransitionTime":"2026-01-01T00:02:00Z"}]}}`,
				subBGen)
			cmd = exec.Command("kubectl", "patch", "slice", "slice-subslice",
				"--subresource", "status", "--type", "merge", "-p", subBActivePatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			for i := 5; i <= 8; i++ {
				nodeName := fmt.Sprintf("kwok-node-%d", i)
				cmd = exec.Command("kubectl", "label", "node", nodeName,
					"cloud.google.com/gke-tpu-topology=2x2x4",
					"cloud.google.com/gke-tpu-slice=slice-subslice",
					"cloud.google.com/gke-tpu-partition-2x2x4-state=HEALTHY",
					"--overwrite")
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			}

			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(podJSON)
			cmd.Dir = dir
			out, err = cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(out))

			Eventually(func() error {
				cmd := exec.Command("kubectl", "get", "pod", "subslice-worker-0", "-n", "default",
					"-o", "jsonpath={.spec.nodeName}:{.status.phase}")
				b, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(b) != "kwok-node-5:Running" {
					return fmt.Errorf("expected kwok-node-5:Running, got %s", string(b))
				}
				return nil
			}, 30*time.Second, time.Second).Should(Succeed())
		})

		It("should migrate only the tainted sub-slice of a 2-slice gang without aborting on healthy member", func() {
			DeferCleanup(func() {
				By("Cleaning up 2-slice gang test resources")
				restoreSubsliceEnv(false)
			})

			By("Configuring PartitionTree with sub-a (Healthy), sub-b (Healthy), and sub-c (NoSchedule)")
			gangInitialPT := `{"status":{"acceleratorType":"tpu7x","root":{` +
				`"partitionId":"np-t1","topology":"4x4x4","state":"Healthy","nodeNames":null,` +
				`"childPartitions":[{"partitionId":"sub-a","topology":"2x2x4","state":"Healthy",` +
				`"nodeNames":["kwok-node-1","kwok-node-2","kwok-node-3","kwok-node-4"]},` +
				`{"partitionId":"sub-b","topology":"2x2x4","state":"Healthy",` +
				`"nodeNames":["kwok-node-5","kwok-node-6","kwok-node-7","kwok-node-8"]},` +
				`{"partitionId":"sub-c","topology":"2x2x4","state":"Healthy",` +
				`"taints":[{"key":"cloud.google.com/partition-state","value":"MAINTENANCE","effect":"NoSchedule"}],` +
				`"nodeNames":["kwok-node-9","kwok-node-10","kwok-node-11","kwok-node-12"]}]}}}`
			cmd := exec.Command("kubectl", "patch", "partitiontree", "pt-node-pool-1",
				"--subresource", "status", "--type", "merge", "-p", gangInitialPT)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			dir, _ := utils.GetProjectDir()
			for _, name := range []string{"gang-sub-0", "gang-sub-1"} {
				gangSliceJSON := fmt.Sprintf(`{
					"apiVersion": "accelerator.gke.io/v1beta1",
					"kind": "Slice",
					"metadata": {
						"name": "%s",
						"annotations": {
							"cloud.google.com/managed-by": "slice-scheduler",
							"cloud.google.com/slice-group-name": "subslice-gang-group",
							"cloud.google.com/num-slices": "2"
						}
					},
					"spec": {
						"type": "tpu7x",
						"topology": "2x2x4",
						"partitionIds": []
					}
				}`, name)
				cmd = exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(gangSliceJSON)
				cmd.Dir = dir
				out, err := cmd.CombinedOutput()
				Expect(err).NotTo(HaveOccurred(), string(out))
			}

			By("Verifying both gang members are scheduled onto sub-a and sub-b")
			var p0, p1 string
			Eventually(func() error {
				b0, err := utils.Run(exec.Command("kubectl", "get", "slice", "gang-sub-0",
					"-o", "jsonpath={.spec.partitionIds[0]}"))
				if err != nil || len(b0) == 0 {
					return fmt.Errorf("gang-sub-0 unbound: %w", err)
				}
				b1, err := utils.Run(exec.Command("kubectl", "get", "slice", "gang-sub-1",
					"-o", "jsonpath={.spec.partitionIds[0]}"))
				if err != nil || len(b1) == 0 {
					return fmt.Errorf("gang-sub-1 unbound: %w", err)
				}
				p0, p1 = strings.TrimSpace(string(b0)), strings.TrimSpace(string(b1))
				if p0 == p1 {
					return fmt.Errorf("expected distinct partitions, both got %s", p0)
				}
				return nil
			}, 30*time.Second, time.Second).Should(Succeed())

			By("Simulating slice-controller ACTIVATE on both gang members (provisioned-partition + observedGeneration)")
			for _, pair := range [][2]string{{"gang-sub-0", p0}, {"gang-sub-1", p1}} {
				slName, part := pair[0], pair[1]
				cmd = exec.Command("kubectl", "annotate", "slice", slName,
					fmt.Sprintf("accelerator.gke.io/provisioned-partition=%s", part), "--overwrite")
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())

				genBytes, err := utils.Run(exec.Command("kubectl", "get", "slice", slName,
					"-o", "jsonpath={.metadata.generation}"))
				Expect(err).NotTo(HaveOccurred())
				gen := strings.TrimSpace(string(genBytes))

				statusPatch := fmt.Sprintf(
					`{"status":{"observedGeneration":%s,"conditions":[{"type":"Ready","status":"True",`+
						`"reason":"ACTIVE","message":"Active","lastTransitionTime":"2026-01-01T00:00:00Z"}]}}`,
					gen)
				cmd = exec.Command("kubectl", "patch", "slice", slName,
					"--subresource", "status", "--type", "merge", "-p", statusPatch)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			}

			By("Applying NoExecute taint to gang-sub-1's partition and clearing NoSchedule on sub-c")
			taintSubA := `[]`
			stateSubA := "Healthy"
			taintSubB := `[]`
			stateSubB := "Healthy"
			if p1 == "sub-a" {
				taintSubA = `[{"key":"cloud.google.com/partition-state","value":"UNHEALTHY","effect":"NoExecute"}]`
				stateSubA = "Unhealthy"
			} else {
				taintSubB = `[{"key":"cloud.google.com/partition-state","value":"UNHEALTHY","effect":"NoExecute"}]`
				stateSubB = "Unhealthy"
			}
			gangTaintedPT := fmt.Sprintf(`{"status":{"acceleratorType":"tpu7x","root":{`+
				`"partitionId":"np-t1","topology":"4x4x4","state":"Healthy","nodeNames":null,`+
				`"childPartitions":[{"partitionId":"sub-a","topology":"2x2x4","state":"%s","taints":%s,`+
				`"nodeNames":["kwok-node-1","kwok-node-2","kwok-node-3","kwok-node-4"]},`+
				`{"partitionId":"sub-b","topology":"2x2x4","state":"%s","taints":%s,`+
				`"nodeNames":["kwok-node-5","kwok-node-6","kwok-node-7","kwok-node-8"]},`+
				`{"partitionId":"sub-c","topology":"2x2x4","state":"Healthy","taints":[],`+
				`"nodeNames":["kwok-node-9","kwok-node-10","kwok-node-11","kwok-node-12"]}]}}}`,
				stateSubA, taintSubA, stateSubB, taintSubB)
			cmd = exec.Command("kubectl", "patch", "partitiontree", "pt-node-pool-1",
				"--subresource", "status", "--type", "merge", "-p", gangTaintedPT)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying gang-sub-1 unbinds N->0 while gang-sub-0 stays undisturbed on its active sub-slice")
			Eventually(func() error {
				b1, err := utils.Run(exec.Command("kubectl", "get", "slice", "gang-sub-1",
					"-o", "jsonpath={.spec.partitionIds}"))
				if err != nil {
					return err
				}
				if string(b1) != "" && string(b1) != "[]" {
					return fmt.Errorf("expected gang-sub-1 unbound, got %s", string(b1))
				}
				return nil
			}, 25*time.Second, time.Second).Should(Succeed())

			b0Now, err := utils.Run(exec.Command("kubectl", "get", "slice", "gang-sub-0",
				"-o", "jsonpath={.spec.partitionIds[0]}:{.metadata.annotations.cloud\\.google\\.com/evict-pods}"))
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(string(b0Now))).To(Equal(p0 + ":"))

			By("Simulating slice-controller completing DEACTIVATE on gang-sub-1 and verifying 0->M placement onto sub-c")
			cmd = exec.Command("kubectl", "annotate", "slice", "gang-sub-1",
				"accelerator.gke.io/provisioned-partition-")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			g1GenBytes, err := utils.Run(exec.Command("kubectl", "get", "slice", "gang-sub-1",
				"-o", "jsonpath={.metadata.generation}"))
			Expect(err).NotTo(HaveOccurred())
			g1Gen := strings.TrimSpace(string(g1GenBytes))

			g1DeactPatch := fmt.Sprintf(
				`{"status":{"observedGeneration":%s,"conditions":[{"type":"Ready","status":"Unknown",`+
					`"reason":"SliceNotCreated","message":"Deactivated",`+
					`"lastTransitionTime":"2026-01-01T00:01:00Z"}]}}`,
				g1Gen)
			cmd = exec.Command("kubectl", "patch", "slice", "gang-sub-1",
				"--subresource", "status", "--type", "merge", "-p", g1DeactPatch)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() (string, error) {
				b, err := utils.Run(exec.Command("kubectl", "get", "slice", "gang-sub-1",
					"-o", "jsonpath={.spec.partitionIds}"))
				return string(b), err
			}, 25*time.Second, time.Second).Should(Equal(`["sub-c"]`))

			b0Final, err := utils.Run(exec.Command("kubectl", "get", "slice", "gang-sub-0",
				"-o", "jsonpath={.spec.partitionIds[0]}"))
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(string(b0Final))).To(Equal(p0))
		})
	})

	Context("NoExecute Taint Toleration Window", func() {
		setRootTaintsJSON := func(taintsJSON string) {
			nodeNames := make([]string, 0, 16)
			for i := 1; i <= 16; i++ {
				nodeNames = append(nodeNames, fmt.Sprintf(`"kwok-node-%d"`, i))
			}
			statusJSON := fmt.Sprintf(
				`{"status":{"acceleratorType":"tpu7x","root":{"partitionId":"np-t1",`+
					`"topology":"4x4x4","state":"Healthy","nodeNames":[%s],"taints":%s}}}`,
				strings.Join(nodeNames, ","), taintsJSON)
			cmd := exec.Command("kubectl", "patch", "partitiontree", "pt-node-pool-1",
				"--subresource", "status", "--type", "merge", "-p", statusJSON)
			_, err := utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
		}

		slicePartitionIDs := func() (string, error) {
			cmd := exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
			out, err := utils.Run(cmd)
			return strings.TrimSpace(string(out)), err
		}

		It("defers transient not-ready NoExecute, evicts after expiry, and evicts on untolerated NoExecute", func() {
			deployJSONOut, err := utils.Run(exec.Command("kubectl", "get", "deployment",
				"slice-scheduler-controller-manager", "-n", namespace, "-o", "json"))
			Expect(err).NotTo(HaveOccurred())
			var deployObj struct {
				Spec struct {
					Template struct {
						Spec struct {
							Containers []struct {
								Args []string `json:"args"`
							} `json:"containers"`
						} `json:"spec"`
					} `json:"template"`
				} `json:"spec"`
			}
			Expect(json.Unmarshal(deployJSONOut, &deployObj)).To(Succeed())
			origArgsBytes, err := json.Marshal(deployObj.Spec.Template.Spec.Containers[0].Args)
			Expect(err).NotTo(HaveOccurred())
			origArgsJSON := string(origArgsBytes)

			DeferCleanup(func() {
				By("Clearing PartitionTree taints and test Slice, and restoring controller args")
				setRootTaintsJSON("[]")
				_, _ = utils.Run(exec.Command("kubectl", "delete", "slice", "slice-1cube", "--ignore-not-found"))
				if origArgsJSON != "" {
					restorePatch := fmt.Sprintf(
						`[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":%s}]`,
						origArgsJSON)
					_, _ = utils.Run(exec.Command("kubectl", "patch", "deployment",
						"slice-scheduler-controller-manager", "-n", namespace,
						"--type", "json", "-p", restorePatch))
					_, _ = utils.Run(exec.Command("kubectl", "rollout", "status",
						"deployment/slice-scheduler-controller-manager", "-n", namespace, "--timeout", "120s"))
				}
			})

			By("Configuring controller-manager with --enable-taint-filtering and 12s NoExecute tolerations")
			patchArgs := `[
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--enable-taint-filtering"},
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--reschedule-healthy-delay=1s"},
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--reschedule-unhealthy-delay=1s"},
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--default-not-ready-toleration-seconds=12"},
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--default-unreachable-toleration-seconds=12"}
			]`
			_, err = utils.Run(exec.Command("kubectl", "patch", "deployment",
				"slice-scheduler-controller-manager", "-n", namespace, "--type", "json", "-p", patchArgs))
			Expect(err).NotTo(HaveOccurred())
			_, err = utils.Run(exec.Command("kubectl", "rollout", "status",
				"deployment/slice-scheduler-controller-manager", "-n", namespace, "--timeout", "120s"))
			Expect(err).NotTo(HaveOccurred())

			By("Applying Slice manifest and waiting for placement onto np-t1")
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml"))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func(g Gomega) {
				pids, err := slicePartitionIDs()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pids).To(Equal(`["np-t1"]`))
			}, time.Minute, time.Second).Should(Succeed())

			By("1. Transient not-ready:NoExecute (< 12s) does NOT evict, even after original deadline")
			t0 := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
			setRootTaintsJSON(fmt.Sprintf(
				`[{"key":"node.kubernetes.io/not-ready","effect":"NoExecute","timeAdded":%q}]`, t0))
			Consistently(func(g Gomega) {
				pids, err := slicePartitionIDs()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pids).To(Equal(`["np-t1"]`))
			}, 4*time.Second, time.Second).Should(Succeed())

			setRootTaintsJSON("[]")
			Consistently(func(g Gomega) {
				pids, err := slicePartitionIDs()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pids).To(Equal(`["np-t1"]`))
			}, 11*time.Second, time.Second).Should(Succeed())

			By("2. Sustained not-ready:NoExecute (> 12s) defers inside window, then evicts once expired")
			tSustained := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
			setRootTaintsJSON(fmt.Sprintf(
				`[{"key":"node.kubernetes.io/not-ready","effect":"NoExecute","timeAdded":%q}]`, tSustained))
			Consistently(func(g Gomega) {
				pids, err := slicePartitionIDs()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pids).To(Equal(`["np-t1"]`))
			}, 6*time.Second, time.Second).Should(Succeed())

			Eventually(func(g Gomega) {
				pids, err := slicePartitionIDs()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pids).To(SatisfyAny(BeEmpty(), Equal("[]")))
			}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

			By("Clearing taint allows Slice to reschedule back onto np-t1")
			setRootTaintsJSON("[]")
			Eventually(func(g Gomega) {
				pids, err := slicePartitionIDs()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pids).To(Equal(`["np-t1"]`))
			}, 30*time.Second, time.Second).Should(Succeed())

			By("3. Untolerated PartitionState=Unhealthy:NoExecute added mid-window triggers prompt eviction")
			tActive := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
			setRootTaintsJSON(fmt.Sprintf(
				`[{"key":"node.kubernetes.io/not-ready","effect":"NoExecute","timeAdded":%q}]`, tActive))
			time.Sleep(2 * time.Second)
			setRootTaintsJSON(fmt.Sprintf(`[
				{"key":"node.kubernetes.io/not-ready","effect":"NoExecute","timeAdded":%q},
				{"key":"cloud.google.com/partition-state","value":"Unhealthy","effect":"NoExecute","timeAdded":%q}
			]`, tActive, tActive))

			Eventually(func(g Gomega) {
				pids, err := slicePartitionIDs()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pids).To(SatisfyAny(BeEmpty(), Equal("[]")))
			}, 6*time.Second, 500*time.Millisecond).Should(Succeed())
		})
	})
})

const kwokVersion = "v0.7.0"

func setupPartitionTree() error {
	// Install KWOK controller to simulate fake nodes with a virtual kubelet.
	// KWOK manages node lifecycle and transitions pods to Running/Succeeded.
	kwokURL := fmt.Sprintf("https://github.com/kubernetes-sigs/kwok/releases/download/%s/kwok.yaml", kwokVersion)
	cmd := exec.Command("kubectl", "apply", "-f", kwokURL)
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to install KWOK controller: %w", err)
	}

	// Install KWOK default stages for node heartbeat and pod lifecycle.
	// Without this, the node controller marks KWOK nodes as unreachable.
	stagesURL := fmt.Sprintf("https://github.com/kubernetes-sigs/kwok/releases/download/%s/stage-fast.yaml", kwokVersion)
	cmd = exec.Command("kubectl", "apply", "-f", stagesURL)
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to install KWOK stages: %w", err)
	}

	// Apply the KWOK pod-complete stage (auto-completes pods after delay)
	cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/pod-complete-delayed.yaml")
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to apply KWOK pod-complete stage: %w", err)
	}

	// Wait for KWOK controller to be ready
	cmd = exec.Command("kubectl", "wait", "--for=condition=Available",
		"deployment/kwok-controller", "-n", "kube-system", "--timeout", "60s")
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("KWOK controller not ready: %w", err)
	}

	// Create fake KWOK-managed nodes with TPU labels.
	// KWOK recognizes nodes with the annotation "kwok.x-k8s.io/node=fake" and
	// provides a virtual kubelet for them.
	// Phase 1: Create bare nodes with KWOK annotation and basic labels.
	for i := 1; i <= 16; i++ {
		nodeName := fmt.Sprintf("kwok-node-%d", i)
		nodeJSON := fmt.Sprintf(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"%s","annotations":{"kwok.x-k8s.io/node":"fake"},"labels":{"type":"kwok"}}}`, nodeName)
		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(nodeJSON)
		dir, _ := utils.GetProjectDir()
		cmd.Dir = dir
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to create KWOK node %s: %s: %w", nodeName, string(output), err)
		}
	}

	// Wait for KWOK to initialize the nodes (heartbeat stage applies first)
	time.Sleep(5 * time.Second)

	// Phase 2: Label nodes with TPU labels and patch status AFTER KWOK initialization.
	// KWOK's node-initialize stage can overwrite labels set during creation.
	for i := 1; i <= 16; i++ {
		nodeName := fmt.Sprintf("kwok-node-%d", i)
		cmd = exec.Command("kubectl", "label", "node", nodeName,
			"cloud.google.com/gke-tpu-topology=4x4x4",
			"cloud.google.com/gke-tpu-accelerator=tpu7x",
			"tpu.google.com/partition-id-4x4x4=np-t1",
			"--overwrite")
		if _, err := utils.Run(cmd); err != nil {
			return fmt.Errorf("failed to label KWOK node %s: %w", nodeName, err)
		}

		// Patch node status with TPU capacity and Ready condition
		statusPatch := fmt.Sprintf(`{"status":{"capacity":{"google.com/tpu":"4","cpu":"96","memory":"384Gi"},"allocatable":{"google.com/tpu":"4","cpu":"96","memory":"384Gi"},"conditions":[{"type":"Ready","status":"True","reason":"KWOKNodeReady","message":"KWOK fake node"}],"addresses":[{"type":"InternalIP","address":"10.0.0.%d"},{"type":"Hostname","address":"%s"}]}}`, i, nodeName)
		cmd = exec.Command("kubectl", "patch", "node", nodeName, "--subresource", "status", "--type", "merge", "-p", statusPatch)
		if _, err := utils.Run(cmd); err != nil {
			return fmt.Errorf("failed to patch status for KWOK node %s: %w", nodeName, err)
		}
	}

	// Apply the sample PartitionTree (metadata + spec only)
	cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/partitiontree-sample.yaml")
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to apply partitiontree-sample.yaml: %w", err)
	}

	// Patch the PartitionTree status subresource — kubectl apply does not
	// write status fields. The scheduler requires status.root to be populated
	// for partition allocation.
	nodeNames := ""
	for i := 1; i <= 16; i++ {
		if i > 1 {
			nodeNames += ","
		}
		nodeNames += fmt.Sprintf(`"kwok-node-%d"`, i)
	}
	statusJSON := fmt.Sprintf(`{"status":{"root":{"partitionId":"np-t1","topology":"4x4x4","state":"Healthy","nodeNames":[%s]}}}`, nodeNames)
	cmd = exec.Command("kubectl", "patch", "partitiontree", "pt-node-pool-1",
		"--subresource", "status", "--type", "merge", "-p", statusJSON)
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to patch PartitionTree status: %w", err)
	}

	return nil
}

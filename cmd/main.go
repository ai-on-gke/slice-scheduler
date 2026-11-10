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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"time"

	uberzap "go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	v1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/generators"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins"
	"github.com/ai-on-gke/slice-scheduler/internal/plugins/taint"
	"github.com/ai-on-gke/slice-scheduler/internal/pod_reconciler"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/scheduler"
	"github.com/ai-on-gke/slice-scheduler/internal/schedulerlib"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"

	// +kubebuilder:scaffold:imports

	_ "github.com/ai-on-gke/slice-scheduler/internal/metrics"
)

var (
	scheme = runtime.NewScheme()

	// Version information
	buildTimestamp = "unknown"
	buildCommit    = "unknown"
	semver         = "unknown"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(slicev1beta1.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var tpuType string
	var metricsAddr string
	var pprofAddr string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var enableNodeReadiness bool
	var enableTaintFiltering bool
	var defaultNotReadyTolerationSeconds int64
	var defaultUnreachableTolerationSeconds int64
	var enableTestingFilter bool
	var enablePodReconciler bool
	var waitForSliceReady bool
	var podReconcilerWorkers int
	var podDrainTimeout time.Duration
	var rescheduleHealthyDelay time.Duration
	var rescheduleUnhealthyDelay time.Duration
	var rescheduleTimeout time.Duration
	flag.StringVar(&tpuType, "tpu-type", "v6e", "Scheduler for a particular tpuType")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metric endpoint binds to. "+
		"Use the port :8080. If not set, it will be '0 in order to disable the metrics server")
	flag.StringVar(&pprofAddr, "pprof-bind-address", "",
		"The address the pprof endpoint binds to. "+
			"If not set or set to '0', pprof is disabled.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set the metrics endpoint is served securely")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.BoolVar(&enableNodeReadiness, "enable-node-readiness", false,
		"If set, the scheduler takes k8s node readiness into account, from the NodeReady condition on each leaf partition.")
	flag.BoolVar(&enableTaintFiltering, "enable-taint-filtering", false,
		"If set, the scheduler takes PartitionTree taints into account: any NoSchedule or "+
			"NoExecute taint keeps new slices off a partition. Pod tolerations are not checked "+
			"(save for --default-not-ready-toleration-seconds and "+
			"--default-unreachable-toleration-seconds on bound slices), so keep node taints "+
			"that workloads tolerate off the PartitionTree with the partition-tree controller's "+
			"taints.nodePropagation.excludedPrefixes. Requires the partition-tree controller "+
			"to be running with --enable-taints, which is what populates the taints, and "+
			"--enable-pod-reconciler, which evicts slices from partitions with a NoExecute taint.")
	flag.Int64Var(&defaultNotReadyTolerationSeconds, "default-not-ready-toleration-seconds",
		watcher.DefaultNodeHealthTolerationSeconds,
		"Seconds a bound slice tolerates node.kubernetes.io/not-ready:NoExecute on its partition "+
			"before eviction (measured from PartitionTaint.timeAdded). 0 evicts immediately; "+
			"negative values tolerate forever.")
	flag.Int64Var(&defaultUnreachableTolerationSeconds, "default-unreachable-toleration-seconds",
		watcher.DefaultNodeHealthTolerationSeconds,
		"Seconds a bound slice tolerates node.kubernetes.io/unreachable:NoExecute on its partition "+
			"before eviction (measured from PartitionTaint.timeAdded). 0 evicts immediately; "+
			"negative values tolerate forever.")
	flag.BoolVar(&enableTestingFilter, "enable-testing-filter", true,
		"If set, the scheduler will enable the testing filter to avoid nodes labeled cloud.google.com/gke-scheduler-testing=NotReady.")
	flag.BoolVar(&enablePodReconciler, "enable-pod-reconciler", false, "Enable the pod reconciler for pod-to-node binding")
	flag.BoolVar(&waitForSliceReady, "wait-for-slice-ready", true,
		"If set, the pod-reconciler will wait for the associated slice to be in READY state before binding.")
	flag.IntVar(&podReconcilerWorkers, "pod-reconciler-workers", 4, "Number of concurrent pod reconciler workers")
	flag.DurationVar(&podDrainTimeout, "pod-drain-timeout", 5*time.Minute,
		"How long the pod-reconciler lets a slice's pods leave gracefully, counted from the first eviction, "+
			"before force-deleting the ones still on their nodes. Covers evictions a PodDisruptionBudget "+
			"refuses and pods slow to terminate. Set to 0 to never force-delete.")
	flag.DurationVar(&rescheduleHealthyDelay, "reschedule-healthy-delay", 1*time.Minute,
		"Delay for rescheduling slices when partitions are healthy.")
	flag.DurationVar(&rescheduleUnhealthyDelay, "reschedule-unhealthy-delay", 5*time.Second,
		"Delay for rescheduling slices when partitions are unhealthy.")
	flag.DurationVar(&rescheduleTimeout, "reschedule-timeout", 10*time.Minute,
		"Timeout for healthy-partition backoff before forcing reschedule. Set to 0 to disable backoff.")
	opts := zap.Options{

		ZapOpts: []uberzap.Option{uberzap.AddCaller()},
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")
	klog.SetLogger(setupLog)

	setupLog.Info("Starting manager", "version", semver, "buildCommit", buildCommit, "buildTimestamp", buildTimestamp)
	if enableTaintFiltering && !enablePodReconciler {
		setupLog.Info("WARNING: --enable-taint-filtering is set without --enable-pod-reconciler; " +
			"slices on partitions with a NoExecute taint will be marked for eviction but never evicted")
	}

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: tlsOpts,
	})

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Pod{}: {
					Field:     fields.SelectorFromSet(fields.Set{"spec.schedulerName": interfaces.SliceSchedulerName}),
					Transform: pod_reconciler.TrimPod,
				},
			},
		},
		Metrics: metricsserver.Options{
			BindAddress:   metricsAddr,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOpts,
		},
		Logger:                 setupLog.WithName("manager"),
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		PprofBindAddress:       pprofAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "1342b0bb.tpu.google.com",
		// Step down voluntarily instead of letting the lease expire, so the next
		// leader can start placing slices immediately rather than waiting out
		// LeaseDuration. Until then no replica schedules anything, which is why a
		// rollout can report success on both replicas while nothing is being placed.
		//
		// The scaffold warns this is unsafe if the binary does work after the manager
		// stops. It does not: mgr.Start is the last statement in main. The safety does
		// not rest on that alone, though — SliceReconciler.Start returns as soon as it
		// has spawned processSchedulingQueue and the processBindingQueue pool, so
		// StopAndWait does not wait for those workers. What makes the release safe is
		// that every API write they make is bound to the context the manager cancels
		// when it stops the leader-election runnables, and that cancellation happens
		// before the lease is released: an in-flight write cannot land after another
		// replica has taken over. The only other goroutines in the binary are
		// requeueWithDelay, which selects on ctx.Done and touches the in-memory queue
		// only, and the scorer pool, which is joined by a WaitGroup inside ScorePhase.
		//
		// Anything added after mgr.Start, or any goroutine that writes to the API under
		// a context the manager does not cancel, has to revisit this.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Index Pods by node name for efficient lookup in the informer cache.
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&corev1.Pod{},
		"spec.nodeName",
		func(rawObj client.Object) []string {
			pod := rawObj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		},
	); err != nil {
		setupLog.Error(err, "unable to set up field indexer for pod nodeName")
		os.Exit(1)
	}

	// Initialize modular scheduler components
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 3}),
	}

	filters := []interfaces.FilterPlugin{
		&plugins.ExcludeDegradedFilter{},
		&taint.TaintFilter{},
		&plugins.HealthyFilter{},
		&plugins.ManagedFilter{},
		&plugins.PriorityFilter{},
	}
	if enableTestingFilter {
		filters = append(filters, &plugins.TestingFilter{})
	}

	scorers := []interfaces.ScorePlugin{
		&plugins.PreemptionScorer{},
		&plugins.DegradationSinkScorer{},
		&plugins.MinimizeSwappedCubesScorer{},
		&plugins.TightestFitScorer{},
	}

	schedulerLib := schedulerlib.NewDefaultScheduler(gens, filters, scorers)

	partitionCache := watcher.NewPartitionCache(
		enableNodeReadiness,
		watcher.WithTaintFiltering(enableTaintFiltering),
		watcher.WithDefaultNotReadyTolerationSeconds(defaultNotReadyTolerationSeconds),
		watcher.WithDefaultUnreachableTolerationSeconds(defaultUnreachableTolerationSeconds),
	)

	slicePolicy := policy.NewCloudPolicy()

	reconciler, err := scheduler.NewSliceReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		ctrl.Log.WithName(interfaces.SliceSchedulerName),
		tpuType,
		mgr.GetEventRecorder(interfaces.SliceSchedulerName),
		enableNodeReadiness,
		schedulerLib,
		partitionCache,
		rescheduleHealthyDelay,
		rescheduleUnhealthyDelay,
		slicePolicy,
		rescheduleTimeout,
	)
	if err == nil {
		reconciler.EnableAsyncBinding(100, 4)
	}
	if err != nil {
		setupLog.Error(err, "unable to Initialize Slice Reconciler")
		os.Exit(1)
	}

	if err = reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Slice")
		os.Exit(1)
	}
	if err := mgr.Add(reconciler); err != nil {
		setupLog.Error(err, "unable to add reconciler as a runnable", "controller", "Slice")
		os.Exit(1)
	}

	if enablePodReconciler {
		podReconciler := pod_reconciler.NewPodReconciler(
			mgr.GetClient(),
			mgr.GetAPIReader(),
			mgr.GetScheme(),
			ctrl.Log.WithName("pod-reconciler"),
			tpuType,
			partitionCache,
			mgr.GetEventRecorder("pod-reconciler"),
			podReconcilerWorkers,
			waitForSliceReady,
			slicePolicy,
		)

		podReconciler.DrainTimeout = podDrainTimeout

		if err := podReconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "PodReconciler")
			os.Exit(1)
		}
		setupLog.Info("PodReconciler enabled", "workers", podReconcilerWorkers, "waitForSliceReady", waitForSliceReady,
			"drainTimeout", podDrainTimeout)
	}

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

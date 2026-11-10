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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	slicev1beta1 "github.com/ai-on-gke/slice-scheduler/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/ai-on-gke/slice-scheduler/internal/policy"
	"github.com/ai-on-gke/slice-scheduler/internal/watcher"
)

const (
	SliceNameLabel = "cloud.google.com/gke-tpu-slice"
)

// SliceUIDAnnotation records the UID of the Slice a pod was bound for. The
// reconciler sets it on the Binding, and the API server copies a Binding's
// annotations onto the pod in the same write that sets spec.nodeName. It tells
// pods of a Slice apart from pods of an earlier Slice with the same name.
const SliceUIDAnnotation = "slice-scheduler.tpu.google.com/slice-uid"

// DefaultSuccessorGracePeriod is how long after a Slice is created its
// no-partitions branch leaves alone pods bound for an earlier Slice of the same
// name. The scheduler tries to place a new Slice as soon as it sees it, so this
// only needs to cover a normal placement. It is kept short because a spared pod
// still occupies its host, which blocks binding there for whichever slice is
// placed on it.
const DefaultSuccessorGracePeriod = time.Minute

// DrainStartedAtAnnotation records on a Slice, in RFC3339, when the drain of
// its pods began, so the drain can be bounded by DrainTimeout. It lives on the
// Slice rather than on pods because a pod a PodDisruptionBudget keeps from
// being evicted is never marked for deletion, and because the clock has to
// survive a leader change.
const DrainStartedAtAnnotation = "slice-scheduler.tpu.google.com/drain-started-at"

// Retry delays for pod reconciler operations.
const (
	retryDelayNodeOccupied  = 1 * time.Second
	retryDelayEvictionDrain = 2 * time.Second
)

// PodReconciler reconciles Slice objects by managing pod lifecycle
// (eviction and binding) for pods associated with each Slice.
type PodReconciler struct {
	client.Client
	// APIReader reads from the API server, bypassing the manager's cache. The
	// drain uses it to confirm no pod is still placed before unbinding.
	APIReader         client.Reader
	Scheme            *runtime.Scheme
	Log               logr.Logger
	TPUType           string
	Cache             *watcher.PartitionCache
	Recorder          events.EventRecorder
	Workers           int
	WaitForSliceReady bool
	Policy            policy.SlicePolicy
	// DrainTimeout is how long a slice's pods get to leave gracefully, counted
	// from the first eviction, before the ones still placed are force-deleted.
	// Zero never force-deletes.
	DrainTimeout time.Duration
	// SuccessorGracePeriod bounds how long handleNoPartitions spares pods bound
	// for an earlier Slice of the same name. Zero means DefaultSuccessorGracePeriod.
	SuccessorGracePeriod time.Duration
}

func NewPodReconciler(k8sClient client.Client, apiReader client.Reader, scheme *runtime.Scheme, logger logr.Logger, tpuType string, cache *watcher.PartitionCache, recorder events.EventRecorder, workers int, waitForSliceReady bool, slicePolicy policy.SlicePolicy) *PodReconciler {
	return &PodReconciler{
		Client:            k8sClient,
		APIReader:         apiReader,
		Scheme:            scheme,
		Log:               logger,
		TPUType:           tpuType,
		Cache:             cache,
		Recorder:          recorder,
		Workers:           workers,
		WaitForSliceReady: waitForSliceReady,
		Policy:            slicePolicy,
	}
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/binding,verbs=create
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("slice", req.NamespacedName)

	// Fetch the Slice instance
	slice := &slicev1beta1.Slice{}
	if err := r.Get(ctx, req.NamespacedName, slice); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch Slice")
		return ctrl.Result{}, err
	}

	managedPods, err := listManagedPods(ctx, r.Client, slice.Name)
	if err != nil {
		log.Error(err, "unable to list Pods for Slice", "sliceName", slice.Name)
		return ctrl.Result{}, err
	}

	// Check for eviction: annotation-based (preemption) or policy-based (failure state)
	hasEvictAnnotation := slice.Annotations != nil && slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr
	partitionBroken := r.isPartitionBroken(slice.Spec.PartitionIds)
	shouldEvict := hasEvictAnnotation || r.Policy.ShouldEvictPod(slice, partitionBroken)

	if shouldEvict {
		return r.handleEviction(ctx, log, slice, managedPods, hasEvictAnnotation)
	}

	return r.handleScheduling(ctx, log, slice, managedPods)
}

// listManagedPods returns the pods labelled for the slice that this scheduler
// manages, read through reader.
func listManagedPods(ctx context.Context, reader client.Reader, sliceName string) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := reader.List(ctx, podList, client.MatchingLabels{SliceNameLabel: sliceName}); err != nil {
		return nil, err
	}
	var managedPods []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Spec.SchedulerName == interfaces.SliceSchedulerName {
			managedPods = append(managedPods, pod)
		}
	}
	return managedPods, nil
}

// placedPods returns the pods a drain waits for: those bound to a node and not
// yet terminal. A Pending pod holds no hardware, and the JobSet, LWS or
// StatefulSet controller recreates an evicted pod under the same slice label,
// so waiting on every labelled pod never finishes.
func placedPods(pods []corev1.Pod) []*corev1.Pod {
	var placed []*corev1.Pod
	for i := range pods {
		if pods[i].Spec.NodeName != "" && !isPodTerminal(&pods[i]) {
			placed = append(placed, &pods[i])
		}
	}
	return placed
}

// isPodTerminal reports whether the pod has finished running. A Succeeded or
// Failed pod keeps spec.nodeName until it is garbage-collected, but no longer
// holds the node's resources.
func isPodTerminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// handleEviction evicts the slice's placed pods. Once none remain, it clears
// PartitionIds and removes the eviction annotation, if this scheduler owns the
// slice.
func (r *PodReconciler) handleEviction(ctx context.Context, log logr.Logger, slice *slicev1beta1.Slice, managedPods []corev1.Pod, hasEvictAnnotation bool) (ctrl.Result, error) {
	placed := placedPods(managedPods)

	// The cache can lag a binding this reconciler just made, so a pod it
	// placed may still read as Pending. Unbinding a slice whose pods still run
	// can't be undone, so confirm against the API server before doing it.
	if len(placed) == 0 && len(slice.Spec.PartitionIds) > 0 && r.ownsSlice(slice) {
		livePods, err := listManagedPods(ctx, r.APIReader, slice.Name)
		if err != nil {
			log.Error(err, "unable to confirm drain against the API server, leaving slice bound", "sliceName", slice.Name)
			return ctrl.Result{}, err
		}
		placed = placedPods(livePods)
		if len(placed) > 0 {
			log.Info("Cache shows slice drained but the API server does not, continuing drain",
				"sliceName", slice.Name, "count", len(placed))
		}
	}

	if len(placed) > 0 {
		log.Info("Evicting pods for slice", "sliceName", slice.Name, "count", len(placed),
			"evictAnnotation", hasEvictAnnotation)
		return r.drainPods(ctx, log, slice, placed, fmt.Sprintf("Slice %s eviction triggered", slice.Name))
	}

	// Every Slice reaches this reconciler, including ones another instance or
	// nothing at all manages. Only the owner may unbind: the scheduler never
	// places a Slice it doesn't own, so clearing PartitionIds would leave the
	// slice unbound for good.
	if !r.ownsSlice(slice) {
		log.Info("All pods evicted, not unbinding slice this scheduler does not manage", "sliceName", slice.Name)
		return ctrl.Result{}, r.clearDrainStart(ctx, slice)
	}

	// Nothing is placed. Pending replacement pods are bound once the slice
	// is placed again.
	log.Info("All pods evicted, unbinding slice", "sliceName", slice.Name,
		"evictAnnotation", hasEvictAnnotation)
	// Unbind: clear PartitionIds
	slice.Spec.PartitionIds = []string{}
	// Remove annotation if present
	if hasEvictAnnotation {
		delete(slice.Annotations, interfaces.EvictPodsAnnotation)
	}
	delete(slice.Annotations, DrainStartedAtAnnotation)
	if err := r.Update(ctx, slice); err != nil {
		log.Error(err, "unable to update slice after eviction")
		return ctrl.Result{}, err
	}
	log.Info("Slice unbound successfully", "sliceName", slice.Name)
	return ctrl.Result{}, nil
}

// handleNoPartitions evicts the placed pods of a slice that holds no
// partitions, except pods bound for an earlier Slice of the same name while
// this one is new. Slices and pods are matched by name only, and a workload
// that is recreated gets a new Slice under the old name. That successor is
// normally placed shortly after it is created; evicting the predecessor's pods
// first restarts the workload, and under a group-restart policy such as LWS
// RecreateGroupOnPodRestart the restart recreates the Slice yet again.
func (r *PodReconciler) handleNoPartitions(ctx context.Context, log logr.Logger, slice *slicev1beta1.Slice, managedPods []corev1.Pod) (ctrl.Result, error) {
	log.Info("Slice has no partitions assigned yet", "sliceName", slice.Name)
	var spareFor time.Duration
	remaining := r.successorGracePeriod() - time.Since(slice.CreationTimestamp.Time)
	var toDrain []*corev1.Pod
	for _, pod := range placedPods(managedPods) {
		if remaining > 0 && boundForEarlierSlice(pod, slice) {
			log.Info("Not evicting pod bound for an earlier slice of the same name yet", "pod", pod.Name,
				"boundForSliceUID", pod.Annotations[SliceUIDAnnotation], "remaining", remaining.Truncate(time.Second))
			spareFor = remaining
			continue
		}
		toDrain = append(toDrain, pod)
	}
	if len(toDrain) > 0 {
		return r.drainPods(ctx, log, slice, toDrain, fmt.Sprintf("Associated Slice %s has no partitions", slice.Name))
	}
	if err := r.clearDrainStart(ctx, slice); err != nil {
		log.Error(err, "unable to clear drain start on slice", "sliceName", slice.Name)
		return ctrl.Result{}, err
	}
	// Look at the spared pods again once the grace period is over.
	return ctrl.Result{RequeueAfter: spareFor}, nil
}

// drainPods evicts the given placed pods of a slice and requeues to check on
// them. A pod stays in the drain until it leaves its node, not just until its
// eviction is accepted, so a pod held up by a PodDisruptionBudget, a finalizer
// or a long preStop hook is still waited on. Once the slice's drain has run for
// DrainTimeout, the pods still placed are force-deleted instead.
//
// A failed eviction doesn't stop the rest of the slice from draining, and is
// not returned as an error: that would put the slice into exponential backoff
// while the deadline it waits on passes.
func (r *PodReconciler) drainPods(ctx context.Context, log logr.Logger, slice *slicev1beta1.Slice, placed []*corev1.Pod, evictedMessage string) (ctrl.Result, error) {
	deadlineExceeded, err := r.markDrainStart(ctx, slice)
	if err != nil {
		log.Error(err, "unable to record drain start on slice", "sliceName", slice.Name)
		return ctrl.Result{}, err
	}

	for _, pod := range placed {
		if deadlineExceeded {
			// A grace period of 0 removes the pod object at once, while the
			// kubelet may still be stopping its containers.
			log.Info("Drain deadline exceeded, force deleting pod", "pod", pod.Name, "timeout", r.DrainTimeout)
			if err := r.Delete(ctx, pod, client.GracePeriodSeconds(0)); err != nil {
				// NotFound: the pod is already gone, so nothing was force-deleted.
				if !apierrors.IsNotFound(err) {
					log.Error(err, "unable to force delete pod", "pod", pod.Name)
					r.Recorder.Eventf(pod, nil, corev1.EventTypeWarning, "EvictionFailed", "Evicting",
						"Slice %s drain exceeded %s; force delete failed: %v", slice.Name, r.DrainTimeout, err)
				}
				continue
			}
			r.Recorder.Eventf(pod, nil, corev1.EventTypeWarning, "ForceDeleted", "Deleting",
				"Slice %s drain exceeded %s; pod force deleted", slice.Name, r.DrainTimeout)
			continue
		}

		if !pod.DeletionTimestamp.IsZero() {
			// Eviction already accepted; wait for the pod to go.
			continue
		}
		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
		}
		log.Info("Triggering eviction for pod", "pod", pod.Name)
		if err := r.Client.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
			if !apierrors.IsNotFound(err) {
				// Usually a 429 from a PodDisruptionBudget.
				log.Error(err, "unable to evict pod, continuing with the rest of the slice", "pod", pod.Name)
				r.Recorder.Eventf(pod, nil, corev1.EventTypeWarning, "EvictionFailed", "Evicting",
					"Slice %s eviction blocked: %v", slice.Name, err)
			}
			continue
		}
		r.Recorder.Eventf(pod, nil, corev1.EventTypeWarning, "Evicted", "Evicting", "%s", evictedMessage)
	}
	return ctrl.Result{RequeueAfter: retryDelayEvictionDrain}, nil
}

// markDrainStart records on the slice when its drain began, the first time it is
// called for a drain, and reports whether DrainTimeout has passed since. It
// never reports a passed deadline when DrainTimeout is zero.
func (r *PodReconciler) markDrainStart(ctx context.Context, slice *slicev1beta1.Slice) (bool, error) {
	if r.DrainTimeout <= 0 {
		return false, nil
	}
	if raw, ok := slice.Annotations[DrainStartedAtAnnotation]; ok {
		if startedAt, err := time.Parse(time.RFC3339, raw); err == nil {
			return time.Since(startedAt) > r.DrainTimeout, nil
		}
		// Unparsable: start the clock again rather than never forcing.
	}
	patch := client.MergeFrom(slice.DeepCopy())
	if slice.Annotations == nil {
		slice.Annotations = map[string]string{}
	}
	slice.Annotations[DrainStartedAtAnnotation] = time.Now().UTC().Format(time.RFC3339)
	return false, r.Patch(ctx, slice, patch)
}

// clearDrainStart removes the drain start from a slice whose drain is over, so
// the next drain doesn't begin already past its deadline.
func (r *PodReconciler) clearDrainStart(ctx context.Context, slice *slicev1beta1.Slice) error {
	if _, ok := slice.Annotations[DrainStartedAtAnnotation]; !ok {
		return nil
	}
	patch := client.MergeFrom(slice.DeepCopy())
	delete(slice.Annotations, DrainStartedAtAnnotation)
	return r.Patch(ctx, slice, patch)
}

func (r *PodReconciler) successorGracePeriod() time.Duration {
	if r.SuccessorGracePeriod > 0 {
		return r.SuccessorGracePeriod
	}
	return DefaultSuccessorGracePeriod
}

// boundForEarlierSlice reports whether the pod was bound for a different Slice
// object than this one. A pod bound before the UID was recorded carries none
// and counts as this slice's own.
func boundForEarlierSlice(pod *corev1.Pod, slice *slicev1beta1.Slice) bool {
	boundFor := pod.Annotations[SliceUIDAnnotation]
	return boundFor != "" && slice.UID != "" && boundFor != string(slice.UID)
}

// handleScheduling handles the normal pod binding flow for a slice.
func (r *PodReconciler) handleScheduling(ctx context.Context, log logr.Logger, slice *slicev1beta1.Slice, managedPods []corev1.Pod) (ctrl.Result, error) {
	if len(slice.Spec.PartitionIds) == 0 {
		return r.handleNoPartitions(ctx, log, slice, managedPods)
	}

	// The slice holds partitions and isn't being evicted, so any drain is over.
	if err := r.clearDrainStart(ctx, slice); err != nil {
		log.Error(err, "unable to clear drain start on slice", "sliceName", slice.Name)
		return ctrl.Result{}, err
	}

	// Check Slice state for readiness if the flag is enabled
	sliceState := policy.GetSliceState(slice)
	if r.WaitForSliceReady && !r.Policy.IsSliceReady(sliceState) {
		log.Info("Waiting for slice to be READY before scheduling pods", "sliceName", slice.Name, "currentState", sliceState)
		return ctrl.Result{}, nil
	}

	// Resolve partition IDs to hostnames in a single lock operation
	nodes, ok := r.Cache.GetNodes(slice.Spec.PartitionIds)
	if !ok {
		log.Info("One or more partitions not found in cache, waiting for cache sync", "partitionIDs", slice.Spec.PartitionIds)
		return ctrl.Result{}, fmt.Errorf("one or more partitions from slice %s not found in cache", slice.Name)
	}

	var hostNames []string
	for _, pID := range slice.Spec.PartitionIds {
		node := nodes[pID]
		var pHostNames []string
		appendHostNamesRecursive(node, &pHostNames)
		hostNames = append(hostNames, pHostNames...)
	}

	if len(hostNames) == 0 {
		log.Info("No hostnames found for slice partitions", "sliceName", slice.Name)
		return ctrl.Result{}, fmt.Errorf("no hostnames found for slice %s", slice.Name)
	}

	// One pod's failure must not hold up the rest of the slice, so every pod
	// gets its turn and the outcomes are combined at the end.
	var bindErrs []error
	var requeueAfter time.Duration
	for i := range managedPods {
		pod := &managedPods[i]
		// A terminal pod has finished running and is never bound again; its
		// replacement, if any, is a separate pod.
		if !pod.DeletionTimestamp.IsZero() || isPodTerminal(pod) {
			continue
		}
		// The API server refuses to bind a pod with scheduling gates. LWS, for
		// one, gates a recreated group's leader until the previous group's pods
		// are gone. Removing the gate is a pod update, which requeues the slice.
		if len(pod.Spec.SchedulingGates) > 0 {
			log.V(1).Info("Pod has scheduling gates, not binding it yet", "pod", pod.Name)
			continue
		}

		retryAfter, err := r.schedulePod(ctx, log, slice, pod, hostNames)
		if err != nil {
			bindErrs = append(bindErrs, fmt.Errorf("pod %s: %w", pod.Name, err))
		}
		if retryAfter > 0 && (requeueAfter == 0 || retryAfter < requeueAfter) {
			requeueAfter = retryAfter
		}
	}
	if len(bindErrs) > 0 {
		return ctrl.Result{}, errors.Join(bindErrs...)
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// schedulePod moves one pod towards its target host: it evicts the pod if it is
// bound elsewhere, and binds it if it is unbound and the host is free and ready.
// A non-zero duration asks for the slice to be reconciled again after it.
func (r *PodReconciler) schedulePod(ctx context.Context, log logr.Logger, slice *slicev1beta1.Slice, pod *corev1.Pod, hostNames []string) (time.Duration, error) {
	index, err := parsePodIndex(pod)
	if err != nil {
		log.Error(err, "unable to parse index from pod", "pod", pod.Name)
		return 0, err
	}

	if index < 0 || index >= len(hostNames) {
		err := fmt.Errorf("pod index %d out of bounds [0, %d)", index, len(hostNames))
		log.Error(err, "Pod index out of bounds", "pod", pod.Name, "index", index, "numHostNames", len(hostNames))
		return 0, err
	}

	targetNode := hostNames[index]

	if pod.Spec.NodeName != "" {
		if pod.Spec.NodeName == targetNode {
			return 0, nil
		}
		log.Info("Deleting pod because its node does not match target node", "pod", pod.Name, "currentNode", pod.Spec.NodeName, "targetNode", targetNode)
		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
		}
		if err := r.Client.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
			log.Error(err, "unable to evict pod", "pod", pod.Name)
			return 0, err
		}
		r.Recorder.Eventf(pod, nil, corev1.EventTypeWarning, "Evicted", "Evicting", "Pod evicted because its node %s does not match target node %s", pod.Spec.NodeName, targetNode)
		return 0, nil
	}

	// A slice being deleted is about to give up its partitions; a pod bound
	// onto them now would hold a host its successor may not be given.
	if !slice.DeletionTimestamp.IsZero() {
		log.Info("Slice is being deleted, not binding pod", "pod", pod.Name)
		return 0, nil
	}

	occupied, err := r.isNodeOccupied(ctx, targetNode)
	if err != nil {
		log.Error(err, "unable to check if node is occupied", "node", targetNode)
		return 0, err
	}
	if occupied {
		log.Info("Target node is occupied, waiting for termination", "pod", pod.Name, "node", targetNode)
		return retryDelayNodeOccupied, nil
	}

	// Node health check — delegated to policy.
	if ready, retryAfter, err := r.Policy.IsNodeReadyForBinding(ctx, r.Client, targetNode, slice.Spec.Topology, slice.Name); !ready {
		if err != nil {
			log.Error(err, "error checking node readiness", "node", targetNode)
		}
		log.Info("Node not ready for binding, waiting", "node", targetNode)
		return retryAfter, err
	}

	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Target: corev1.ObjectReference{
			Kind: "Node",
			Name: targetNode,
		},
	}
	if slice.UID != "" {
		binding.Annotations = map[string]string{SliceUIDAnnotation: string(slice.UID)}
	}

	log.Info("Binding pod to node", "pod", pod.Name, "node", targetNode)
	if err := r.Client.SubResource("binding").Create(ctx, pod, binding); err != nil {
		log.Error(err, "unable to create binding", "pod", pod.Name)
		return 0, err
	}
	r.Recorder.Eventf(pod, nil, corev1.EventTypeNormal, "Scheduled", "Binding", "Successfully bound pod to node %s", targetNode)
	return 0, nil
}

// ownsSlice reports whether this scheduler instance manages the slice, using the
// same test as the scheduler's Slice predicates.
func (r *PodReconciler) ownsSlice(slice *slicev1beta1.Slice) bool {
	return interfaces.OwnsSlice(slice.Annotations[watcher.ManagedByAnnotation], string(slice.Spec.Type), r.TPUType)
}

// isNodeOccupied checks if any pods are currently assigned to the given node.
func (r *PodReconciler) isNodeOccupied(ctx context.Context, nodeName string) (bool, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return false, err
	}

	// Only consider pods managed by the slice-scheduler.
	// System DaemonSet pods (kube-system, gmp-system, etc.) are always
	// present on every node and must be ignored.
	for _, p := range podList.Items {
		if p.Spec.SchedulerName != interfaces.SliceSchedulerName {
			continue
		}
		if isPodTerminal(&p) {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&slicev1beta1.Slice{}).
		// Watch for Pod events and trigger a reconciliation for the associated Slice.
		// This ensures that when a new Pod is created (or updated/deleted) for a Slice
		// that is already Ready, the reconciler will be notified to perform binding.
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			pod, ok := o.(*corev1.Pod)
			if !ok {
				return nil
			}
			if pod.Spec.SchedulerName != interfaces.SliceSchedulerName {
				return nil
			}
			sliceName, ok := pod.Labels[SliceNameLabel]
			if !ok {
				return nil
			}
			return []reconcile.Request{
				{
					NamespacedName: types.NamespacedName{
						Name: sliceName,
					},
				},
			}
		}), builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				pod, ok := e.Object.(*corev1.Pod)
				return ok && pod.Spec.SchedulerName == interfaces.SliceSchedulerName
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				pod, ok := e.ObjectNew.(*corev1.Pod)
				return ok && pod.Spec.SchedulerName == interfaces.SliceSchedulerName
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				pod, ok := e.Object.(*corev1.Pod)
				return ok && pod.Spec.SchedulerName == interfaces.SliceSchedulerName
			},
			GenericFunc: func(e event.GenericEvent) bool {
				pod, ok := e.Object.(*corev1.Pod)
				return ok && pod.Spec.SchedulerName == interfaces.SliceSchedulerName
			},
		})).
		Named("slice-scheduler-pod-reconciler").
		WithOptions(controller.Options{MaxConcurrentReconciles: r.Workers}).
		Complete(r)
}

// isPartitionBroken checks if any partition in the slice is broken (not found
// in cache or marked unhealthy). Uses the same GetPartitionInfo pattern as
// the scheduler's shouldBackoffHealthy.
func (r *PodReconciler) isPartitionBroken(partitionIds []string) bool {
	if len(partitionIds) == 0 {
		return false
	}
	state := watcher.NewPartitionWatcher(r.Cache).GetState()
	infos := state.GetPartitionInfo(partitionIds)
	if len(infos) != len(partitionIds) {
		// Not all partitions found in cache — consider broken
		return true
	}
	for _, info := range infos {
		if info.IsBroken {
			return true
		}
	}
	return false
}

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

package watcher

import (
	"context"
	"math"
	"time"

	partitiontreev1 "github.com/ai-on-gke/slice-scheduler/apis/partition/v1beta1"
	"github.com/ai-on-gke/slice-scheduler/internal/interfaces"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	TpuPodIDLabel            = "cloud.google.com/tpu-pod-id"
	GkeSchedulerTestingLabel = "cloud.google.com/gke-scheduler-testing"
	SchedulerTestingNotReady = "NotReady"
)

// CacheOption configures optional PartitionCache behavior.
type CacheOption func(*PartitionCache)

// WithTaintFiltering enables the taint-aware scheduling paths. It is off by default:
// with it unset the cache records no taints, which makes every taint consumer in the
// scheduler inert.
func WithTaintFiltering(enabled bool) CacheOption {
	return func(pc *PartitionCache) {
		pc.enableTaintFiltering = enabled
	}
}

// DefaultNodeHealthTolerationSeconds is the production CLI and Helm default for
// node.kubernetes.io/not-ready and node.kubernetes.io/unreachable NoExecute tolerations,
// matching Kubernetes' default 300s pod NoExecute toleration window.
const DefaultNodeHealthTolerationSeconds int64 = 300

// WithDefaultNotReadyTolerationSeconds sets how long a bound slice tolerates
// node.kubernetes.io/not-ready:NoExecute before eviction (0 = immediate; <0 = forever).
func WithDefaultNotReadyTolerationSeconds(seconds int64) CacheOption {
	return func(pc *PartitionCache) {
		pc.defaultNotReadyTolerationSeconds = seconds
	}
}

// WithDefaultUnreachableTolerationSeconds sets how long a bound slice tolerates
// node.kubernetes.io/unreachable:NoExecute before eviction (0 = immediate; <0 = forever).
func WithDefaultUnreachableTolerationSeconds(seconds int64) CacheOption {
	return func(pc *PartitionCache) {
		pc.defaultUnreachableTolerationSeconds = seconds
	}
}

// WithClock overrides time.Now when evaluating NoExecute toleration deadlines.
func WithClock(now func() time.Time) CacheOption {
	return func(pc *PartitionCache) {
		if now != nil {
			pc.now = now
		}
	}
}

// NewPartitionCache creates an empty PartitionCache. Optional features default to their
// zero values (taint filtering disabled; NoExecute toleration windows 0s = immediate
// eviction) unless enabled via CacheOption; cmd/main.go wires DefaultNodeHealthTolerationSeconds.
func NewPartitionCache(enableNodeReadiness bool, opts ...CacheOption) *PartitionCache {
	pc := &PartitionCache{
		partitionTreeCache:     make(map[string]*PartitionNode),
		partitionNodeCache:     make(map[string]*PartitionNode),
		leafPartitionNodeCache: make(map[string]*PartitionNode),
		enableNodeReadiness:    enableNodeReadiness,
		now:                    time.Now,
	}
	for _, opt := range opts {
		opt(pc)
	}
	return pc
}

// TaintFilteringEnabled reports whether the taint-aware paths are switched on, so callers
// can skip work the flag makes pointless before doing it.
func (pc *PartitionCache) TaintFilteringEnabled() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.enableTaintFiltering
}

// NoExecuteWait reports whether partitionID (or any descendant) carries an evictable
// NoExecute taint and, if so, how long remains before the earliest eviction deadline
// expires (0 when eviction is due immediately). Taints with a negative toleration
// (tolerate forever) do not trigger eviction; always false unless taint filtering is on.
func (pc *PartitionCache) NoExecuteWait(partitionID string) (bool, time.Duration) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if !pc.enableTaintFiltering {
		return false, 0
	}
	now := pc.now()
	dueAt, ok := pc.nodeNoExecuteDeadlineLocked(pc.partitionNodeCache[partitionID], now)
	if !ok {
		return false, 0
	}
	if !now.Before(dueAt) {
		return true, 0
	}
	return true, dueAt.Sub(now)
}

const maxTolerationSeconds = math.MaxInt64 / int64(time.Second)

// tolerationDuration converts whole toleration seconds to a time.Duration, clamping
// values above ~292 years to math.MaxInt64 instead of overflowing int64 negative.
func tolerationDuration(tol int64) time.Duration {
	if tol > maxTolerationSeconds {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(tol) * time.Second
}

func (pc *PartitionCache) tolerationSecondsForKey(key string) (int64, bool) {
	switch key {
	case corev1.TaintNodeNotReady:
		return pc.defaultNotReadyTolerationSeconds, true
	case corev1.TaintNodeUnreachable:
		return pc.defaultUnreachableTolerationSeconds, true
	default:
		return 0, false
	}
}

// nodeNoExecuteDeadlineLocked returns the earliest eviction deadline across blocked
// NoExecute taints on node (which already unions its descendants' taints). An untolerated
// or zero-second taint returns a zero time.Time{} so it sorts before any positive deadline
// and evaluates as immediately due. Must be called with pc.mu held.
func (pc *PartitionCache) nodeNoExecuteDeadlineLocked(node *PartitionNode, now time.Time) (time.Time, bool) {
	if node == nil {
		return time.Time{}, false
	}
	var earliest time.Time
	found := false
	for i := range node.Taints {
		t := &node.Taints[i]
		if !interfaces.IsNoExecuteBlocked(t) {
			continue
		}
		tol, toleratedKey := pc.tolerationSecondsForKey(t.Key)
		if !toleratedKey || tol == 0 {
			return time.Time{}, true
		}
		if tol < 0 {
			continue
		}
		since := now
		if t.TimeAdded != nil {
			since = t.TimeAdded.Time
		}
		due := since.Add(tolerationDuration(tol))
		if !found || due.Before(earliest) {
			earliest = due
			found = true
		}
	}
	return earliest, found
}

// NoExecuteDeadlines returns the earliest eviction deadline for every partition in the
// named tree that carries an evictable NoExecute taint (zero time.Time{} when due immediately).
// Comparing deadlines across UpdateTree detects both newly tainted partitions and partitions
// whose eviction deadline moved earlier (e.g., an untolerated partition-state taint added
// onto a partition already tolerating node.kubernetes.io/not-ready); the controller does not
// propagate taints up the tree, so diffing PartitionTree objects directly would miss an
// interior partition whose leaf was tainted.
func (pc *PartitionCache) NoExecuteDeadlines(treeName string) map[string]time.Time {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	out := make(map[string]time.Time)
	if !pc.enableTaintFiltering {
		return out
	}
	now := pc.now()
	walkNodes(pc.partitionTreeCache[treeName], func(node *PartitionNode) {
		if due, ok := pc.nodeNoExecuteDeadlineLocked(node, now); ok {
			out[node.Info.PartitionID] = due
		}
	})
	return out
}

// SlicesOnNoExecutePartitions returns the reservation of every slice holding a partition
// of the named tree that carries an evictable NoExecute taint (as reported by
// NoExecuteDeadlines). A slice holding several such partitions appears once per partition.
// The returned pointers are shared with the cache; InUseBy is replaced rather than written
// through, so callers may read them unlocked.
func (pc *PartitionCache) SlicesOnNoExecutePartitions(treeName string) []*interfaces.SliceInfo {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if !pc.enableTaintFiltering {
		return nil
	}
	now := pc.now()
	var found []*interfaces.SliceInfo
	walkNodes(pc.partitionTreeCache[treeName], func(node *PartitionNode) {
		if node.InUseBy != nil {
			if _, ok := pc.nodeNoExecuteDeadlineLocked(node, now); ok {
				found = append(found, node.InUseBy)
			}
		}
	})
	return found
}

// walkNodes calls fn on every node of the subtree, parents before children.
func walkNodes(node *PartitionNode, fn func(*PartitionNode)) {
	if node == nil {
		return
	}
	fn(node)
	for _, child := range node.Children {
		walkNodes(child, fn)
	}
}

func (pc *PartitionCache) GetRootNode(name string) (*PartitionNode, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	node, ok := pc.partitionTreeCache[name]
	return node, ok
}

func (pc *PartitionCache) SetRootNode(name string, node *PartitionNode) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.partitionTreeCache[name] = node
}

// DeletePartitionTree atomically removes a full partition tree from the cache.
func (pc *PartitionCache) DeletePartitionTree(ctx context.Context, name string) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if root, ok := pc.partitionTreeCache[name]; ok {
		logf.Info("Deleting partition tree from cache", "treeName", name)
		pc.deleteNodesFromMapRecursive(root)
		delete(pc.partitionTreeCache, name)
	}
}

func (pc *PartitionCache) GetNode(partitionID string) (*PartitionNode, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	node, ok := pc.partitionNodeCache[partitionID]
	return node, ok
}

// GetNodes returns the partition nodes for all given partition IDs.
// Returns false if any partition ID is not found in the cache.
func (pc *PartitionCache) GetNodes(partitionIDs []string) (map[string]*PartitionNode, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	nodes := make(map[string]*PartitionNode)
	for _, id := range partitionIDs {
		node, ok := pc.partitionNodeCache[id]
		if !ok {
			return nil, false
		}
		nodes[id] = node
	}
	return nodes, true
}

func (pc *PartitionCache) GetLeafNode(nodeName string) (*PartitionNode, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	node, ok := pc.leafPartitionNodeCache[nodeName]
	return node, ok
}

// ReservePartition marks a partition as in-use. It returns false if the partition is already in use.
func (pc *PartitionCache) ReservePartition(ctx context.Context, sInfo *interfaces.SliceInfo) (success bool, existingSlice string) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()

	isFailed := false
	reservedPartitions := []string{}
	for _, partitionID := range sInfo.PartitionIds {
		node, ok := pc.partitionNodeCache[partitionID]
		if !ok {
			// This can happen if a slice references a partition from a tree that doesn't match its type.
			logf.V(1).Info("Attempted to reserve non-existent partition", "sliceName", sInfo.Name, "partitionId", partitionID)
			isFailed = true
			break
		}
		if node.IsUsed() && node.InUseBy.Name != sInfo.Name {
			existingSlice = node.InUseBy.Name
			// This can happen for two reasons:
			// 1) Race condition where scheduler picks a partition that becomes unavailable later
			// 2) Slices created outside of the scheduler with duplicate partitions
			// A webhook should prevent 2 from ever happening for any scheduler to reliably work
			logf.Info("Failed to reserve partition for slice; already in use", "sliceName", sInfo.Name, "partitionId", partitionID, "existingSlice", node.InUseBy.Name)
			isFailed = true
			break
		}
		// A partition physically contains its descendants and is contained by its
		// ancestors, so holding any of them is just as much a conflict as holding
		// this node. The generators reason about the hierarchy already, but the
		// rollback and watch-driven paths (safeReserve, handleSliceCreate,
		// handleSliceUpdate, Start) reserve without consulting them.
		if holder := overlappingHolder(node, sInfo.Name); holder != "" {
			existingSlice = holder
			logf.Info("Failed to reserve partition for slice; an overlapping partition is already in use",
				"sliceName", sInfo.Name, "partitionId", partitionID, "existingSlice", holder)
			isFailed = true
			break
		}
		node.InUseBy = sInfo
		reservedPartitions = append(reservedPartitions, partitionID)
	}
	if isFailed {
		// on failures, roll back the reservations we've made so far.
		for _, reservedID := range reservedPartitions {
			if rNode, rok := pc.partitionNodeCache[reservedID]; rok {
				rNode.InUseBy = nil
			}
		}
		return false, existingSlice
	}

	logf.Info("Successfully reserved partitions for slice", "sliceName", sInfo.Name, "partitionIds", sInfo.PartitionIds)
	return true, ""
}

// overlappingHolder returns the name of a slice other than sliceName that holds a
// partition physically overlapping node, i.e. one of its ancestors or descendants.
// It returns "" when no such slice exists. Must be called with the lock held.
func overlappingHolder(node *PartitionNode, sliceName string) string {
	for anc := node.Parent; anc != nil; anc = anc.Parent {
		if anc.InUseBy != nil && anc.InUseBy.Name != sliceName {
			return anc.InUseBy.Name
		}
	}
	var descendant func(*PartitionNode) string
	descendant = func(n *PartitionNode) string {
		for _, child := range n.Children {
			if child.InUseBy != nil && child.InUseBy.Name != sliceName {
				return child.InUseBy.Name
			}
			if name := descendant(child); name != "" {
				return name
			}
		}
		return ""
	}
	return descendant(node)
}

// ReleasePartitions frees a partition if it's held by the given slices. A hold taken by
// a different Slice of the same name (see SliceInfo.SameSlice) is kept: a Slice can be
// recreated and placed on its predecessor's partitions before the predecessor's delete
// is seen, and that delete must not free them.
//
// It returns the IDs of the partitions it freed, which is empty when none of the slices
// held anything.
func (pc *PartitionCache) ReleasePartitions(ctx context.Context, slices []*interfaces.SliceInfo) []string {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	var freed []string
	for _, s := range slices {
		for _, partitionID := range s.PartitionIds {
			if node, ok := pc.partitionNodeCache[partitionID]; ok && node.InUseBy.SameSlice(s) {
				node.InUseBy = nil
				freed = append(freed, partitionID)
				logf.Info("Released partition for slice", "sliceName", s.Name, "partitionId", partitionID)
			}
		}
	}
	return freed
}

// Overlaps reports whether partitions a and b share hardware: they are the same partition,
// or one contains the other. Unknown partitions overlap nothing but themselves.
func (pc *PartitionCache) Overlaps(a, b string) bool {
	if a == b {
		return true
	}
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	na, oka := pc.partitionNodeCache[a]
	nb, okb := pc.partitionNodeCache[b]
	if !oka || !okb {
		return false
	}
	return isAncestor(na, nb) || isAncestor(nb, na)
}

// isAncestor reports whether anc is a strict ancestor of node.
func isAncestor(anc, node *PartitionNode) bool {
	for p := node.Parent; p != nil; p = p.Parent {
		if p == anc {
			return true
		}
	}
	return false
}

// Holds reports whether every partition sInfo names is reserved by that same Slice.
func (pc *PartitionCache) Holds(sInfo *interfaces.SliceInfo) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	for _, partitionID := range sInfo.PartitionIds {
		node, ok := pc.partitionNodeCache[partitionID]
		if !ok || !node.InUseBy.SameSlice(sInfo) {
			return false
		}
	}
	return true
}

func (pc *PartitionCache) preserveKubeNodeState(node *PartitionNode, kNodeMap map[string]*KubeNode) {
	if node == nil || kNodeMap == nil {
		return
	}
	if node.isLeafPartitionNode() && node.KubeNode != nil {
		if _, ok := kNodeMap[node.KubeNode.Name]; !ok {
			kNodeMap[node.KubeNode.Name] = &KubeNode{
				Name:            node.KubeNode.Name,
				Ready:           node.KubeNode.Ready,
				TestingNotReady: node.KubeNode.TestingNotReady,
			}
		}
	}
	for _, child := range node.Children {
		pc.preserveKubeNodeState(child, kNodeMap)
	}
}

// deleteNodesFromMapRecursive is a recursive helper. Must be called with lock held.
func (pc *PartitionCache) deleteNodesFromMapRecursive(node *PartitionNode) {
	delete(pc.partitionNodeCache, node.Info.PartitionID)
	if node.isLeafPartitionNode() {
		// Range rather than index [0]. A leaf is now defined by having no
		// children, so it may carry any number of node names, and NodeNames[0]
		// would panic on a childless partition that lists none. The insert side
		// only populates single-node leaves, so the extra iterations delete keys
		// that were never present, which is a no-op.
		for _, nodeName := range node.Info.NodeNames {
			delete(pc.leafPartitionNodeCache, nodeName)
		}
	}
	for _, child := range node.Children {
		pc.deleteNodesFromMapRecursive(child)
	}
}

// getUsageState extracts the inUseBy mapping from the cache. Must be called with lock held.
func (pc *PartitionCache) getUsageState() map[string]*interfaces.SliceInfo {
	usageMap := make(map[string]*interfaces.SliceInfo)
	for _, root := range pc.partitionTreeCache {
		var extract func(*PartitionNode)
		extract = func(node *PartitionNode) {
			if node.IsUsed() {
				usageMap[node.Info.PartitionID] = node.InUseBy
			}
			for _, child := range node.Children {
				extract(child)
			}
		}
		extract(root)
	}
	return usageMap
}

// applyUsageState applies a saved usage state to the cache. Must be called with lock held.
func (pc *PartitionCache) applyUsageState(usageMap map[string]*interfaces.SliceInfo) {
	for pID, sInfo := range usageMap {
		if node, ok := pc.partitionNodeCache[pID]; ok {
			node.InUseBy = sInfo
		}
	}
}

func isNodeHealthNoExecuteTaint(t *partitiontreev1.PartitionTaint) bool {
	return t != nil && t.Effect == partitiontreev1.TaintEffectNoExecute &&
		(t.Key == corev1.TaintNodeNotReady || t.Key == corev1.TaintNodeUnreachable)
}

func collectFallbackNodeHealthFirstSeen(root *PartitionNode) map[string]time.Time {
	if root == nil {
		return nil
	}
	out := make(map[string]time.Time)
	walkNodes(root, func(node *PartitionNode) {
		if !node.fallbackNodeHealthFirstSeen.IsZero() {
			out[node.Info.PartitionID] = node.fallbackNodeHealthFirstSeen
		}
	})
	return out
}

// buildNodeTree is a recursive helper to construct the in-memory partitionNode tree. Must be called with lock held.
func (pc *PartitionCache) buildNodeTree(info *partitiontreev1.PartitionInfo, parent *PartitionNode, podName string, kNodeMap map[string]*KubeNode, prevFirstSeen map[string]time.Time, now time.Time, loggr logr.Logger) *PartitionNode {
	node := &PartitionNode{
		Info:   info,
		Parent: parent,
	}
	pc.partitionNodeCache[info.PartitionID] = node
	// Every node in the tree belongs to the pod its root belongs to, so the pod
	// name is propagated down the recursion rather than stamped only on the root.
	// Sub-cube partitions are the ones that get scored for bin-packing, and
	// plugins.TightestFitScorer locates a partition's tree by matching PodName; a
	// blank name matches no root, so the scorer used to fall through to
	// FitScore=math.MaxInt and DensityScore=0 for every sub-cube candidate, tie
	// every candidate against every other, and leave placement to enumeration
	// order. Two other readers -- generators.countUniquePods and the pod label on
	// the scheduling-outcome metrics -- were blank for the same reason.
	node.PodName = podName

	node.Children = make([]*PartitionNode, len(info.ChildPartitions))
	for i := range info.ChildPartitions {
		node.Children[i] = pc.buildNodeTree(&info.ChildPartitions[i], node, podName, kNodeMap, prevFirstSeen, now, loggr)
	}
	if pc.enableTaintFiltering {
		own := info.Taints
		hasNodeHealthNoExecute := false
		hasNilNodeHealthTime := false
		var earliestExplicit time.Time
		for i := range info.Taints {
			t := &info.Taints[i]
			if isNodeHealthNoExecuteTaint(t) {
				hasNodeHealthNoExecute = true
				if t.TimeAdded == nil {
					hasNilNodeHealthTime = true
				} else if earliestExplicit.IsZero() || t.TimeAdded.Time.Before(earliestExplicit) {
					earliestExplicit = t.TimeAdded.Time
				}
			}
		}
		if hasNodeHealthNoExecute {
			firstSeen := prevFirstSeen[info.PartitionID]
			if !earliestExplicit.IsZero() && (firstSeen.IsZero() || earliestExplicit.Before(firstSeen)) {
				firstSeen = earliestExplicit
			}
			if firstSeen.IsZero() {
				firstSeen = now
			}
			node.fallbackNodeHealthFirstSeen = firstSeen
			if hasNilNodeHealthTime {
				stamped := metav1.NewTime(firstSeen)
				own = make([]partitiontreev1.PartitionTaint, len(info.Taints))
				for i := range info.Taints {
					own[i] = *info.Taints[i].DeepCopy()
					if isNodeHealthNoExecuteTaint(&own[i]) && own[i].TimeAdded == nil {
						own[i].TimeAdded = stamped.DeepCopy()
					}
				}
			}
		}
		node.Taints = unionTaints(own, node.Children)
	}
	// Populate the k8s node information to the leaf partition node
	if len(info.ChildPartitions) == 0 && len(info.NodeNames) == 1 {
		// populate the leafPartitionNodeCache
		pc.leafPartitionNodeCache[info.NodeNames[0]] = node

		// Only set if the k8s node is found.
		// We are using the condition node.KubeNode != nil to make the scheduling decision.
		// Check details in isKubeNodeSchedulable().
		// So DO NOT create kubeNode if the k8s node is not retrieved from the cache.
		if kNode, ok := kNodeMap[info.NodeNames[0]]; ok {
			node.KubeNode = kNode
			loggr.V(2).Info("Added kube node to a leaf partition node", "name", node.KubeNode.Name, "ready", node.KubeNode.Ready)
		}
	}
	return node
}

// unionTaints returns own followed by every child's taints not already present, in order,
// so the result is deterministic for a given tree. Duplicate (Key, Value, Effect) taints
// collapse into a single entry retaining the earliest non-nil TimeAdded. Children are built
// first and already carry their whole subtree, so one level is enough. The result never
// aliases own or child taints, which point into the informer's copy of the PartitionTree.
func unionTaints(own []partitiontreev1.PartitionTaint, children []*PartitionNode) []partitiontreev1.PartitionTaint {
	type taintKey struct {
		key    string
		value  string
		effect partitiontreev1.TaintEffect
	}
	var out []partitiontreev1.PartitionTaint
	seenIdx := make(map[taintKey]int)
	add := func(taints []partitiontreev1.PartitionTaint) {
		for i := range taints {
			t := &taints[i]
			k := taintKey{key: t.Key, value: t.Value, effect: t.Effect}
			idx, exists := seenIdx[k]
			if !exists {
				seenIdx[k] = len(out)
				out = append(out, *t.DeepCopy())
				continue
			}
			if t.TimeAdded != nil && (out[idx].TimeAdded == nil || t.TimeAdded.Before(out[idx].TimeAdded)) {
				out[idx].TimeAdded = t.TimeAdded.DeepCopy()
			}
		}
	}
	add(own)
	for _, child := range children {
		add(child.Taints)
	}
	return out
}

// UpdateTree atomically updates a partition tree in the cache.
func (pc *PartitionCache) UpdateTree(ctx context.Context, tree *partitiontreev1.PartitionTree, nodes []corev1.Node) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()

	logf.V(2).Info("Updating partition tree in cache", "treeName", tree.Name, "numKubeNodes", len(nodes), "enableNodeReadiness", pc.enableNodeReadiness)

	// convert the node list to a map, key is the node name, value is the node object
	nodeMap := make(map[string]*KubeNode)
	for _, node := range nodes {
		nodeMap[node.Name] = &KubeNode{
			Name:            node.Name,
			Ready:           isNodeReady(&node),
			TestingNotReady: isTestingNotReady(&node),
		}
	}

	// Preserve current usage state, kube node state, and fallback NoExecute timestamps
	tempUsageMap := pc.getUsageState()
	oldRoot := pc.partitionTreeCache[tree.Name]
	prevFirstSeen := collectFallbackNodeHealthFirstSeen(oldRoot)
	pc.preserveKubeNodeState(oldRoot, nodeMap)

	// Remove old tree if it exists
	if oldRoot != nil {
		pc.deleteNodesFromMapRecursive(oldRoot)
	}

	// Add new tree
	if tree.Status.Root != nil {
		newRoot := pc.buildNodeTree(tree.Status.Root, nil, tree.Labels[TpuPodIDLabel], nodeMap, prevFirstSeen, pc.now(), logf)
		pc.partitionTreeCache[tree.Name] = newRoot
	}

	// Restore usage state
	pc.applyUsageState(tempUsageMap)
}

func (pc *PartitionCache) UpdateLeafNodeCache(ctx context.Context, node *corev1.Node) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if kNode, ok := pc.leafPartitionNodeCache[node.Name]; ok {
		if kNode.KubeNode == nil {
			kNode.KubeNode = &KubeNode{Name: node.Name}
		}
		kNode.KubeNode.Ready = isNodeReady(node)
		kNode.KubeNode.TestingNotReady = isTestingNotReady(node)
		logf.V(2).Info("Updating leaf node status in cache", "node name", node.Name, "ready", kNode.KubeNode.Ready, "testingNotReady", kNode.KubeNode.TestingNotReady)
	}
}

func isTestingNotReady(node *corev1.Node) bool {
	if node == nil || node.Labels == nil {
		return false
	}
	return node.Labels[GkeSchedulerTestingLabel] == SchedulerTestingNotReady
}

func isNodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

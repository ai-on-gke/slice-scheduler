# User Playbook: Slice Scheduler

A practical, debugging-focused guide for GKE customers using the
**Slice Scheduler**. This playbook focuses on what you can see and act
on with `kubectl` and standard monitoring tools.

> Every behavioral claim in this playbook is verified against the
> controller's source. If you want to dig deeper, see the
> [Source Map](#appendix-a-source-map) appendix.

---

## Table of Contents

1. [What is the Slice Scheduler?](#1-what-is-the-slice-scheduler)
2. [Quick Reference](#2-quick-reference)
3. [Slice lifecycle](#3-slice-lifecycle)
4. [Inspecting the scheduler](#4-inspecting-the-scheduler)
5. [Triage decision tree](#5-triage-decision-tree)
6. [Troubleshooting scenarios](#6-troubleshooting-scenarios)
7. [Events & conditions cheatsheet](#7-events--conditions-cheatsheet)
8. [Metrics & monitoring](#8-metrics--monitoring)
9. [FAQ](#9-faq)

---

## 1. What is the Slice Scheduler?

The **Slice Scheduler** is a Kubernetes controller that assigns TPU
partition IDs to `Slice` CRs (`accelerator.gke.io/v1beta1`) and
optionally binds pods to the corresponding nodes. It operates in two
cooperating reconcile loops:

| Component          | What it does                                                                     |
|--------------------|----------------------------------------------------------------------------------|
| **SliceReconciler**    | Watches `Slice` CRs, `PartitionTree` CRs, and `Node` objects. Maintains an in-memory partition cache, enqueues slice groups into a priority queue, runs the Filter → Generate → Score scheduling pipeline, and writes `spec.partitionIds` back to the Slice CR. |
| **PodReconciler**      | Watches pods with `schedulerName: slice-scheduler` and their associated Slices. Resolves partition IDs → hostnames via the partition cache, and creates `Binding` objects to assign pods to nodes. Evicts pods whose slices become unhealthy or lose partitions. |

**Supported TPUs:** v6e (`v6e`) and Ironwood (`tpu7x`). Each scheduler
instance handles exactly one TPU type, controlled by the `--tpu-type`
flag.

### How scheduling works (high-level)

```
Slice CR created/updated
     │
     ▼
┌───────────────────┐
│ Reconcile()       │  Validates that the Slice needs scheduling,
│                   │  determines its workload group, builds a PQ item
└────────┬──────────┘
         │ push to priority queue
         ▼
┌───────────────────┐
│ processScheduling │  Worker goroutine: dequeues by priority, waits
│     Queue()       │  until the group is complete (all slices present)
└────────┬──────────┘
         │
         ▼
┌───────────────────────────────────────────┐
│ tryScheduleSliceGroup()                   │
│  1. Filter (HealthyFilter, ManagedFilter, │
│            PriorityFilter)                │
│  2. Generate (GroupGenerator)             │
│  3. Score (PreemptionScorer,              │
│            DegradationSinkScorer,         │
│            TightestFitScorer)             │
│  4. Reserve (optimistic cache lock)       │
│  5. Bind (API update: partitionIds)       │
└───────────────────────────────────────────┘
```

### Key labels and annotations

| Label / Annotation                                   | Set on | Purpose                                                    |
|------------------------------------------------------|--------|------------------------------------------------------------|
| `cloud.google.com/managed-by`                        | Slice  | Must equal `slice-scheduler` for the scheduler to act.     |
| `cloud.google.com/slice-group-name`                     | Slice  | Groups slices into a workload for gang-scheduling.         |
| `cloud.google.com/num-slices`                        | Slice  | Expected group size; all slices must arrive before scheduling. |
| `cloud.google.com/slice-priority`                    | Slice  | Integer priority (higher = processed first). Default: `0`. |
| `cloud.google.com/ici-degradation-tolerance`         | Slice  | `none` or `all`. Controls OCS degradation handling (Ironwood only). |
| `cloud.google.com/gke-tpu-slice` (`SliceNameLabel`)  | Pod    | Links a pod to its Slice CR for the PodReconciler.         |
| `cloud.google.com/tpu-pod-id`                          | PartitionTree | Identifies the TPU pod (MIG group) for a partition tree. |

### What a schedulable Slice looks like

```yaml
apiVersion: accelerator.gke.io/v1beta1
kind: Slice
metadata:
  name: my-workload-slice-0
  annotations:
    cloud.google.com/managed-by: slice-scheduler
    cloud.google.com/slice-group-name: my-workload
    cloud.google.com/num-slices: "2"
    cloud.google.com/slice-priority: "100"
spec:
  type: tpu7x
  topology: 4x4x4
  partitionIds: []           # empty → needs scheduling
status:
  conditions: []
```

After successful scheduling, `spec.partitionIds` is populated:

```yaml
spec:
  partitionIds:
    - my-pool-root-0
```

---

## 2. Quick Reference

```bash
# List all Slices (cluster-scoped)
kubectl get slices

# Human-readable summary for one Slice
kubectl describe slice <name>

# Check what partitions a Slice is assigned to
kubectl get slice <name> -o jsonpath='{.spec.partitionIds}'

# Check Slice Ready condition
kubectl get slice <name> \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")]}{"\n"}'

# See scheduler events for a Slice
kubectl get events --field-selector involvedObject.kind=Slice,involvedObject.name=<name>

# List all pods managed by the slice-scheduler
kubectl get pods --all-namespaces \
  -o wide --field-selector spec.schedulerName=slice-scheduler

# Check scheduler health
kubectl get --raw '/healthz'  # on the scheduler's health-probe port (:8081)

# Tail scheduler logs
kubectl -n <namespace> logs -l app=slice-scheduler -f

# List PartitionTrees the scheduler watches
kubectl get partitiontrees

# Dump the partition tree for a specific pool
kubectl get partitiontree <pool> -o jsonpath='{.status.root}' | jq .
```

---

## 3. Slice lifecycle

### 3.1 States

A Slice's scheduling state is determined by its `spec.partitionIds` and
its `Ready` condition:

| State                | `partitionIds` | `Ready` condition    | What it means                                              |
|----------------------|----------------|----------------------|------------------------------------------------------------|
| **Pending**          | `[]` (empty)   | absent               | Newly created, waiting to be scheduled.                    |
| **Scheduled**        | populated      | absent or any        | Partitions assigned; waiting for the slice-controller to activate. |
| **Active**           | populated      | `True`, reason=`ACTIVE` | Fully healthy and operational.                          |
| **Unhealthy**        | populated      | `False`, reason=`UNHEALTHY` | Partitions are broken; scheduler will attempt rescheduling. |
| **Failed**           | populated      | `False`, reason=`SliceCreationFailed` | Slice creation failed; scheduler will attempt rescheduling. |
| **Evicted**          | `[]` (cleared) | any                  | Preempted or evicted; scheduler will reschedule.           |

### 3.2 Scheduling triggers

The scheduler reconciles a Slice when **all** of these are true:

1. `cloud.google.com/managed-by` annotation equals `slice-scheduler`.
2. `spec.type` matches the scheduler's `--tpu-type`.
3. One of:
   - `spec.partitionIds` is empty (new slice), or
   - The `Ready` condition is `False` with reason `UNHEALTHY` or
     `SliceCreationFailed` (rescheduling).
4. `DeletionTimestamp` is zero (not being deleted).
5. When `spec.partitionIds` is empty, no N→0 deactivation is in progress
   (otherwise `Reconcile` skips placement and requeues after `5s`):
   `accelerator.gke.io/provisioned-partition` is absent or empty, and any
   nonzero `status.observedGeneration` has caught up to
   `metadata.generation` (`status.observedGeneration: 0` on a brand-new
   Slice does not block scheduling).

### 3.3 Group scheduling

Slices are grouped by the `cloud.google.com/slice-group-name` annotation.
The scheduler waits until all slices in the group arrive
(`len(slices) >= num-slices`) before attempting to schedule. If
`slice-group-name` is absent, the slice name itself is used as the group
ID (group of one).

### 3.4 Priority queue

Within the scheduling queue, groups are ordered by the
`cloud.google.com/slice-priority` annotation (higher integer =
processed first). If two groups have the same priority, order is
insertion-based. A duplicate push (same `GroupId`) updates the
priority in-place rather than adding a second entry.

### 3.5 Rescheduling delays

| Condition                                        | Delay before requeueing  |
|--------------------------------------------------|--------------------------|
| Unhealthy or failed slice                        | `--reschedule-unhealthy-delay` (default: 5 s)  |
| Rescheduling requested but partitions are healthy | `--reschedule-healthy-delay` (default: 1 min)  |
| Scheduling failure (no capacity)                 | 5 s (hardcoded)          |
| Reserve or bind failure                          | 1 s (hardcoded)          |

### 3.6 Preemption

When no free partitions can satisfy a request, the scheduler considers
preemption. Three filters gate which partitions are eligible:

| Filter             | Rule                                                                        |
|--------------------|-----------------------------------------------------------------------------|
| **HealthyFilter**  | Excludes broken or unhealthy partitions.                                    |
| **ManagedFilter**  | Only considers preempting slices managed by `slice-scheduler`. Partitions occupied by other controllers are never touched. |
| **PriorityFilter** | A slice can only preempt victims with **strictly lower** priority.          |

After filtering, candidates are ranked by three scorers:

| Scorer                     | Objective                                                                  |
|----------------------------|----------------------------------------------------------------------------|
| **PreemptionScorer**       | Prefers candidates requiring fewer or cheaper preemptions.                 |
| **DegradationSinkScorer**  | For Ironwood slices tolerating degradation, prefers degraded cubes to preserve normal ones. |
| **TightestFitScorer**      | Bin-packing: prefers already-fragmented trees to keep whole trees free.    |

When a victim is selected:
1. The victim's `spec.partitionIds` are cleared in the API.
2. A `Preempted` Warning event is recorded on the victim Slice.
3. A `Evicted` Warning event is recorded if the slice was being rescheduled and couldn't fit.

### 3.7 Pod binding (PodReconciler)

The PodReconciler handles the second phase: binding pods to nodes. It
is enabled via `--enable-pod-reconciler` and runs with configurable
concurrency (`--pod-reconciler-workers`, default: 4).

**Pod selection criteria:**
- `spec.schedulerName` must be `slice-scheduler`.
- Pod must have the label `cloud.google.com/gke-tpu-slice` pointing to
  a valid Slice CR.

**Binding flow:**

1. Fetch the associated Slice CR.
2. **Eviction checks:**
   - If the slice state is `FAILED` or `SliceHealthProbeFailed` → evict the pod.
   - If `spec.partitionIds` is empty and the pod is already bound → evict (partitions lost).
   - If the pod is bound to the wrong node → evict.
3. If `--wait-for-slice-ready` is set (default: `true`) and the slice
   is not `ACTIVE` → skip, wait for the slice to become ready.
4. Resolve partition IDs to hostnames via the partition cache
   (deterministic ordering from the partition tree).
5. Parse the pod's worker index from annotations/labels/name suffix.
6. Map index → target node hostname.
7. **Pre-bind checks:**
   - Wait for any existing slice-scheduler pods to drain from the target node.
   - Verify the target node has the partition-state health label
     (`cloud.google.com/gke-tpu-partition-<topology>-state` = `HEALTHY`
     or `DEGRADED`).
8. Create a `Binding` object to assign the pod to the node.

---

## 4. Inspecting the scheduler

### Scheduler health

```bash
# Health and readiness probes (default :8081)
kubectl -n <namespace> get --raw '/healthz'
kubectl -n <namespace> get --raw '/readyz'
```

### Cache state (via logs)

The scheduler logs cache operations at various verbosity levels:

```bash
# Standard operations (level 0)
kubectl -n <namespace> logs -l app=slice-scheduler | grep -E 'partition|cache|reserve|release'

# Detailed cache state (level 2, requires --zap-log-level=2)
kubectl -n <namespace> logs -l app=slice-scheduler | grep 'schedulable'
```

Key log messages to watch for:

| Message pattern                                      | What it means                                                   |
|------------------------------------------------------|-----------------------------------------------------------------|
| `Successfully reserved partitions for slice`         | Cache reservation succeeded.                                    |
| `Released partition for slice`                       | Partition freed in cache.                                       |
| `Conflict detected: Partition assigned to ... already in use` | Two slices claim the same partition; cache rejected.   |
| `Updating partition tree in cache`                   | PartitionTree CR updated, cache refreshed.                      |
| `Adding new partition tree to cache`                 | New PartitionTree CR discovered.                                |
| `Deleting partition tree from cache`                 | PartitionTree CR deleted, cache purged.                         |

### Queue depth

```bash
# Via Prometheus metrics endpoint
curl -s http://<scheduler-metrics-addr>/metrics | grep slice_queue_depth
```

### Partition availability

```bash
# Show all PartitionTrees and their root topology
kubectl get partitiontrees -o custom-columns=\
NAME:.metadata.name,\
TYPE:.status.acceleratorType,\
TOPOLOGY:.status.root.topology,\
STATE:.status.root.state,\
OCCUPANCY:.status.root.occupancy

# Show all leaves and their occupancy
kubectl get partitiontree <pool> -o json | jq -r '
  .. | objects | select(.partitionId) |
  "\(.partitionId)\t\(.topology)\t\(.state)\t\(.occupancy)\t\(.sliceId // "none")"'
```

### Slice assignment overview

```bash
# All slices and their partition assignments
kubectl get slices -o custom-columns=\
NAME:.metadata.name,\
TYPE:.spec.type,\
TOPOLOGY:.spec.topology,\
STATE:.status.conditions[0].reason,\
PARTITIONS:.spec.partitionIds

# Slices still waiting to be scheduled
kubectl get slices -o json | jq -r '
  .items[] | select(.spec.partitionIds == null or (.spec.partitionIds | length) == 0) |
  .metadata.name'

# Slices that are unhealthy
kubectl get slices -o json | jq -r '
  .items[] | select(
    .status.conditions[]? |
    select(.type=="Ready" and .status=="False" and
      (.reason=="UNHEALTHY" or .reason=="SliceCreationFailed"))
  ) | .metadata.name'
```

---

## 5. Triage decision tree

```
Symptom you observed
│
├── Slice stuck with empty partitionIds (never scheduled)
│       → §6.1
│
├── Slice was scheduled, now partitionIds are empty (evicted/preempted)
│       → §6.2
│
├── Slice is scheduled but Ready=False (UNHEALTHY)
│       → §6.3
│
├── Pod stuck in Pending (not being bound to a node)
│       → §6.4
│
├── Pod evicted unexpectedly
│       → §6.5
│
├── Pod bound to wrong node
│       → §6.6
│
├── FailedToSchedule events repeating
│       → §6.7
│
├── Scheduler pod itself is unhealthy / crash-looping
│       → §6.8
│
└── Preemption happened unexpectedly
        → §6.9
```

---

## 6. Troubleshooting scenarios

Each scenario gives **symptoms → what it means → what to do**.

### 6.1 Slice stuck with empty `partitionIds` (never scheduled)

**Symptoms**
- `kubectl get slice <name> -o jsonpath='{.spec.partitionIds}'`
  returns `[]` or nothing.
- `FailedToSchedule` events on the Slice.

**What it means**
The scheduler cannot find enough healthy, available partitions to
satisfy the topology request.

**What to do**

1. **Check annotations.** Ensure the Slice has
   `cloud.google.com/managed-by: slice-scheduler` and the correct
   `spec.type` matching the scheduler's `--tpu-type`.

2. **Check group completeness.** If using multi-slice workloads, verify
   all slices in the group exist:
   ```bash
   kubectl get slices -o json | jq -r '
     .items[] | select(.metadata.annotations["cloud.google.com/slice-group-name"] == "<workload>") |
     .metadata.name'
   ```
   The scheduler waits until `num-slices` Slices arrive before attempting
   to schedule the group.

3. **Check partition availability.** Look at PartitionTrees:
   ```bash
   kubectl get partitiontree <pool> -o json | jq '
     .. | objects | select(.partitionId) | select(.occupancy != "Occupied") |
     {id: .partitionId, topology: .topology, state: .state}'
   ```
   If all partitions are `Occupied`, you need more capacity or must
   preempt lower-priority workloads.

4. **Check the deactivation guard.** If `spec.partitionIds` is empty while
   `accelerator.gke.io/provisioned-partition` is non-empty (or while a
   nonzero `status.observedGeneration` still lags `metadata.generation`),
   the scheduler skips 0→M placement until `slice-controller` finishes
   N→0 deactivation.

5. **Check scheduler logs** for the specific failure reason:
   ```bash
   kubectl -n <namespace> logs -l app=slice-scheduler | grep '<slice-name>'
   ```

### 6.2 Slice was scheduled, now `partitionIds` are empty

**Symptoms**
- Slice previously had partition IDs, now they're `[]`.
- `Preempted` or `Evicted` events on the Slice.

**What it means**
Either:
- A higher-priority workload preempted this slice, or
- The slice was being rescheduled and no placement could be found, so it
  was evicted from its old partitions.

**What to do**

1. Check events on the Slice:
   ```bash
   kubectl get events --field-selector involvedObject.name=<slice-name>
   ```
2. Look for `Preempted` events — they include the name of the
   preempting workload/group.
3. Look for `Evicted` events — the message says
   "Slice evicted due to lack of available partitions for rescheduling."
4. If preempted: raise the slice's priority annotation, or investigate
   why a higher-priority workload needed those partitions.
5. The scheduler will automatically re-attempt scheduling after the
   requeue delay (5 s by default for scheduling failures).

### 6.3 Slice is scheduled but `Ready=False`

**Symptoms**
- `spec.partitionIds` is populated.
- `Ready=False`, `reason=UNHEALTHY` or `reason=SliceCreationFailed`.

**What it means**
The slice-controller (a separate component) tried to activate the slice
on the assigned partitions but the hardware is degraded or the GCE
operation failed.

**What to do**

1. The scheduler **will automatically reschedule** after
   `--reschedule-unhealthy-delay` (default: 5 s). If partitions are
   still healthy according to the cache, it backs off for
   `--reschedule-healthy-delay` (default: 1 min) to avoid thrashing.

2. Inspect the PartitionTree to see if the underlying partitions are
   actually unhealthy:
   ```bash
   kubectl get partitiontree <pool> -o json | jq '
     .. | objects | select(.partitionId == "<partition-id>") |
     {state, occupancy}'
   ```

3. If the partition is `Unhealthy` in the PartitionTree, this is a
   hardware issue — engage GKE/TPU support.

### 6.4 Pod stuck in Pending (not being bound)

**Symptoms**
- Pod is `Pending` with `schedulerName: slice-scheduler`.
- No `Scheduled` event on the pod.

**What to do**

1. **Is the PodReconciler enabled?**
   ```bash
   kubectl -n <namespace> logs -l app=slice-scheduler | grep 'PodReconciler enabled'
   ```
   If not present, the `--enable-pod-reconciler` flag is off.

2. **Does the pod have the slice-name label?**
   ```bash
   kubectl get pod <pod> -o jsonpath='{.metadata.labels.cloud\.google\.com/gke-tpu-slice}'
   ```
   The label `cloud.google.com/gke-tpu-slice` must point to a valid
   Slice CR.

3. **Does the Slice have partitions?**
   ```bash
   kubectl get slice <slice-name> -o jsonpath='{.spec.partitionIds}'
   ```
   If empty, the slice hasn't been scheduled yet → see §6.1.

4. **Is the scheduler waiting for slice readiness?**
   If `--wait-for-slice-ready=true` (default), the PodReconciler waits
   for the Slice to reach `ACTIVE` state before binding.
   ```bash
   kubectl get slice <slice-name> \
     -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}'
   ```

5. **Is the target node occupied?** The PodReconciler waits for existing
   `slice-scheduler` pods to drain from the target node before binding.
   Check:
   ```bash
   kubectl get pods --field-selector spec.nodeName=<target-node> \
     -o jsonpath='{range .items[?(@.spec.schedulerName=="slice-scheduler")]}{.metadata.name}{"\n"}{end}'
   ```

6. **Is the partition-state health label missing?** The PodReconciler
   checks for `cloud.google.com/gke-tpu-partition-<topology>-state` on
   the target node. For superslices (topology > 4x4x4), it checks for
   `4x4x4-state` instead.
   ```bash
   kubectl get node <target-node> -o json | jq '.metadata.labels | with_entries(
     select(.key | test("gke-tpu-partition.*-state")))'
   ```

7. **Can the pod index be parsed?** The PodReconciler determines the
   pod's worker index from (in order of precedence):
   - `batch.kubernetes.io/job-completion-index` annotation
   - `tpu.google.com/worker-id` annotation
   - `apps.kubernetes.io/pod-index` label
   - The last `-<number>` segment of the pod name

### 6.5 Pod evicted unexpectedly

**Symptoms**
- Pod was running, then terminated.
- An `Evicted` event on the pod.

**What it means**
The PodReconciler evicted the pod for one of these reasons:

| Event message                                         | Cause                                                      |
|-------------------------------------------------------|------------------------------------------------------------|
| `Slice <name> is UNHEALTHY`                           | Slice state is `FAILED` or `SliceHealthProbeFailed`.       |
| `Associated Slice <name> has no partitions`           | Slice lost its partition assignment (preemption/eviction). |
| `Pod evicted because its node ... does not match target node ...` | Partition reassignment changed the target node.  |

**What to do**
- If the slice is unhealthy → see §6.3.
- If the slice lost partitions → see §6.2.
- If the target node changed → the pod will be rescheduled
  automatically to the correct node on next reconcile.

### 6.6 Pod bound to wrong node

**Symptoms**
- Pod is running but on a node that doesn't match the expected
  partition assignment.

**What it means**
This should not happen under normal operation. The PodReconciler
checks and evicts pods on wrong nodes. If it persists:

**What to do**
1. Verify the partition cache is consistent:
   ```bash
   kubectl -n <namespace> logs -l app=slice-scheduler | grep 'Resolved partition to nodeNames'
   ```
2. Check if the PartitionTree has been updated with new node
   assignments.
3. The PodReconciler will detect the mismatch on next reconcile and
   evict the pod.

### 6.7 `FailedToSchedule` events repeating

**Symptoms**
- `FailedToSchedule` Warning events on multiple Slices.
- `slice_schedule_errors_total` metric is climbing.

**What it means**
The scheduler keeps trying but cannot find capacity. Common reasons:

1. **No capacity:** All partitions are occupied.
2. **All partitions broken:** No `Healthy` partitions in the
   PartitionTree.
3. **Priority too low:** Partitions are held by equal-or-higher-priority
   workloads, and the `PriorityFilter` blocks preemption.
4. **Unmanaged partitions:** Partitions occupied by workloads not
   managed by `slice-scheduler` (the `ManagedFilter` blocks these).
5. **Topology mismatch:** The requested topology doesn't fit the
   available partition shape.

**What to do**
1. Check the `slice_schedule_errors_total` metric's `reason` label for
   the specific failure reason.
2. Inspect available partitions (see §4).
3. Consider raising the workload's priority or scaling the node pool.

### 6.8 Scheduler pod crash-looping

**Symptoms**
- Scheduler pod is in `CrashLoopBackOff`.

**What to do**
1. Check logs for the crash reason:
   ```bash
   kubectl -n <namespace> logs -l app=slice-scheduler --previous
   ```
2. Common causes:
   - **Invalid `--tpu-type`:** Must be exactly `v6e` or `tpu7x`.
   - **Unable to list PartitionTrees:** RBAC issue — the scheduler
     needs `get/list/watch` on `partitiontrees` and
     `slices`, plus `get/list/watch` on `nodes` and `pods`.
   - **Leader election conflict:** Another instance holds the lease.
     Check `--leader-elect` flag.

### 6.9 Unexpected preemption

**Symptoms**
- A running workload's Slice lost its partitions.
- `Preempted` event on the Slice naming the preempting workload.

**What it means**
A workload with higher `cloud.google.com/slice-priority` was scheduled
and selected this Slice's partitions as the lowest-cost preemption
target.

**What to do**
1. Check the preemptor's priority vs. the victim's priority.
2. The `PriorityFilter` ensures only **strictly lower** priority
   workloads can be preempted.
3. To protect a workload: raise its priority annotation.
4. To understand why this particular workload was chosen: the
   `PreemptionScorer` minimizes total preemption cost, the
   `TightestFitScorer` prefers already-fragmented trees, and the
   `DegradationSinkScorer` steers degradation-tolerant workloads toward
   degraded cubes.

---

## 7. Events & conditions cheatsheet

### 7.1 Slice events

Events are recorded on Slice CRs via the standard core `v1.Event` API.

```bash
kubectl get events --field-selector involvedObject.kind=Slice
```

| Reason              | Type      | When                                                                         |
|---------------------|-----------|------------------------------------------------------------------------------|
| `SuccessfulScheduled` | `Normal`  | Slice scheduled successfully to partitions. Message includes partition IDs. |
| `Preempted`           | `Warning` | Slice was preempted to make space for another workload. Message names the preemptor. |
| `Evicted`             | `Warning` | Slice evicted due to lack of available partitions for rescheduling.          |
| `FailedToSchedule`    | `Warning` | Slice failed to schedule due to lack of available partitions.               |

### 7.2 Pod events (PodReconciler)

Events are recorded on Pod objects by the `pod-reconciler` event source.

```bash
kubectl get events --field-selector involvedObject.kind=Pod,source=pod-reconciler
```

| Reason      | Type      | When                                                                            |
|-------------|-----------|---------------------------------------------------------------------------------|
| `Scheduled` | `Normal`  | Pod successfully bound to a node. Message includes the target node name.        |
| `Evicted`   | `Warning` | Pod evicted. Message describes the reason (unhealthy slice, partition loss, or node mismatch). |

### 7.3 The `Ready` condition on Slices

The `Ready` condition is set by the **slice-controller** (not the
scheduler), but the scheduler reads it to decide on rescheduling.

| Status  | Reason                     | Scheduler behavior                                                |
|---------|----------------------------|-------------------------------------------------------------------|
| `True`  | `ACTIVE`                   | Slice is healthy. No scheduler action.                           |
| `False` | `UNHEALTHY`                | Triggers rescheduling after `--reschedule-unhealthy-delay`.      |
| `False` | `SliceCreationFailed`      | Triggers rescheduling after `--reschedule-unhealthy-delay`.      |
| `False` | `FAILED`                   | PodReconciler evicts associated pods.                            |
| `False` | `SliceHealthProbeFailed`   | PodReconciler evicts associated pods.                            |
| absent  | (no condition)             | PodReconciler treats as `SliceNotCreated` — skips binding.       |

---

## 8. Metrics & monitoring

### 8.1 Prometheus metrics

All metrics are registered on the controller-runtime metrics server
(default port via `--metrics-bind-address`).

| Metric                                          | Type      | Labels                                  | Description                                                               |
|-------------------------------------------------|-----------|-----------------------------------------|---------------------------------------------------------------------------|
| `slice_schedule_attempts_total`                 | Counter   | `tpu_type`, `topology`, `is_reschedule` | Total number of slice scheduling attempts.                                |
| `slice_schedule_errors_total`                   | Counter   | `tpu_type`, `topology`, `reason`        | Total number of failed scheduling attempts. `reason` gives the failure cause; `reserve_failed` and `bind_failed` are internal errors. |
| `slice_schedule_success_total`                  | Counter   | `tpu_type`, `topology`, `tpu_pod`       | Total number of successful scheduling attempts.                           |
| `slice_schedule_duration_seconds`               | Histogram | `tpu_type`, `topology`, `success`       | Time taken to schedule a slice (end-to-end `tryScheduleSliceGroup`).      |
| `slice_preemptions_total`                       | Counter   | `tpu_type`, `topology`, `tpu_pod`       | Total number of times a slice caused one or more preemptions.             |
| `preempted_slices_total`                        | Counter   | `tpu_type`, `topology`, `tpu_pod`       | Total number of individual slices that have been preempted. `topology` and `tpu_pod` are the preempted slice's. |
| `slice_queue_depth`                             | Gauge     | `tpu_type`, `topology`                  | Number of slice groups currently waiting in the priority queue.           |
| `slice_scheduler_reconciliation_errors_total`   | Counter   | `error_type`                            | Total number of reconciliation errors (e.g. `GetSliceError`).            |
| `slice_scheduler_queue_latency_seconds`         | Histogram | `tpu_type`, `topology`                  | Time a group spends in the scheduling queue (enqueue to dequeue).        |
| `slice_scheduler_step_duration_seconds`         | Histogram | `tpu_type`, `topology`, `step`          | Time taken by individual scheduling algorithm steps (filter, generate, score). |

### 8.2 Useful PromQL queries

**Scheduling success rate**

```promql
sum(rate(slice_schedule_success_total[5m]))
/
sum(rate(slice_schedule_attempts_total[5m]))
```

**Scheduling errors by reason**

```promql
sum by (reason) (rate(slice_schedule_errors_total[5m]))
```

**Queue depth over time**

```promql
sum by (topology) (slice_queue_depth)
```

**P99 scheduling latency**

```promql
histogram_quantile(0.99,
  sum by (le, topology) (
    rate(slice_schedule_duration_seconds_bucket[5m])
  )
)
```

**P99 queue wait time**

```promql
histogram_quantile(0.99,
  sum by (le, topology) (
    rate(slice_scheduler_queue_latency_seconds_bucket[5m])
  )
)
```

**Preemption rate**

```promql
sum(rate(slice_preemptions_total[5m]))
```

### 8.3 Suggested alerts

| Alert                            | Condition                                                                  | Severity |
|----------------------------------|----------------------------------------------------------------------------|----------|
| Scheduling error rate high       | `rate(slice_schedule_errors_total[5m]) > 0.1` sustained for 10 min         | Warning  |
| Queue depth growing              | `slice_queue_depth > 10` sustained for 5 min                              | Warning  |
| Scheduling latency high          | P99 `slice_schedule_duration_seconds` > 30 s sustained for 5 min           | Warning  |
| Queue wait time high             | P99 `slice_scheduler_queue_latency_seconds` > 60 s sustained for 5 min     | Warning  |
| Preemption storm                 | `rate(preempted_slices_total[5m]) > 1` sustained for 5 min                 | Critical |
| Reconciliation errors            | `rate(slice_scheduler_reconciliation_errors_total[5m]) > 0` sustained for 5 min | Warning  |

---

## 9. FAQ

**Q. Can I have multiple scheduler instances for the same TPU type?**
Use `--leader-elect=true` so only one instance is active at a time.
Running two active instances for the same `--tpu-type` will cause cache
conflicts and double-scheduling.

**Q. Can I run one scheduler for v6e and another for tpu7x?**
Yes. Each instance is scoped to exactly one `--tpu-type`. They share
the same PartitionTree CRs but maintain separate caches and only act
on Slices matching their type.

**Q. What happens if I delete a Slice while it has partitions?**
The scheduler's watch handler detects the deletion, releases the
partitions in the in-memory cache, and removes the slice from its
group. The partitions become available for other workloads.

**Q. Why does my multi-slice workload never get scheduled?**
The scheduler waits for all `num-slices` Slices to exist before
attempting to schedule the group. Verify that all Slices in the
workload have been created and have the same `slice-group-name`
annotation.

**Q. How does the scheduler decide which TPU pod (MIG) to place on?**
The `TightestFitScorer` prefers already-fragmented pods to maximize
bin-packing. The `DegradationSinkScorer` steers degradation-tolerant
workloads to degraded cubes. These combine to balance utilization and
health across the fleet.

**Q. What is the `accelerator.gke.io/provisioned-partition` annotation?**
`slice-controller` stamps this annotation on an active sub-slice to record
the provisioned GCE partition and removes it once N→0 teardown completes.
When `spec.partitionIds` is cleared to `[]`, the scheduler skips 0→M
placement while this annotation remains non-empty (or while a nonzero
`status.observedGeneration` still lags `metadata.generation`) so it does
not race with GCE teardown. While `spec.partitionIds` is non-empty, active
sub-slices are reconciled normally for taint eviction, moved-slice eviction,
gang membership tracking, and failure rescheduling.

**Q. Can I manually assign partitions to a Slice?**
Yes — set `spec.partitionIds` directly. The scheduler's watch handler
will pick up the change and reserve those partitions in the cache. But
if those partitions are already in use, you'll see a conflict log.
Using the scheduler's annotation-driven flow is recommended.

**Q. What does `enable-node-readiness` do?**
When enabled, the scheduler considers Kubernetes Node readiness when
evaluating partitions, as reported by the `NodeReady` condition the
partition-tree controller sets on each leaf partition. A leaf whose
condition is not `True`, or is missing, is treated as unschedulable. When disabled, partitions are evaluated
purely on their PartitionTree state (health, occupancy, sliceId).

**Q. How does the PodReconciler determine which node a pod should go to?**
It resolves `spec.partitionIds` → hostnames via the partition cache,
sorts them deterministically (by partition tree order), and maps the
pod's worker index to a hostname.

**Q. Where can I see what the scheduler is doing?**
Start with the in-cluster signals: Slice events (§7.1), Pod events
(§7.2), and the Prometheus metrics (§8.1). For deeper debugging, tail
the controller logs:

```bash
kubectl -n <namespace> logs -l app=slice-scheduler -f
```

Increase verbosity with `--zap-log-level=2` for detailed cache and
scheduling decisions.

---

## Appendix A: Source Map

| Concept                        | Source file                                             |
|--------------------------------|---------------------------------------------------------|
| Reconcile loop, state machine  | `internal/scheduler/slice_scheduler.go`                 |
| Kubernetes events              | `internal/scheduler/events.go`                          |
| Prometheus metrics             | `internal/metrics/metrics.go`                           |
| Pod binding & eviction         | `internal/pod_reconciler/pod_reconciler.go`             |
| Pod index parsing & helpers    | `internal/pod_reconciler/util.go`                       |
| Partition cache                | `internal/watcher/partition_cache.go`                   |
| Partition node (schedulability)| `internal/watcher/partition_node.go`                    |
| Partition watcher              | `internal/watcher/watcher.go`                           |
| Cache types                    | `internal/watcher/types.go`                             |
| Filter plugins                 | `internal/plugins/filters.go`                           |
| Score plugins                  | `internal/plugins/scorers.go`                           |
| Filtered state evaluation      | `internal/plugins/filtered_state.go`                    |
| Priority queue                 | `internal/pq/priority_queue.go`                         |
| Scheduler interfaces           | `internal/interfaces/scheduler.go`                      |
| Slice CRD types                | `apis/slice/v1beta1/slice_types.go`                     |
| CLI flags & main               | `cmd/main.go`                                           |
| Utility functions              | `internal/scheduler/util.go`                            |

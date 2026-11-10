# Release Notes

## v0.7.0

- Slice Scheduler controller that places `Slice` custom resources onto GCE
  accelerator partitions reported by `PartitionTree` resources.
- Filter, generate, and score placement pipeline with single- and multi-cube
  placement, a priority queue, and priority-based preemption.
- Supports TPU `v6e` and `tpu7x`, with one scheduler instance per accelerator
  type (`--tpu-type`).
- Optional node readiness and `PartitionTree` taint awareness, including
  `NoExecute` slice eviction with configurable toleration windows.
- Optional PodReconciler that binds pods using `schedulerName: slice-scheduler`
  to the nodes of their assigned partitions, and drains pods when a slice is
  evicted.
- Backoff and retry handling for failed or unhealthy slices.
- Helm chart and kustomize deployment.

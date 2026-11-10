# How to Contribute to Slice Scheduler

Please note: This project is currently not accepting external contributions or pull requests. Instead please open an issue and project maintainers will be in touch.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License Agreement (CLA). You (or your employer) retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of the project. Head over to <https://cla.developers.google.com/> to see your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one (even if it was for a different project), you probably don't need to do it again.

## Community Guidelines

This project follows [Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## Development Workflow

### Prerequisites

- **Go**: `v1.25.7+`
- **Docker**: `17.03+` (or Podman)
- **kubectl**: configured against your target cluster
- **Kind**: for e2e tests (`go install sigs.k8s.io/kind@latest`)
- **controller-gen** / **kustomize** / **golangci-lint** / **addlicense**: Managed automatically via the Makefile tools directory (`bin/`)

### Getting Started

```bash
git clone https://github.com/ai-on-gke/slice-scheduler
cd slice-scheduler
make test
```

The Dockerfile builds with Go module vendoring, but `vendor/` is git-ignored. Run `go mod vendor` after cloning, and after any dependency change, before running `make docker-build`.

### Building and Testing

1. **Format and lint code**:
   ```bash
   make fmt
   make vet
   make lint
   ```

2. **Regenerate code and manifests** after changing API types or `+kubebuilder` RBAC markers:
   ```bash
   make manifests generate
   ```
   Never edit files under `config/crd/bases/`, `config/rbac/role.yaml`, or `zz_generated.deepcopy.go` by hand.

3. **Verify license headers**:
   Ensure all source files have appropriate Apache 2.0 copyright headers:
   ```bash
   make check-license
   ```
   To automatically add missing license headers:
   ```bash
   make addlicense
   ```

4. **Run unit and integration tests** (envtest with a real etcd and kube-apiserver):
   ```bash
   make test
   ```

5. **Run e2e tests** (creates a Kind cluster, deploys the controller, and tears the cluster down):
   ```bash
   make test-e2e
   ```

6. **Run presubmit checks**:
   Runs unit tests, linting, and license checks in a single pass:
   ```bash
   make presubmit
   ```

### Code Guidelines

- Follow existing patterns. The codebase uses standard `testing` and envtest for integration tests.
- The scheduling pipeline is modular: add new filters in `internal/plugins/filters.go`, scorers in `internal/plugins/scorers.go`, or generators in `internal/generators/`.
- All cache mutations go through `PartitionCache` methods which hold the `RWMutex`.
- The `solver.Cost` interface requires the **Monotonicity** property: if C1 < C2, then C1 + A ≤ C2 + A.

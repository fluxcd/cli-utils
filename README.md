# Flux kstatus 

[![release](https://img.shields.io/github/v/tag/fluxcd/cli-utils?label=release)](https://github.com/fluxcd/cli-utils/tags)
[![license](https://img.shields.io/github/license/fluxcd/cli-utils.svg)](https://github.com/fluxcd/cli-utils/blob/main/LICENSE)
[![test](https://github.com/fluxcd/cli-utils/workflows/test/badge.svg)](https://github.com/fluxcd/cli-utils/actions)

This repository is a hard fork of [kubernetes-sigs/cli-utils](https://github.com/kubernetes-sigs/cli-utils).

We've forked `cli-utils` in 2023 as the upstream repo lagged months behind Kubernetes & Kustomize,
which [blocked](https://github.com/fluxcd/flux2/issues/3564) our ability to use the latest Kustomize features in Flux.
We have since made a number of improvements to the [kstatus](pkg/kstatus) package
specific to Flux's needs, and we intend to maintain this fork for the foreseeable future.

Besides Flux, the `kstatus` fork is also used by other projects such as Helm, Cilium, Pulumi, Talos and
many others. The Flux maintainers are committed to maintaining it as a standalone library
for the benefit of the wider Kubernetes ecosystem.

## kstatus

The `kstatus` package provides utilities for computing the status of Kubernetes resources,
and for polling the cluster to determine when resources have reached a ready state or have stalled.

This package is used to determine the health and readiness of resources managed by Flux controllers,
and to provide feedback to users about the state of their GitOps pipelines.

Instead of using `kstatus` directly, we recommend using
the [github.com/fluxcd/pkg/ssa](https://github.com/fluxcd/pkg/tree/main/ssa) package
that offers a high level API for applying resources and waiting for them to be ready:

```go
// WaitForSetWithContext checks if the given ObjMetadataSet has been fully reconciled.
// The provided context can be used to cancel the operation.
func (m *ResourceManager) WaitForSetWithContext(ctx context.Context, set object.ObjMetadataSet, opts WaitOptions) error
```

### CEL Extensions

While the `kstatus` package can determine the status of native Kubernetes resources (e.g. a Deployment)
and Flux custom resources that subscribe to the "abnormal-true" polarity pattern (`Reconciling` and `Stalled` conditions),
it may not be able to determine the status of other custom resources in a reliable manner.

To overcome the limitations of kstatus, we have extended it with CEL expressions to enable Flux users
to define the logic for computing the status which is evaluated against the resource's status fields.
This allows accurate status computation for custom resources regardless of the conditions they use,
and it also allows for more complex logic to be implemented in the status computation.

References:

- [Flux Kustomization health expressions docs](https://fluxcd.io/flux/components/kustomize/kustomizations/#health-check-expressions)
- [Flux HelmRelease health expressions docs](https://fluxcd.io/flux/components/helm/helmreleases/#health-check-expressions)
- [CEL extensions for kstatus source code](https://github.com/fluxcd/pkg/tree/main/runtime/cel)

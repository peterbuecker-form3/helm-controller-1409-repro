# Reproduction case for fluxcd/helm-controller#1409

See [fluxcd/helm-controller#1409](https://github.com/fluxcd/helm-controller/issues/1409) for a detailed description of the issue being reproduced here.

## Requirements

The following tools must be available to run the reproduction case:

- make
- [Go](https://go.dev/)
- [kind](https://kind.sigs.k8s.io/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Flux](https://fluxcd.io/flux/installation/) (`flux` CLI)

## Description

The reproduction case may be run via:

```sh
make repro
```

This will:

- create a kind cluster
- install Flux
- run the reproduction case

Key parameters that can be tuned are:

- `HELM_RELEASE_COUNT`: the number of `HelmRelease` objects to deploy concurrently (default: 1)
- `PATCH_STORM_WORKERS`: the number of concurrent patch-storm workers to use (default: 8)

For example:

```sh
make repro HELM_RELEASE_COUNT=2 PATCH_STORM_WORKERS=4
```

To just run the test case against an already created kind cluster, run:

```sh
make test
```

## The reproduction case

The reproduction case tries to force a race condition inside `helm-controller` where a helm release is fully installed from a Helm point of view, but the status conditions on the `HelmRelease` object are not updated to reflect that.

This is achieved by continuously updating annotations on the target `HelmRelease` objects to cause the informer cache in `helm-controller` to fall behind, which will cause `helm-controller` to fail patching the status conditions due to conflicts.

## Cleanup

To cleanup and delete the kind cluster, run:

```sh
make clean
```

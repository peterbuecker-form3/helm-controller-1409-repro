# Reproduction case for fluxcd/helm-controller#1409

See [fluxcd/helm-controller#1409](https://github.com/fluxcd/helm-controller/issues/1409) for a detailed description of the issue being reproduced here.

## Description

The reproduction case may be run against a kind cluster via:

```sh
make repro
```

This will:

- create a kind cluster
- install Flux
- make the podinfo chart available
- run the reproduction case

## Reproducing the issue

Run `make repro` to start from scratch, or `make test` to re-run the reproduction on an existing kind cluster. The reproduction will be attempted up to 10 times, and will log `issue was reproduced` in case of success.

The reproduction case tries to force a race condition inside `helm-controller` where a helm release is fully installed, but the status conditions on the `HelmRelease` object are not updated to reflect the successful installation.

This is achieved by continuously updating annotations on target `HelmRelease` object(s) to continuously increase `metadata.resourceVersion`, which in turns increases the likelihood of `helm-controller` hitting conflicts while attempting to update the status conditions.

By default, `make test` will deploy a single `HelmRelease` object and patch it with 8 concurrent patch-storm workers. Please see the `Makefile` for more details.

## Cleanup

To cleanup and delete the kind cluster, run:

```sh
make clean
```

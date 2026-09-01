# Repro for https://github.com/fluxcd/helm-controller/issues/1409

CLUSTER_NAME ?= repro-1409
NODE_IMAGE   ?= kindest/node:v1.34.0
FLUX_VERSION ?= v2.9.4

# HelmReleases per attempt, and concurrent annotation patchers on each.
HELM_RELEASE_COUNT  ?= 1
PATCH_STORM_WORKERS ?= 8

# Optional. Behind a TLS-intercepting proxy, pass the CA so source-controller
# can fetch the chart index: make repro CA_BUNDLE_PATH=/path/to/ca.crt
CA_BUNDLE_PATH ?=

# Must match FLUX_VERSION. Required in case of TLS-intercepting proxies interfering
# with the kind cluster.
PRELOAD_IMAGES := \
	ghcr.io/fluxcd/helm-controller:v1.6.3 \
	ghcr.io/fluxcd/source-controller:v1.9.4

.PHONY: help
help:
	@echo "make repro   create kind cluster, install Flux, run reproduction case"
	@echo "make test    run reproduction case against the current cluster"
	@echo "make clean   delete the kind cluster"

.PHONY: repro
repro: cluster flux test

.PHONY: test
test: guard-context
	go run . -helm-release-count=$(HELM_RELEASE_COUNT) -patch-storm-workers=$(PATCH_STORM_WORKERS)

.PHONY: cluster
cluster:
	kind get clusters | grep -qx $(CLUSTER_NAME) || \
		kind create cluster --name $(CLUSTER_NAME) --image $(NODE_IMAGE)

.PHONY: flux
flux: guard-context $(if $(strip $(CA_BUNDLE_PATH)),preload)
	flux install --version=$(FLUX_VERSION) \
		--components=source-controller,helm-controller \
		--log-level=debug

.PHONY: preload
preload:
	@for i in $(PRELOAD_IMAGES); do \
		docker pull -q "$$i" || { echo "host pull failed for $$i"; exit 1; }; \
		kind load docker-image "$$i" --name $(CLUSTER_NAME); \
	done

.PHONY: guard-context
guard-context:
	@ctx=$$(kubectl config current-context 2>/dev/null); \
	if [ "$$ctx" != "kind-$(CLUSTER_NAME)" ]; then \
		echo "refusing to run: kubectl context is '$$ctx', expected 'kind-$(CLUSTER_NAME)'"; \
		echo "run 'make cluster' first"; \
		exit 1; \
	fi

.PHONY: clean
clean:
	kind delete cluster --name $(CLUSTER_NAME)

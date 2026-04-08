# Copyright 2026 The Flux Authors.
# SPDX-License-Identifier: Apache-2.0

# Get the currently used golang install path
# (in GOPATH/bin, unless GOBIN is set).
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: tidy vet fmt lint test ## Run the full verification pipeline (tidy, vet, fmt, lint, test).

##@ Development

.PHONY: fmt
fmt: ## Format Go source files in-place with gofmt.
	go fmt ./...

.PHONY: lint
lint: golangci-lint ## Run golangci-lint against the whole module.
	$(GOLANGCI_LINT) run

.PHONY: tidy
tidy: ## Sync go.mod and go.sum with the module's imports.
	go mod tidy

.PHONY: test
test: ## Run unit tests under ./pkg/... with the race detector and coverage enabled.
	go test -race -cover ./pkg/...

.PHONY: vet
vet: ## Run go vet to catch suspicious constructs.
	go vet ./...

.PHONY: scan
scan: govulncheck ## Scan the module for known vulnerabilities.
	@$(GOVULNCHECK) ./...

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GOVULNCHECK = $(LOCALBIN)/govulncheck-$(GOVULNCHECK_VERSION)

# Pinned version of golangci-lint; bump here to upgrade the linter across CI and local runs.
GOLANGCI_LINT_VERSION ?= v2.11.4
.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# Pinned version of govulncheck; bump here to upgrade the scanner across CI and local runs.
GOVULNCHECK_VERSION ?= v1.1.4
.PHONY: govulncheck
govulncheck: $(GOVULNCHECK) ## Install govulncheck locally if necessary.
$(GOVULNCHECK): $(LOCALBIN)
	$(call go-install-tool,$(GOVULNCHECK),golang.org/x/vuln/cmd/govulncheck,$(GOVULNCHECK_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary (ideally with version)
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f $(1) ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv "$$(echo "$(1)" | sed "s/-$(3)$$//")" $(1) ;\
}
endef

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

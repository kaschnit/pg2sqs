.DEFAULT_GOAL := help

## Shell config
SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

## Tools
GO ?= go
GOLANGCI_LINT ?= $(GO) tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint
KO ?= $(GO) tool github.com/google/ko
KUBECTL ?= $(GO) tool k8s.io/kubernetes/cmd/kubectl
KIND ?= $(GO) tool sigs.k8s.io/kind
HELM ?= $(GO) tool helm.sh/helm/v4/cmd/helm

MODULE = $(shell $(GO) list -m)

# KIND
KIND_CLUSTER_NAME := "kind-pg2sqs-test"

# Command package
CMD := $(CURDIR)/cmd/pg2sqs

## Location for build artifacts
BUILD_DIR := $(CURDIR)/build

# Image
IMG_DIR := $(BUILD_DIR)/image
IMG_TAR_FILE := $(IMG_DIR)/pg2sqs.tar

# Helm
CHART_DIR := $(CURDIR)/charts/pg2sqs

$(BUILD_DIR):
	mkdir -p "$(BUILD_DIR)"

$(IMG_DIR):
	mkdir -p "$(IMG_DIR)"

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: clean
clean: ## Clean up files.
	find . -name .DS_Store -type f -delete
	rm -rf $(BUILD_DIR)

##@ Development

.PHONY: generate
generate: generate-code ## Generate files.

.PHONY: generate-code
generate-code: go-generate ## Generate code.

.PHONY: go-generate
go-generate: TARGET := ./...
go-generate: ## Run go generate.
	$(GO) generate $(TARGET)

.PHONY: check
check: generate go-tidy-check go-fix-check lint test envtest # Run all automated checks.

.PHONY: fix
fix: generate go-tidy go-fix lint-fix # Run all automated fixes.

.PHONY: go-tidy
go-tidy: generate ## Tidy go.mod and go.sum.
	$(GO) mod tidy

.PHONY: go-tidy-check
go-tidy-check: generate ## Check if go.mod and go.sum are tidy.
	$(GO) mod tidy --diff

.PHONY: go-fix
go-fix: generate ## Run go fix
	$(GO) fix ./...

.PHONY: go-fix-checl
go-fix-check: generate ## Run go fix
	$(GO) fix -diff ./...

.PHONY: go-mod-download
go-mod-download: generate ## Download dependencies from go.mod and go.sum.
	$(GO) mod download

.PHONY: install-deps
install-deps: go-mod-download ## Install dependencies.

.PHONY: lint
lint: generate ## Run linters.
	$(GOLANGCI_LINT) run ./...

.PHONY: lint-fix
lint-fix: generate ## Run linters and perform fixes.
	$(GOLANGCI_LINT) run --fix ./...

.PHONY: test
test: TESTFLAGS := -v -race
test: TESTTARGET := ./...
test: generate ## Run unit tests.
	$(GO) test -tags=unit $(TESTFLAGS) $(TESTTARGET)

##@ Build

.PHONY: build
build: generate $(BUILD_DIR) ## Build manager binary.
	$(GO) build -o $(BUILD_DIR)/pg2sqs $(CMD)

.PHONY: run
run: generate ## Run the application.
	$(GO) run $(CMD)

.PHONY: image
image: PUSH := false
image: generate $(IMG_DIR) ## Build an image and optionally push it.
	KO_DOCKER_REPO=pg2sqs \
		$(KO) build \
			--push=$(PUSH) \
			--platform=linux/$(shell $(GO) env GOARCH) \
			--tarball=$(IMG_TAR_FILE) \
			--bare \
			--tags=development \
			$(CMD)

.PHONY: kind-delete
kind-delete: ## Delete the KIND testing cluster.
	$(KIND) delete cluster --name "$(KIND_CLUSTER_NAME)"

.PHONY: kind-create
kind-create: kind-delete ## Create a KIND cluster for testing.
	$(KIND) create cluster --name "$(KIND_CLUSTER_NAME)"

.PHONY: kind-deploy
kind-deploy: generate image kind-create ## Deploy the application to a KIND cluster for testing.
	$(KIND) load image-archive $(IMG_TAR_FILE) --name "$(KIND_CLUSTER_NAME)"
	$(HELM) install pg2sqs $(CHART_DIR) \
		--values test/kind/values.yaml \
		--namespace pg2sqs \
		--create-namespace
	$(KUBECTL) apply -f test/kind/base

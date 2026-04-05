GOTOOLCHAIN ?= go1.26.1
GO ?= go
BIN_DIR ?= ./bin
GOBIN ?= $(shell $(GO) env GOBIN)

export GOTOOLCHAIN

ifeq ($(GOBIN),)
GOBIN := $(shell $(GO) env GOPATH)/bin
endif

GOLANGCI_LINT ?= $(GOBIN)/golangci-lint
COVERAGE_OUT ?= coverage.out
COVERAGE_MIN ?= 90.0
PACKAGE_LIST_CMD = $(GO) list ./... | grep -v '/test/'
PACMAN_TEST_IMAGE ?= pacman-test:local
PACMAN_TEST_PGEXT_IMAGE ?= pacman-pgext-postgres:local
DOCKER_BUILD_PROGRESS ?= plain
GO_TEST_INTEGRATION_FLAGS ?= -v
PG_EXTENSION_DIR ?= ./postgresql/pacman_agent
PG_EXTENSION_IMAGE ?= postgres:17-bookworm
PG_EXTENSION_OUTPUT ?= $(BIN_DIR)/pg-extension
PG_CONFIG ?= pg_config

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X github.com/polkiloo/pacman/internal/version.Version=$(VERSION) \
	-X github.com/polkiloo/pacman/internal/version.Commit=$(COMMIT) \
	-X github.com/polkiloo/pacman/internal/version.BuildDate=$(BUILD_DATE)

.PHONY: fmt test test-integration docker-build-test-image docker-build-pgext-image coverage coverage-check lint lint-install build build-pacmand build-pacmanctl build-pg-extension package-pg-extension install-pg-extension clean-pg-extension tidy clean openapi-codegen-check

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

openapi-codegen-check:
	@for spec in \
		docs/openapi/components-meta.yaml \
		docs/openapi/schemas-enums.yaml \
		docs/openapi/schemas-patroni.yaml \
		docs/openapi/schemas-operations.yaml \
		docs/openapi/schemas-cluster.yaml \
		docs/openapi/paths-probes.yaml \
		docs/openapi/paths-patroni.yaml \
		docs/openapi/paths-pacman.yaml; do \
		tmp=$$(mktemp); \
		trap 'rm -f "$$tmp"' EXIT; \
		$(GO) tool oapi-codegen -config docs/openapi/oapi-codegen.yaml -o "$$tmp" "$$spec" || exit $$?; \
		rm -f "$$tmp"; \
	done

docker-build-test-image:
	docker build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-runner.Dockerfile -t $(PACMAN_TEST_IMAGE) .

docker-build-pgext-image:
	docker build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-pgext-postgres.Dockerfile -t $(PACMAN_TEST_PGEXT_IMAGE) .

test-integration: docker-build-test-image docker-build-pgext-image
	PACMAN_TEST_IMAGE=$(PACMAN_TEST_IMAGE) PACMAN_TEST_PGEXT_IMAGE=$(PACMAN_TEST_PGEXT_IMAGE) $(GO) test $(GO_TEST_INTEGRATION_FLAGS) -tags=integration ./test/...

coverage:
	@packages="$$( $(PACKAGE_LIST_CMD) )"; \
	if [ -z "$$packages" ]; then \
		echo "failed to resolve coverage package list" >&2; \
		exit 1; \
	fi; \
	$(GO) test -coverprofile=$(COVERAGE_OUT) $$packages

coverage-check: coverage
	@coverage=$$($(GO) tool cover -func=$(COVERAGE_OUT) | awk '/^total:/ { gsub("%", "", $$3); print $$3 }'); \
	if awk "BEGIN { exit !($$coverage > $(COVERAGE_MIN)) }"; then \
		printf 'coverage %s%% is above %s%%\n' "$$coverage" "$(COVERAGE_MIN)"; \
	else \
		printf 'coverage %s%% must be above %s%%\n' "$$coverage" "$(COVERAGE_MIN)" >&2; \
		exit 1; \
	fi

lint:
	$(GOLANGCI_LINT) run

lint-install:
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

build: build-pacmand build-pacmanctl
# The PostgreSQL extension is built inside a Docker container.
# See postgresql/pacman_agent/Makefile.docker. Run: make build-pg-extension

PG_EXTENSION_MAKE = $(MAKE) -C $(PG_EXTENSION_DIR) -f Makefile.docker \
	PG_EXTENSION_IMAGE="$(PG_EXTENSION_IMAGE)" \
	PG_CONFIG="$(PG_CONFIG)" \
	OUTPUT_DIR="$(abspath $(PG_EXTENSION_OUTPUT))" \
	PACMAND_BIN="$(abspath $(BIN_DIR)/pacmand)"

build-pg-extension:
	$(PG_EXTENSION_MAKE) build

package-pg-extension: build-pg-extension build-pacmand
	$(PG_EXTENSION_MAKE) package

install-pg-extension: package-pg-extension
	$(PG_EXTENSION_MAKE) install

clean-pg-extension:
	$(PG_EXTENSION_MAKE) clean

build-pacmand:
	mkdir -p $(BIN_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pacmand ./cmd/pacmand

build-pacmanctl:
	mkdir -p $(BIN_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pacmanctl ./cmd/pacmanctl

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN_DIR)

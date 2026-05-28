GOTOOLCHAIN ?= go1.26.1
GO ?= go
BIN_DIR ?= ./bin
GOBIN ?= $(shell $(GO) env GOBIN)
CONTAINER_RUNTIME ?= docker
RPM_BUILDER_IMAGE ?= pacman-rpm-builder:el9
RPM_BUILDER_BASE_IMAGE ?= rockylinux:9
RPM_BUILDER_DOCKER_RETRY_ATTEMPTS ?= 5
RPM_BUILDER_DOCKER_RETRY_DELAY_SECONDS ?= 10
RPM_OUTPUT_DIR ?= $(CURDIR)/bin/rpm
RPM_VERSION ?= 0.1.0
RPM_RELEASE ?= 1
RPM_SOURCE_DATE_EPOCH ?= $(shell git log -1 --pretty=%ct 2>/dev/null || date +%s)
RPM_VALIDATE_RELEASE1_DIR ?= $(CURDIR)/bin/rpm/release1
RPM_VALIDATE_RELEASE2_DIR ?= $(CURDIR)/bin/rpm/release2
RPM_VALIDATION_IMAGE ?= rockylinux:9

export GOTOOLCHAIN

ifeq ($(GOBIN),)
GOBIN := $(shell $(GO) env GOPATH)/bin
endif

GOLANGCI_LINT ?= $(GOBIN)/golangci-lint
COVERAGE_OUT ?= coverage.out
COVERAGE_MIN ?= 90.0
PACMAN_TEST_IMAGE ?= pacman-test:local
PACMAN_TEST_PGEXT_IMAGE ?= pacman-pgext-postgres:local
PACMAN_TEST_POSTGRES_IMAGE ?= $(PACMAN_TEST_PGEXT_IMAGE)
PACMAN_ANSIBLE_INSTALL_IMAGE ?= pacman-ansible-install:local
PACMAN_ANSIBLE_INSTALL_RPM_DIR ?= $(CURDIR)/bin/ansible-install-rpm
DOCKER_BUILD_PROGRESS ?= plain
GO_TEST_INTEGRATION_FLAGS ?= -v
GO_TEST_INTEGRATION_PACKAGE ?= ./test/integration
GO_TEST_INSTALL_PACKAGE ?= ./test/installintegration
TESTCONTAINERS_RYUK_DISABLED ?=
JEPSEN_CI_SCRIPT ?= go run ./tools/jepsenctl run ci
JEPSEN_DOCKER_SCRIPT ?= go run ./tools/jepsenctl run docker
PG_EXTENSION_DIR ?= ./postgresql/pacman_agent
PG_EXTENSION_IMAGE ?= postgres:17-bookworm
PG_EXTENSION_OUTPUT ?= $(BIN_DIR)/pg-extension
PG_CONFIG ?= pg_config

INTEGRATION_TEST_ENV = PACMAN_TEST_IMAGE=$(PACMAN_TEST_IMAGE) \
	PACMAN_TEST_PGEXT_IMAGE=$(PACMAN_TEST_PGEXT_IMAGE) \
	PACMAN_TEST_POSTGRES_IMAGE=$(PACMAN_TEST_POSTGRES_IMAGE) \
	PACMAN_ANSIBLE_INSTALL_IMAGE=$(PACMAN_ANSIBLE_INSTALL_IMAGE) \
	TESTCONTAINERS_RYUK_DISABLED=$(TESTCONTAINERS_RYUK_DISABLED)

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X github.com/polkiloo/pacman/internal/version.Version=$(VERSION) \
	-X github.com/polkiloo/pacman/internal/version.Commit=$(COMMIT) \
	-X github.com/polkiloo/pacman/internal/version.BuildDate=$(BUILD_DATE)

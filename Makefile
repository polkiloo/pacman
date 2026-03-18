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
COVERAGE_MIN ?= 80.0

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X github.com/polkiloo/pacman/internal/version.Version=$(VERSION) \
	-X github.com/polkiloo/pacman/internal/version.Commit=$(COMMIT) \
	-X github.com/polkiloo/pacman/internal/version.BuildDate=$(BUILD_DATE)

.PHONY: fmt test coverage coverage-check lint lint-install build build-pacmand build-pacmanctl tidy clean

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

coverage:
	$(GO) test -coverprofile=$(COVERAGE_OUT) ./...

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

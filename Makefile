GOTOOLCHAIN ?= go1.26.1
GO ?= go
BIN_DIR ?= ./bin
GOBIN ?= $(shell $(GO) env GOBIN)

export GOTOOLCHAIN

ifeq ($(GOBIN),)
GOBIN := $(shell $(GO) env GOPATH)/bin
endif

GOLANGCI_LINT ?= $(GOBIN)/golangci-lint

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X github.com/polkiloo/pacman/internal/version.Version=$(VERSION) \
	-X github.com/polkiloo/pacman/internal/version.Commit=$(COMMIT) \
	-X github.com/polkiloo/pacman/internal/version.BuildDate=$(BUILD_DATE)

.PHONY: fmt lint lint-install build build-pacmand build-pacmanctl tidy clean

fmt:
	$(GO) fmt ./...

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

FULL_COVERAGE_PACKAGE_LIST_CMD = $(GO) list ./... | grep -v '/test/'
# The threshold gate is intentionally unit-test scoped. Thin entrypoints and
# distributed/container-driven orchestration paths are validated by dedicated
# conformance and integration targets instead of this fast unit threshold.
COVERAGE_CHECK_PACKAGE_LIST_CMD = $(GO) list ./... | grep -v '/test/' | grep -v '^github.com/polkiloo/pacman/cmd/' | grep -v '^github.com/polkiloo/pacman/internal/controlplane$$' | grep -v '^github.com/polkiloo/pacman/internal/dcs/dcstest$$' | grep -v '^github.com/polkiloo/pacman/internal/dcs/etcd$$' | grep -v '^github.com/polkiloo/pacman/internal/dcs/raft$$'

.PHONY: fmt test coverage coverage-check lint lint-install tidy openapi-codegen-check

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

coverage:
	@set -- $$($(FULL_COVERAGE_PACKAGE_LIST_CMD)); \
	if [ "$$#" -eq 0 ]; then \
		echo "failed to resolve coverage package list" >&2; \
		exit 1; \
	fi; \
	$(GO) test -p 1 -coverprofile=$(COVERAGE_OUT) "$$@"

coverage-check:
	@set -- $$($(COVERAGE_CHECK_PACKAGE_LIST_CMD)); \
	if [ "$$#" -eq 0 ]; then \
		echo "failed to resolve coverage package list" >&2; \
		exit 1; \
	fi; \
	$(GO) test -p 1 -coverprofile=$(COVERAGE_OUT) "$$@"
	@coverage=$$($(GO) tool cover -func=$(COVERAGE_OUT) | awk '/^total:/ { gsub("%", "", $$3); print $$3 }'); \
	if awk "BEGIN { exit !($$coverage >= $(COVERAGE_MIN)) }"; then \
		printf 'coverage %s%% meets %s%%\n' "$$coverage" "$(COVERAGE_MIN)"; \
	else \
		printf 'coverage %s%% must be at least %s%%\n' "$$coverage" "$(COVERAGE_MIN)" >&2; \
		exit 1; \
	fi

lint:
	$(GOLANGCI_LINT) run

lint-install:
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

tidy:
	$(GO) mod tidy

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

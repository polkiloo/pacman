---
name: test-selection
description: Choose the smallest relevant pacman test set from changed files.
---

# Test Selection

Pick the smallest test that can fail for the change, then widen only when
contracts cross package boundaries.

- `internal/controlplane/**`: `go test -count=1 ./internal/controlplane`.
- `internal/cluster/**`: `go test -count=1 ./internal/cluster`; add
  `./internal/controlplane` for state or operation semantics.
- `internal/agent/**`: `go test -count=1 ./internal/agent`; add
  `./internal/postgres` when PostgreSQL probes/actions changed.
- `internal/postgres/**`: `go test -count=1 ./internal/postgres`; run targeted
  integration tests only for real PostgreSQL/container behavior.
- `internal/dcs/**`: package test plus `make test-dcs-conformance` for backend
  contract changes.
- `internal/httpapi/**`, `internal/api/**`, `docs/openapi/**`: API package
  tests; add docs/OpenAPI checks if schemas or generated contract files changed.
- `tools/jepsenctl/**`, `jepsen/**`, `mk/jepsen.mk`: `go test -count=1
  ./tools/jepsenctl/cmd`; run the smallest Jepsen make target only when harness
  behavior changed.
- `deploy/**`, `packaging/**`, `mk/**`: use the matching Make target; avoid
  full integration runs unless deployment behavior changed.
- `test/integration/**`, `test/installintegration/**`: run the named test with
  `-run` before the whole package.

Always run `git diff --check` before finishing. Use `go test ./...` for broad
model/API changes or when package boundaries are uncertain.

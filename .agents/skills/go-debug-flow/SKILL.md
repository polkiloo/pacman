---
name: go-debug-flow
description: Minimal Go build, test, debug, and lint workflow for pacman.
---

# Go Debug Flow

Start narrow. Run package-level tests before broader targets.

1. Identify the package from changed files.
2. Run targeted tests:
   - `go test -count=1 ./internal/controlplane`
   - `go test -count=1 ./internal/agent`
   - `go test -count=1 ./tools/jepsenctl/cmd`
3. For a specific test, use `-run '^TestName$' -v`.
4. For flakes or race-prone logic, rerun the smallest package/test with
   `-count=20` before widening scope.
5. Use `go test ./...` only after targeted tests pass or when shared contracts
   changed.
6. Build binaries with repo Make targets when packaging/link flags matter;
   otherwise `go test` usually compiles enough.
7. Run `make lint` or `golangci-lint run` when touching style-sensitive or
   shared packages.

Debugging hints:

- Preserve context cancellation and deadlines in tests.
- Prefer table-driven cases for state-machine and API validation.
- Use existing test helpers in the package before adding new fixtures.
- Check generated/OpenAPI/package artifacts only when the touched subsystem owns
  them.

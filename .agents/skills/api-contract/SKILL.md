---
name: api-contract
description: Maintain PACMAN-native and Patroni-compatible API contracts.
---

# API Contract

Use when touching HTTP handlers, API models, OpenAPI, or Patroni compatibility.

Primary locations:

- `internal/httpapi`: handlers, middleware, server behavior.
- `internal/api`, `internal/api/native`: shared API contract types.
- `docs/api-contract.md`: contract guidance.
- `docs/openapi/**`, `docs/openapi.yaml`: OpenAPI source and combined spec.
- `internal/app/pacmanctl`: client-facing command behavior.

Rules:

- Keep handlers, request/response models, OpenAPI schemas, examples, and tests
  aligned.
- Preserve PACMAN-native API behavior unless the change is intentional.
- Patroni-compatible endpoints should match expected shape, status codes, and
  auth semantics documented in compatibility tests.
- Unsupported Patroni config patches must fail explicitly, not silently drift.
- Redact secrets in logs and errors.
- Add or update focused handler/model tests for contract changes.

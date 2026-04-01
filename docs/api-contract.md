# API Contract Review

This note records the current PACMAN API contract decisions for the `11. API / Contract`
backlog slice.

## Review Outcome

The OpenAPI draft in [openapi.yaml](/Users/maikllevitskiy/project/pacman/docs/openapi.yaml)
has been aligned with:

- the current PACMAN domain model in `internal/cluster`,
  `internal/controlplane`, and `internal/agent/model`;
- Patroni's documented REST API semantics in
  `docs/rest_api.rst`.

Key review decisions:

- PACMAN keeps its native `/api/v1/*` control-plane API for PACMAN-aware clients.
- PACMAN also defines Patroni-compatible top-level endpoints so existing health
  checks, monitoring integrations, and administrative clients can migrate with
  minimal request-shape changes.
- `GET /api/v1/cluster` is the control-plane projection of `cluster.ClusterStatus`.
- `GET /api/v1/cluster/spec` is the persisted desired state `cluster.ClusterSpec`.
- `GET /api/v1/members` returns the aggregated `cluster.MemberStatus` view.
- `GET /api/v1/nodes/{nodeName}` returns the node-local `agent/model.NodeStatus` view,
  including PostgreSQL details and local control-plane publish status.
- Patroni-compatible endpoints now include:
  `/`, `/primary`, `/read-write`, `/leader`, `/standby-leader`, `/standby_leader`,
  `/replica`, `/read-only`, `/synchronous`, `/sync`, `/read-only-sync`, `/quorum`,
  `/read-only-quorum`, `/asynchronous`, `/async`, `/health`, `/liveness`,
  `/readiness`, `/patroni`, `/metrics`, `/cluster`, `/history`, `/config`,
  `/switchover`, `/failover`, `/restart`, `/reload`, and `/reinitialize`.
- `POST /api/v1/operations/switchover` remains the PACMAN-native switchover API.
- `POST /api/v1/operations/failover` remains the PACMAN-native failover API.
- Patroni-compatible routes that intentionally share a PACMAN-native core workflow
  are tagged in the OpenAPI contract with `x-pacman-native-operation`.
- Patroni-compatible routes that are expected to be callable by `patronictl`
  against a PACMAN node are tagged with `x-patronictl-compatible`.
- Patroni-compatible `POST /switchover` uses the Patroni request shape with
  required `leader` plus optional `candidate` and `scheduled_at`.
- Patroni-compatible `POST /failover` uses the Patroni request shape with required
  `candidate` and optional `leader`.
- Rejoin remains an internal control-plane workflow in v1 and is not exposed as a
  public mutating API.

## Safety Rules Reflected In The Contract

The API contract must not offer operator inputs that bypass the failover and
switchover safety model already encoded in the control plane.

Required invariants:

- Manual failover does not bypass quorum checks, lag limits, timeline checks,
  `no_failover` policy, or fencing requirements.
- Switchover accepts only a ready standby and still validates current-primary
  health, target readiness, and scheduling policy.
- Patroni-compatible `POST /failover` keeps PACMAN safety gates even though
  Patroni may allow riskier operator-driven promotion in some degraded cases.
- Patroni-compatible probe endpoints preserve Patroni request semantics such as
  human-readable lag values, replica-state checks, and free-form tag filters
  where the contract can express them.
- Topology-changing requests are explicit operations and return an `Operation`
  record instead of mutating hidden cluster state.
- Maintenance mode remains a first-class resource because it changes safety
  behavior for topology operations.

## Patroni Compatibility Notes

The migration goal is request and response compatibility where practical, while
keeping PACMAN's stronger control-plane safety model.

Compatibility guarantees in the contract:

- Patroni probe and monitoring paths remain top-level and use Patroni naming.
- Patroni administrative paths keep Patroni request-body field names such as
  `scheduled_at`, `restart_pending`, and `from-leader`.
- Lag query parameters accept Patroni-style human-readable values.
- Patroni probe aliases such as `/sync`, `/async`, and `/standby_leader` are
  explicitly present.

Intentional differences from Patroni:

- PACMAN also exposes a native `/api/v1/*` API instead of forcing all clients to
  use Patroni-style payloads.
- PACMAN will not expose an unsafe API contract that bypasses quorum, timeline,
  lag, or fencing rules simply because Patroni permits some of those flows.
- Authentication is required for mutating administrative endpoints even on the
  Patroni-compatible path surface.

## Authentication And Authorization Model

PACMAN v1 uses two access modes:

- Unauthenticated probe and monitoring access for Patroni-compatible read paths such
  as `/health`, `/liveness`, `/readiness`, `/primary`, `/replica`, `/patroni`,
  `/metrics`, `/cluster`, and `/history`. These are intended for health checks,
  load balancers, and monitoring, and should normally be protected by network
  policy or local binding rather than application credentials.
- Authenticated administrative access for mutating Patroni-compatible endpoints
  such as `/config`, `/switchover`, `/failover`, `/restart`, `/reload`, and
  `/reinitialize`.
- Authenticated control-plane access for every `/api/v1/*` endpoint.

Supported authentication mechanisms:

- Bearer token authentication for `pacmanctl` and automation clients.
- Mutual TLS client authentication for trusted cluster members and tightly
  controlled internal automation.

Authorization model:

- Read-only endpoints require a principal allowed to read cluster state.
- Mutating endpoints such as maintenance changes and topology operations require
  an operator/admin-capable principal.
- Authentication failures return `401 Unauthorized`.
- Authorization failures return `403 Forbidden`.

The exact token issuer, certificate authority, and role mapping are deployment
concerns and will be implemented separately under the security backlog.

## Versioning And Compatibility Policy

PACMAN uses path-major versioning for the external control-plane API:

- Versioned management endpoints live under `/api/v1`.
- Patroni-compatible endpoints stay top-level and keep Patroni naming for
  migration stability.

Compatibility rules within a major version:

- New optional fields, new endpoints, and new non-breaking response metadata may
  be added in the same major version.
- Existing fields will not be removed, renamed, or have their meaning changed in
  place.
- Existing success and failure status-code semantics will not be repurposed.
- Breaking request or response changes require a new major path such as `/api/v2`.

Client obligations for forward compatibility:

- Treat unknown object fields as ignorable.
- Treat unknown enum values as opaque strings and fail soft where possible.
- Treat operation IDs and history IDs as opaque identifiers.

Deprecation policy:

- A deprecated endpoint or field must be documented before removal.
- PACMAN should emit `Deprecation` and `Sunset` headers once the HTTP layer is
  implemented.
- A deprecated behavior must remain available for at least one minor release and
  at least 90 days before removal in the next major version.

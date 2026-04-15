# PostgreSQL Background Worker Extension

This document defines the first concrete boundary for PACMAN's PostgreSQL
background-worker mode.

## Boundary

PACMAN now has two distinct bootstrap layers around one shared local-agent
runtime:

- `pacmand` process mode:
  parses flags, loads YAML, and calls [`internal/app/localagent`](../internal/app/localagent).
- PostgreSQL extension mode:
  is responsible for `shared_preload_libraries` registration, defining the
  `pacman.*` GUC surface, and translating those settings into the same
  node-runtime config before handing off to the shared local-agent lifecycle.

The shared local-agent core remains in:

- [`internal/agent`](../internal/agent)
- [`internal/app/localagent`](../internal/app/localagent)

That keeps both bootstraps thin and avoids duplicating heartbeat, local
PostgreSQL observation, bootstrap-cluster-spec handling, or control-plane
publication logic.

## Extension Identity

- Extension name: `pacman_agent`
- `shared_preload_libraries` entry: `pacman_agent`
- PostgreSQL support policy: PostgreSQL `17.x` only for now
- Repository layout: [`postgresql/pacman_agent`](../postgresql/pacman_agent)

Installed layout is expected to be:

- shared library: `$libdir/pacman_agent.so`
- control file: `$sharedir/extension/pacman_agent.control`
- SQL install script: `$sharedir/extension/pacman_agent--0.1.0.sql`

## GUC Bridge

The extension defines these postmaster-level GUCs:

- `pacman.node_name`
- `pacman.node_role`
- `pacman.api_address`
- `pacman.control_address`
- `pacman.helper_path`
- `pacman.postgres_data_dir`
- `pacman.postgres_bin_dir`
- `pacman.postgres_listen_address`
- `pacman.postgres_port`
- `pacman.cluster_name`
- `pacman.initial_primary`
- `pacman.seed_addresses`
- `pacman.expected_members`

The Go-side typed bridge lives in [`internal/pgext`](../internal/pgext). It
normalizes the GUC snapshot into the validated PACMAN node config shape that
the shared local-agent runtime already consumes.

At runtime the C worker exports that snapshot as `PACMAN_PGEXT_*` environment
variables and launches:

- `pacmand -pgext-env`

That keeps the C side limited to PostgreSQL registration and helper-process
supervision while the existing Go daemon continues to own heartbeat, local
observation, HTTP API serving, and control-plane publication.

## Lifecycle Wiring

The extension worker now:

- starts a `pacmand -pgext-env` helper process after PostgreSQL reaches
  `RecoveryFinished`
- mirrors PostgreSQL container credentials (`POSTGRES_*` to `PG*`) when needed
  so the helper can probe the local server
- forwards shutdown via `SIGTERM`
- restarts the helper when it exits unexpectedly

That makes the background worker a thin supervisor around the shared PACMAN
local-agent lifecycle.

## Logging and Failure Isolation

The helper process emits the same JSON `slog` stream as normal `pacmand`
process mode, but PostgreSQL extension mode adds explicit runtime metadata:

- `runtime_mode=embedded_worker`
- `failure_isolation=helper_process`
- `error_propagation=structured_stderr_and_exit_status`

Those fields define the operating contract for background-worker mode:

- helper-process failures stay isolated from PostgreSQL backend sessions
- startup/configuration failures surface in PostgreSQL logs through the helper's
  structured stderr stream and non-zero exit status
- the C background worker remains responsible for supervision and restart, while
  the Go helper owns only PACMAN runtime behavior

To troubleshoot reconcile and state-aggregation decisions, set
`PACMAN_LOG_LEVEL=debug` before PostgreSQL starts. The helper process inherits
that setting and emits reconciliation summaries with counts, cluster phase, and
member names only; it does not dump full desired/observed state payloads or
secret-bearing config values.

## Packaging and Images

Top-level build/install targets now cover both the extension assets and the
helper binary:

- `make build-pg-extension`
- `make package-pg-extension`
- `make install-pg-extension`
- `make docker-build-pgext-image`

`package-pg-extension` stages:

- `lib/pacman_agent.so`
- `share/extension/pacman_agent.control`
- `share/extension/pacman_agent--0.1.0.sql`
- `bin/pacmand`

The Docker-backed integration image lives at
[`test/docker/pacman-pgext-postgres.Dockerfile`](../test/docker/pacman-pgext-postgres.Dockerfile).

Full example settings for extension mode live in
[`docs/examples/pacman-agent.postgresql.conf`](./examples/pacman-agent.postgresql.conf).

## Current Scope

Implemented in this pass:

- extension/runtime boundary definition
- shared local-agent bootstrap extraction out of `pacmand`
- extension scaffold with C sources, `.control`, and SQL install script
- background-worker registration through `_PG_init()`
- postmaster GUC bridge definition
- `pacmand -pgext-env` bootstrap mode for the shared local-agent runtime
- helper-process startup, shutdown, and restart supervision
- staged packaging/install flow for library, SQL assets, and helper binary
- extension-specific PostgreSQL Docker image for integration tests
- Docker-backed lifecycle/integration coverage for startup, restart, invalid
  config, local observation, and shutdown

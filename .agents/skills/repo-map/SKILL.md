---
name: repo-map
description: Repository layout for the pacman Go PostgreSQL HA cluster manager.
---

# Repo Map

Use this first when locating code. Prefer opening the narrow directory for the
subsystem instead of loading broad docs.

- `cmd/pacmand`, `cmd/pacmanctl`: binary entrypoints.
- `internal/app/pacmand`, `internal/app/pacmanctl`: CLI/application wiring.
- `internal/controlplane`: HA decisions, leader/source-of-truth, failover,
  switchover, rejoin, epochs, and operation flow.
- `internal/cluster`: domain model: roles, members, status, maintenance,
  operations, specs, and state-machine contracts.
- `internal/agent`: node agent orchestration around PostgreSQL and reporting.
- `internal/postgres`: PostgreSQL probes, commands, WAL/LSN, health, recovery,
  standby, `pg_rewind`, and command wrappers.
- `internal/dcs`: DCS abstraction plus `etcd`, `memory`, and `raft` backends.
- `internal/httpapi`, `internal/api`: PACMAN-native and Patroni-compatible API
  handlers/types/contracts.
- `docs`: architecture, API contract, OpenAPI, examples, demo, migration docs.
- `docs/openapi`: split OpenAPI source files; combined spec is
  `docs/openapi.yaml`.
- `test/integration`, `test/installintegration`, `test/testenv`: integration
  fixtures and end-to-end tests.
- `jepsen`, `tools/jepsenctl`: distributed-systems test matrix, harness,
  checkers, and artifacts.
- `deploy`: lab, Patroni lab, Jepsen, Ansible, and systemd deployment assets.
- `packaging`, `mk`, `tools/rpmctl`, `tools/pkglist`: packaging and Make
  automation.
- `postgresql/pacman_agent`: PostgreSQL extension code.

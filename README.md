# PACMAN

**PACMAN** — **Postgres Autonomous Cluster Manager**

PACMAN is a Go-based high-availability cluster manager for PostgreSQL.

It focuses on a small but important goal: provide safe and understandable PostgreSQL HA with automatic failover, controlled switchover, and explicit rejoin of failed primaries.

---

## Why

PACMAN is built around a few core ideas:

- PostgreSQL HA should be treated as a distributed system problem
- cluster-wide decisions must not be made by a single node in isolation
- topology changes should be explicit state transitions
- the cluster must have one authoritative source of truth
- failover must be quorum-based and fencing-aware

---

## Architecture

PACMAN has two main parts:

- **Node agent** — runs on each PostgreSQL node, observes local state, and executes commands
- **Control plane** — maintains cluster state, elects a leader, and decides failover/switchover

The full architecture diagram now lives in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) so the README stays compact and the diagram can evolve independently.

## API Contract

A draft OpenAPI contract for the control-plane API lives in [docs/openapi.yaml](docs/openapi.yaml).
The maintainable split source for that contract lives in [docs/openapi](docs/openapi), using `oapi-codegen`-compatible external references across the module files.
It now includes both PACMAN-native `/api/v1/*` endpoints and Patroni-compatible top-level routes to support seamless migration.
The review notes, authentication model, and compatibility policy for that contract live in [docs/api-contract.md](docs/api-contract.md).

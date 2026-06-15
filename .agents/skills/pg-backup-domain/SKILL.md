---
name: pg-backup-domain
description: PostgreSQL backup, WAL, recovery, and rejoin context for pacman.
---

# PostgreSQL Backup Domain

Use this when changes touch backup, WAL, recovery, rejoin, or standby behavior.

Key locations:

- `internal/postgres`: WAL/LSN helpers, recovery, standby, `pg_rewind`, health,
  command wrappers, and PostgreSQL probes.
- `internal/controlplane/rejoin*`: rejoin planning, execution, continuation,
  and finalize logic.
- `docs/PATRONI_MIGRATION.md`, `docs/POSTGRES_EXTENSION.md`,
  `docs/examples/*`: compatibility and operational examples.
- `postgresql/pacman_agent`: PostgreSQL extension surface.

Rules:

- Treat timeline and system identifier mismatches as safety-critical.
- Prefer `pg_rewind` only when preconditions are explicit and tested.
- Verify WAL receiver, replication slots, replay LSN, and primary timeline when
  accepting a replica or rejoined node.
- Preserve acknowledged-write safety assumptions in failover/rejoin tests.
- Do not hide async data loss; classify and report it explicitly.
- Keep destructive recovery actions guarded by role and cluster-state checks.

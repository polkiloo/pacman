---
name: cluster-state-machine
description: PACMAN domain rules for cluster state transitions and operation history.
---

# Cluster State Machine

Use for failover, switchover, rejoin, maintenance, roles, epochs, and history.

Primary locations:

- `internal/cluster`: state model, roles, maintenance, operations, specs.
- `internal/controlplane`: transition planning/execution and operation history.
- `docs/ARCHITECTURE.md`, `docs/TODO.md`: architecture and product roadmap.

Rules:

- Make every transition explicit: requested, planned, executed, finalized, or
  failed.
- Preserve monotonic epoch/operation ordering across retries and restarts.
- A member role change must line up with PostgreSQL role, timeline, and health.
- Switchover is operator-directed and should reject unsafe targets.
- Failover is safety-first: prefer no promotion over unsafe promotion.
- Rejoin must prove the old primary is safe, rewound, or reinitialized before it
  can serve again.
- Maintenance mode changes availability expectations but must not weaken
  split-brain prevention.
- Operation history must explain who/what/when/result for HA decisions.

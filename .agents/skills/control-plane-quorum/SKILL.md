---
name: control-plane-quorum
description: Rules for PACMAN control-plane quorum, leases, epochs, fencing, and split-brain prevention.
---

# Control Plane Quorum

Use when touching leader election, source-of-truth reads/writes, failover
eligibility, leases, epochs, fencing, or DCS behavior.

Primary locations:

- `internal/controlplane`: leader, source-of-truth, failover/switchover/rejoin.
- `internal/dcs`: DCS interface and backend implementations.
- `internal/fencing`: fencing hooks and decisions.
- `docs/ARCHITECTURE_DCS.md`: DCS design context.

Rules:

- The DCS/source of truth owns cluster membership, leases, epochs, and operation
  records.
- Never promote from stale membership, stale lease, or missing quorum evidence.
- Epoch and lease updates must be monotonic and checked before acting.
- Fencing-required paths fail closed when fencing is unavailable or fails.
- Prevent split brain before optimizing availability.
- Quorum checks must distinguish DCS availability from PostgreSQL replication
  health.
- Retried decisions must be idempotent and safe after partial execution.

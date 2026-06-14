---
name: pr-review-checklist
description: Lightweight review checklist for Go HA and distributed-systems code.
---

# PR Review Checklist

Lead with correctness risks and file/line findings. Keep summaries brief.

Check:

- State transitions are explicit, validated, and persisted where required.
- Failover/switchover/rejoin paths preserve epoch, role, timeline, and history
  invariants.
- Quorum and lease decisions have a single source of truth and avoid split
  brain.
- Fencing-aware code fails closed when fencing is required but unavailable.
- Concurrency uses contexts, cancellation, deadlines, and bounded retries.
- Operations are idempotent across retries and process restarts.
- PostgreSQL actions verify role, timeline, LSN, replication, and recovery
  state before destructive steps.
- API changes keep handlers, models, OpenAPI docs, compatibility responses, and
  tests aligned.
- Logs/metrics include enough identifiers to debug cluster decisions.
- Tests cover failure paths, stale state, retry behavior, and contract edges.

Do not spend review budget on broad refactors unless they affect safety,
operability, or the requested change.

---
name: node-agent-postgres
description: Rules for PACMAN node agent PostgreSQL observation, lifecycle actions, and state reporting.
---

# Node Agent And PostgreSQL

Use when changing `pacmand` node behavior, PostgreSQL probes, lifecycle actions,
or member reporting.

Primary locations:

- `internal/agent`: node-side orchestration and reporting.
- `internal/postgres`: PostgreSQL commands, probes, WAL/LSN, recovery, standby.
- `internal/peerapi`: member-to-member API behavior.
- `cmd/pacmand`, `internal/app/pacmand`: process wiring.

Rules:

- Observe before acting: role, recovery state, timeline, LSN, system identifier,
  replication health, and process health.
- Lifecycle actions must be idempotent and safe when retried.
- Promote/demote/rejoin must align with control-plane intent and current epoch.
- Do not report a node healthy until PostgreSQL state and PACMAN role agree.
- Rejoin must avoid serving divergent timelines.
- Include command output and clear errors for failed PostgreSQL actions.
- Keep node reports compact but sufficient for control-plane decisions.

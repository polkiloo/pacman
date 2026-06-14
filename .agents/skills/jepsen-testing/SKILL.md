---
name: jepsen-testing
description: PACMAN distributed-systems testing rules for Jepsen-style failures and invariants.
---

# Jepsen Testing

Use for changes in `jepsen/**`, `tools/jepsenctl/**`, HA safety, or failure
injection behavior.

Primary locations:

- `jepsen/README.md`, `jepsen/TODO.md`, `jepsen/UNSUPPORTED.md`.
- `tools/jepsenctl/cmd`: runner, harness, checkers, target/case registry.
- `mk/jepsen.mk`: local and CI entrypoints.

Rules:

- Preserve invariants: single writable primary, acknowledged write
  preservation, timeline convergence, safe old-primary rejoin, correct routing.
- Record machine-checkable history, nemesis schedule, target, start/heal/stop,
  command result, snapshots, and checker output.
- Partitions and crashes must heal or report failed cleanup explicitly.
- Unsupported or Patroni-only profiles are calibration/configuration issues, not
  PACMAN regressions; see `jepsen/UNSUPPORTED.md`.
- Prefer targeted runs: `go test -count=1 ./tools/jepsenctl/cmd`, then the
  smallest `make jepsen-docker-case-*` target needed.
- Keep artifacts useful for failure explanation before adding more matrix cases.

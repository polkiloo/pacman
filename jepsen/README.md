# PACMAN Jepsen Harness

This directory contains the first executable PACMAN Jepsen campaign contract.
The current implementation is a Docker Compose lab smoke harness, intended to
make local and CI Jepsen entry points run real PACMAN lab validation instead of
skipping while the full Clojure workload/nemesis suite is built out.

Run locally through the Dockerized control node:

```bash
make jepsen-docker-smoke
make jepsen-docker-nightly
```

The smoke campaign bootstraps the Docker lab and runs the existing lab
verification stage. The nightly campaign bootstraps the same lab, verifies it,
runs a planned switchover, then verifies it again.

Campaigns reset `deploy/lab/.local/` before bootstrap by default so repeated
runs start from a clean PostgreSQL and DCS state. Set
`PACMAN_JEPSEN_RESET_LAB=false` only when preserving the lab for interactive
debugging.

Artifacts are written under:

```text
jepsen/store/pacman/<campaign>/<timestamp>/
bin/jepsen-ci/<campaign>/summary.md
```

This harness deliberately uses the existing `deploy/lab` topology, which is two
PACMAN data nodes plus external etcd. The broader Jepsen plan in
`docs/JEPSEN.md` still tracks the later full 3-data-node target, Patroni
baseline, Clojure workload generators, and packet/kill nemeses.

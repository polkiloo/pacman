# PACMAN Jepsen Harness

This directory contains the executable PACMAN Jepsen campaign contract for the
Docker Compose lab. The harness runs PACMAN lab bootstrap, PostgreSQL workload
histories, nemesis actions, SQL checkers, and artifact collection from a
Dockerized control node.

Run locally through the Dockerized control node:

```bash
make jepsen-list-cases
make jepsen-docker-smoke
make jepsen-docker-nightly
```

The smoke campaign bootstraps the Docker lab, verifies it, runs
`append-smoke:none`, and verifies the lab again. The nightly campaign defaults to
the broader implemented matrix:

```text
append-smoke:none
append-failover:kill
single-key-register:packet
read-committed-txn:slow-network
serializable-txn:packet,kill
append-failover:repeated-failure
```

Run one case at a time by name:

```bash
make jepsen-docker-case-append-smoke-none
make jepsen-docker-case-append-failover-kill
make jepsen-docker-case-single-key-register-packet
make jepsen-docker-case-read-committed-txn-slow-network
make jepsen-docker-case-serializable-txn-packet-kill
make jepsen-docker-case-append-failover-repeated-failure
```

The same target also accepts the explicit `workload:nemesis` form:

```bash
PACMAN_JEPSEN_CASE='serializable-txn:packet,kill' make jepsen-docker-case
./scripts/local/run-jepsen-docker.sh case serializable-txn:packet,kill
```

Override the case list when running manually:

```bash
PACMAN_JEPSEN_CASES="single-key-register:packet read-committed-txn:slow-network" \
  make jepsen-docker-smoke
```

Implemented workload profiles:

- `append-smoke`
- `append-failover`
- `single-key-register`
- `read-committed-txn`
- `serializable-txn`

Implemented nemesis profiles:

- `none`
- `kill`
- `packet`
- `packet,kill`
- `slow-network`
- `repeated-failure`

Campaigns reset `deploy/lab/.local/` before bootstrap by default so repeated
runs start from a clean PostgreSQL and DCS state. Set
`PACMAN_JEPSEN_RESET_LAB=false` only when preserving the lab for interactive
debugging.

Artifacts are written under:

```text
jepsen/store/pacman/<campaign>/<timestamp>/
bin/jepsen-ci/<campaign>/summary.md
```

Each run writes campaign-level `jepsen-history.edn`, `nemesis-schedule.edn`,
`case-results.jsonl`, per-case `history.edn`, workload `checker.json`,
`primary-observations.jsonl`, `single-primary-checker.json`,
`acknowledged-write-checker.json`, `timeline-checker.json`, nemesis logs,
PACMAN cluster/history snapshots, Docker logs, PostgreSQL logs, and a small
`index.html` for operator review.

This harness deliberately uses the existing `deploy/lab` topology, which is
three PACMAN data nodes plus external etcd. The broader Jepsen plan in
`docs/JEPSEN.md` still tracks the Patroni baseline, optional witness target, and
Clojure/Jepsen checker port.

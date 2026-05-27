# PACMAN Jepsen Harness

This directory contains the executable PACMAN Jepsen campaign contract for the
Docker Compose lab. The harness runs PACMAN lab bootstrap, PostgreSQL workload
histories, nemesis actions, SQL checkers, and artifact collection from a
Dockerized control node.

Run locally through the Dockerized control node:

```bash
make jepsen-list-cases
make jepsen-check-case-targets
make jepsen-docker-smoke
make jepsen-docker-nightly
```

The smoke campaign bootstraps the Docker lab, verifies it, runs
`append-smoke:none`, and verifies the lab again. The default nightly campaign
runs the full implemented matrix. Each case starts from a fresh Docker lab so
destructive profiles cannot poison later cases:

```text
append-smoke:none
append-switchover:switchover
append-failover:kill
append-failover:packet
append-failover:packet,kill
append-failover:primary-dcs-partition
append-failover:primary-replication-partition
append-failover:failover-chain
open-transaction-failover:kill
vip-routing:switchover
append-dcs-quorum:dcs-kill-one
append-dcs-quorum:dcs-lose-majority
append-dcs-quorum:primary-dcs-majority-partition
append-dcs-quorum:dcs-full-restart
append-dcs-quorum:dcs-slow-network
single-key-register:packet
read-committed-txn:slow-network
serializable-txn:packet,kill
append-failover:repeated-failure
```

The `append-switchover:switchover` case issues a manual PACMAN switchover while
append writes are in flight. Nightly records the real checker result for every
case in `case-results.jsonl`; a failed destructive profile remains a failed
nightly case, but the runner continues through the rest of the matrix. After the
matrix finishes, the harness bootstraps a fresh lab and runs one post-campaign
manual switchover using the current cluster membership to select a healthy
non-primary target.

Run one case at a time by name:

```bash
make jepsen-docker-case-append-smoke-none
make jepsen-docker-case-append-switchover-switchover
make jepsen-docker-case-append-failover-kill
make jepsen-docker-case-append-failover-packet
make jepsen-docker-case-append-failover-packet-kill
make jepsen-docker-case-append-failover-primary-dcs-partition
make jepsen-docker-case-append-failover-primary-replication-partition
make jepsen-docker-case-append-failover-failover-chain
make jepsen-docker-case-open-transaction-failover-kill
make jepsen-docker-case-vip-routing-switchover
make jepsen-docker-case-append-dcs-quorum-dcs-kill-one
make jepsen-docker-case-append-dcs-quorum-dcs-lose-majority
make jepsen-docker-case-append-dcs-quorum-primary-dcs-majority-partition
make jepsen-docker-case-append-dcs-quorum-dcs-full-restart
make jepsen-docker-case-append-dcs-quorum-dcs-slow-network
make jepsen-docker-case-single-key-register-packet
make jepsen-docker-case-read-committed-txn-slow-network
make jepsen-docker-case-serializable-txn-packet-kill
make jepsen-docker-case-append-failover-repeated-failure
```

`make jepsen-check-case-targets` verifies every case listed by
`go run ./tools/jepsenctl cases list` has both `jepsen-case-<name>` and
`jepsen-docker-case-<name>` Make targets, so the implemented MVP matrix remains
runnable one case at a time.

The same target also accepts the explicit `workload:nemesis` form:

```bash
PACMAN_JEPSEN_CASE='serializable-txn:packet,kill' make jepsen-docker-case
go run ./tools/jepsenctl run docker case serializable-txn:packet,kill
```

Override the case list when running manually:

```bash
PACMAN_JEPSEN_CASES="single-key-register:packet read-committed-txn:slow-network" \
  make jepsen-docker-smoke
```

Implemented workload profiles:

- `append-smoke`
- `append-switchover`
- `append-failover`
- `append-dcs-quorum`
- `open-transaction-failover`
- `vip-routing`
- `single-key-register`
- `read-committed-txn`
- `serializable-txn`

Implemented nemesis profiles:

- `none`
- `switchover`
- `kill`
- `packet`
- `packet,kill`
- `primary-dcs-partition`
- `primary-replication-partition`
- `dcs-kill-one`
- `dcs-lose-majority`
- `primary-dcs-majority-partition`
- `dcs-full-restart`
- `dcs-slow-network`
- `failover-chain`
- `slow-network`
- `repeated-failure`

Campaigns reset `deploy/lab/.local/` before bootstrap by default so repeated
runs start from a clean PostgreSQL and DCS state. After artifact collection, the
runners destroy the Docker Compose lab and assert no lab containers remain. Set
`PACMAN_JEPSEN_RESET_LAB=false` only when reusing an existing lab, and set
`PACMAN_JEPSEN_DESTROY_LAB=false` only when preserving the lab for interactive
debugging.

Every suite bootstrap records `pacman-cluster-before*.json` and asserts the
clean target shape before workload execution: a healthy PACMAN cluster with
exactly `alpha-1`, `alpha-2`, and `alpha-3`, one current primary, and two
streaming replicas.

Non-`none` nemesis cases wait `PACMAN_JEPSEN_POST_NEMESIS_SETTLE_SECONDS`
seconds after the nemesis heals before final checker sampling. The default is
`10`, which gives promoted timelines and restarted nodes time to settle while
the primary sampler continues recording the transition.

Artifacts are written under:

```text
jepsen/store/pacman/<campaign>/<timestamp>/
bin/jepsen-ci/<campaign>/summary.md
```

Each run writes campaign-level `jepsen-history.edn`, `nemesis-schedule.edn`,
`case-results.jsonl`, per-case `history.edn`, `nemesis-schedule.edn`,
`nemesis-schedule-checker.log`, workload `checker.json`,
`primary-observations.jsonl`, `single-primary-checker.json`,
`acknowledged-write-checker.json`, `timeline-checker.json`,
`old-primary-rejoin-checker.json`, `manual-switchover-checker.json`,
`client-traffic-during-nemesis-checker.json`,
`replication-traffic-during-nemesis-checker.json`,
`dcs-traffic-during-nemesis-checker.json`,
`acknowledged-op-ids.txt`, `final-primary-op-counts.tsv`,
`pacman-cluster-snapshots.jsonl`, `pg-stat-replication.json`,
`pg-stat-wal-receiver.jsonl`, nemesis logs, PACMAN cluster/history snapshots,
Docker logs, PostgreSQL logs, and a small `index.html` for operator review.

Make and CI Jepsen entrypoints now go through `jepsenctl`:
`go run ./tools/jepsenctl run ci ...` for host execution and
`go run ./tools/jepsenctl run docker ...` for Dockerized local execution.

The workload engine still keeps live PostgreSQL collection in shell. Shell owns
Docker Compose service selection, `psql` execution, and lab credentials; Go
checkers consume deterministic artifact files. `final-primary-op-counts.tsv`
is the stable handoff from the final-primary SQL query into the Go
acknowledged-write checker. Revisit this as the remaining Jepsen workload and
nemesis orchestration moves behind `jepsenctl`.

Lab orchestration also remains in shell for now. `deploy/lab` is still the
source of truth for bootstrap, reset, destroy, Docker Compose lifecycle, and
container-local command execution. The remaining shell removal plan is tracked
in `jepsen/TODO.md`.

This harness deliberately uses the existing `deploy/lab` topology, which is
three PACMAN data nodes plus external etcd. The broader Jepsen plan in
`docs/JEPSEN.md` still tracks the Patroni baseline, optional witness target, and
Clojure/Jepsen checker port.

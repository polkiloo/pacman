# PACMAN Jepsen Harness

This directory contains the executable PACMAN Jepsen campaign contract for the
Docker Compose lab. The harness runs PACMAN lab bootstrap, PostgreSQL workload
histories, nemesis actions, SQL checkers, and artifact collection from a
Dockerized control node.

Run locally through the Dockerized control node:

```bash
make jepsen-list-cases
make jepsen-check-case-targets
go run ./tools/jepsenctl targets list
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
- `append-sync` (Patroni calibration only)
- `append-sync-two` (Patroni calibration only)
- `append-strict-sync` (Patroni calibration only)
- `append-max-lag` (Patroni calibration only)
- `append-check-timeline` (Patroni calibration only)
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
- `sync-standby-kill` (Patroni sync calibration only)
- `no-standby` (Patroni strict-sync calibration only)
- `lagging-replica-failover` (Patroni max-lag calibration only)
- `stale-timeline-failover` (Patroni timeline calibration only)

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

DCS quorum cases poll for a fully healthy post-heal sample for up to
`PACMAN_JEPSEN_DCS_RECOVERY_TIMEOUT_SECONDS` seconds at
`PACMAN_JEPSEN_DCS_RECOVERY_INTERVAL_SECONDS` intervals. The defaults are `10`
and `1`, which tolerate transient etcd election responses without masking a
quorum that fails to recover.

Artifacts are written under:

```text
jepsen/store/<target-store>/<campaign>/<timestamp>/
bin/jepsen-ci/<campaign>/summary.md
```

The default target is `pacman-3-data`, which stores artifacts under
`jepsen/store/pacman/...`. The target registry also includes a separate
`patroni-3-data` baseline profile with three Patroni data-node slots and the
same three-node etcd DCS shape. Patroni baseline artifacts use
`jepsen/store/patroni/...` so calibration runs stay separate from PACMAN runs:

```bash
go run ./tools/jepsenctl targets list
PACMAN_JEPSEN_TARGET=patroni-3-data go run ./tools/jepsenctl run docker smoke
```

`patroni-3-data` uses the dedicated `deploy/patroni-lab` Compose stack, three
Patroni-managed PostgreSQL nodes, and a three-node etcd quorum. Its enabled
baseline cases are `append-smoke:none`, `append-failover:kill`, and
`single-key-register:packet`. The opt-in Patroni configuration calibration
cases are `append-sync:kill`, `append-sync:sync-standby-kill`,
`append-sync-two:none`, `append-strict-sync:no-standby`,
`append-max-lag:lagging-replica-failover`, and
`append-check-timeline:stale-timeline-failover`:

```bash
PACMAN_JEPSEN_TARGET=patroni-3-data \
  go run ./tools/jepsenctl run docker case append-sync-kill
PACMAN_JEPSEN_TARGET=patroni-3-data \
  go run ./tools/jepsenctl run docker case append-sync-sync-standby-kill
PACMAN_JEPSEN_TARGET=patroni-3-data \
  go run ./tools/jepsenctl run docker case append-sync-two-none
PACMAN_JEPSEN_TARGET=patroni-3-data \
  go run ./tools/jepsenctl run docker case append-strict-sync-no-standby
PACMAN_JEPSEN_TARGET=patroni-3-data \
  go run ./tools/jepsenctl run docker case append-max-lag-lagging-replica-failover
PACMAN_JEPSEN_TARGET=patroni-3-data \
  go run ./tools/jepsenctl run docker case append-check-timeline-stale-timeline-failover
```

The synchronous cases configure Patroni through its dynamic DCS configuration
API before the workload starts. The standby-kill case stops an active
synchronous standby and requires Patroni to retain an available synchronous
standby. The two-standby case configures `synchronous_node_count=2` and requires
both standbys to be selected. The strict-sync case stops both standbys, requires
a bounded write probe to become unavailable, restarts both standbys, and
requires writes to recover. Other Patroni workload/nemesis profiles remain
disabled until their target-specific fault controls are implemented. The
max-lag case configures `maximum_lag_on_failover`, pauses WAL replay on one
replica until it exceeds the threshold, stops the primary, and requires Patroni
to promote the other replica while recording replay recovery.
The timeline case configures `check_timeline=true`, keeps one replica on the old
timeline while promoting the other, stops the promoted node, and requires the
stale replica to remain read-only until the correct-timeline node returns.

Compare an explicit PACMAN run with an explicit Patroni calibration run using
their `case-results.jsonl` files:

```bash
go run ./tools/jepsenctl artifacts compare-baseline \
  --pacman-results jepsen/store/pacman/<campaign>/<run>/case-results.jsonl \
  --patroni-results jepsen/store/patroni/<campaign>/<run>/case-results.jsonl
```

The comparison joins results by the exact `workload:nemesis` profile. PACMAN
profiles without a matching Patroni baseline are reported as
`no-matching-profile` and are not compared with a different fault profile.

Each run writes campaign-level `jepsen-history.edn`, `nemesis-schedule.edn`,
`case-results.jsonl`, per-case `history.edn`, `nemesis-schedule.edn`,
`nemesis-schedule-checker.log`, workload `checker.json`,
`primary-observations.jsonl`, `single-primary-checker.json`,
`acknowledged-write-checker.json`, `timeline-checker.json`,
`old-primary-rejoin-checker.json`, `manual-switchover-checker.json`,
`client-traffic-during-nemesis-checker.json`,
`replication-traffic-during-nemesis-checker.json`,
`dcs-traffic-during-nemesis-checker.json`,
`synchronous-replication-config.json`,
`synchronous-replication-checker.json`,
`synchronous-standby-kill-checker.json`, `synchronous-standby-kill-probes.jsonl`,
`strict-sync-no-standby-checker.json`, `strict-sync-write-probes.jsonl`,
`maximum-lag-on-failover-config.json`, `maximum-lag-on-failover-checker.json`,
`maximum-lag-on-failover-probes.jsonl`,
`patroni-check-timeline-config.json`, `patroni-check-timeline-checker.json`,
`patroni-check-timeline-probes.jsonl`,
`acknowledged-op-ids.txt`, `final-primary-op-counts.tsv`,
`pacman-cluster-snapshots.jsonl`, `pg-stat-replication.json`,
`pg-stat-wal-receiver.jsonl`, nemesis logs, PACMAN cluster/history snapshots,
Docker logs, PostgreSQL logs, and a small `index.html` for operator review.

Make and CI Jepsen entrypoints now go through `jepsenctl`:
`go run ./tools/jepsenctl run ci ...` for host execution and
`go run ./tools/jepsenctl run docker ...` for Dockerized local execution.

The Jepsen workload engine, nemesis scheduling, artifact collection, lab
bootstrap/destroy calls, Docker Compose execution, and checker handoff logic now
live in `jepsenctl`. The `final-primary-op-counts.tsv` file remains as an
explicit acknowledged-write checker artifact, not as a shell handoff.

`deploy/lab` remains the product-supported local lab interface for bootstrap,
reset, destroy, and demo workflows. Jepsen calls that interface from Go instead
of carrying a separate `jepsen/lib` shell library.

This harness deliberately uses the existing `deploy/lab` topology, which is
three PACMAN data nodes plus external etcd. The broader Jepsen plan in
`docs/JEPSEN.md` still tracks the Patroni baseline, optional witness target, and
broader Jepsen campaign plan.

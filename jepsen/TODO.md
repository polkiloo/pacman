# PACMAN Jepsen TODO

This file is a PACMAN Jepsen roadmap, not a requirement to implement every
possible PostgreSQL HA chaos test.

The goal is to prove PACMAN's core HA safety claims first:

- [x] PACMAN never exposes two writable PostgreSQL primaries at the same time.
- [x] Acknowledged writes are preserved, or any async-mode loss is explicitly
      measured and reported as allowed data loss.
- [x] Failover promotes only eligible replicas.
- [x] A former primary rejoins safely and does not silently continue on a
      divergent timeline.
- [x] Surviving nodes converge to one PostgreSQL timeline after failover.
- [x] Client write routing follows the current PACMAN primary.

Patroni is useful as a calibration baseline, but PACMAN is the product under
test. Patroni-specific features belong in the baseline track unless PACMAN has
equivalent semantics.

## Current Baseline

- [x] Dockerized Jepsen control runner.
- [x] Docker lab target with three PACMAN data nodes.
- [x] Clean lab reset/bootstrap before campaigns.
- [x] Per-case Jepsen artifact directories.
- [x] Single-case runner and named Make targets.
- [x] Smoke append workload with `none` nemesis.
- [x] Basic append/register/transaction workload scaffolding.
- [x] Basic `none`, `kill`, `packet`, `packet,kill`, `slow-network`, and
      `repeated-failure` nemesis scaffolding.

## MVP-1: Required Safety Core

Implement these before expanding the campaign matrix.

### Checkers

- [x] Single writable primary checker.
  - [x] Sample every data node with `pg_is_in_recovery()`.
  - [x] Record node identity, writable state, timeline, and observation time.
  - [x] Fail if two nodes are writable during the same observation window.

- [x] Acknowledged write preservation checker.
  - [x] Insert globally unique operation IDs.
  - [x] Track only writes that returned a successful commit.
  - [x] Verify acknowledged operation IDs exist exactly once on the final
        primary.
  - [x] Classify missing acknowledged writes as failure unless the run is an
        explicitly async-loss measurement profile.

- [x] Timeline checker.
  - [x] Record timeline before failover.
  - [x] Record timeline after failover.
  - [x] Verify promoted primary advances timeline.
  - [x] Verify replicas converge to the promoted timeline.
  - [x] Verify old primary requires safe rejoin or reinitialization if divergent.

### Cases

- [x] `append-smoke:none`
- [x] `append-failover:kill`
- [x] `append-failover:packet`
- [x] `append-failover:packet,kill`
- [x] old primary rejoin after failover

### Observability

Collect these for every MVP-1 run:

- [x] Jepsen operation history.
- [x] Every MVP case writes machine-checkable history.
- [x] Failure injection history.
- [x] PACMAN cluster snapshot after run.
- [x] PACMAN history snapshot after run.
- [x] PostgreSQL logs.
- [x] PACMAN logs.
- [x] DCS logs.
- [x] PACMAN cluster snapshot before, during, and after each nemesis window.
- [x] PostgreSQL role/timeline/LSN snapshot from every data node.
- [x] `pg_stat_replication` from the current primary.
- [x] `pg_stat_wal_receiver` from replicas.

## MVP-2: Strong HA Behavior

Implement after MVP-1 is stable and repeatable.

- [x] Manual switchover under append workload.
- [x] Primary-alone network partition.
- [x] Isolate primary from DCS while client traffic remains available.
- [x] DCS traffic blocked while replication stays healthy.
- [x] Replication traffic blocked while DCS stays healthy.
- [x] Repeated failover chain across all three data nodes.
- [x] Open transaction during failover.
- [x] Router sends writes only to current PACMAN primary, if a router is part of
      the supported deployment.
  - [x] Covered through the supported `vip-manager` PostgreSQL VIP route.

## MVP-3: DCS Quorum Campaigns

These campaigns now run against the Docker lab's real three-node external etcd
quorum.

- [x] Three-node etcd target for PACMAN Jepsen.
- [x] Kill one DCS node.
- [x] Lose DCS majority.
- [x] Isolate primary from DCS majority.
- [x] Full DCS restart.
- [x] Slow DCS / DCS latency campaign.

## Post-MVP Hardening

These are valuable, but not required for the first PACMAN Jepsen safety suite.

- [ ] Slow network / repeated-failure soak profiles.
- [ ] Multi-run 30-minute soak profile.
- [ ] Archived seeds, schedules, histories, and summaries for failed soak runs.
- [ ] Disk full on primary.
- [ ] Disk full on replica.
- [ ] Slow disk / fsync stall.
- [ ] Clock skew.
- [ ] Flapping network.
- [ ] Watchdog required but unavailable.
- [ ] PACMAN crash on primary while PostgreSQL remains alive.
- [ ] Slow PostgreSQL shutdown after leader loss.
- [ ] VM/container pause longer than DCS TTL.

## Conditional Tracks

Only implement these when PACMAN exposes or claims the corresponding behavior.

### Patroni Baseline

Use Patroni to calibrate the Jepsen harness and checkers, not as the main product
target.

- [x] Patroni three-data-node baseline target.
- [x] Patroni `append-smoke:none`.
- [x] Patroni `append-failover:kill`.
- [x] Patroni `single-key-register:packet`.
- [x] Store Patroni artifacts under `jepsen/store/patroni/...`.
- [x] Compare PACMAN results only against matching workload/nemesis profiles.

### Synchronous Replication Semantics

Skip until PACMAN has explicit supported semantics for sync/strict-sync behavior.

- [x] Sync-mode acknowledged write preservation.
- [x] Strict-sync no-standby unavailability behavior.
- [x] Synchronous standby kill.
- [x] `synchronous_node_count > 1` behavior.

### Patroni-Specific Configuration

These stay in the Patroni baseline track unless PACMAN adds equivalent config
surface.

- [x] `synchronous_mode=true`.
- [x] `synchronous_mode_strict=true`.
- [x] `maximum_lag_on_failover`.
- [x] `check_timeline=true`.
- [x] `patronictl pause` / `resume`.
- [x] Patroni dynamic config changes through DCS.

## Go Automation Migration Plan

Move brittle validation, checker logic, and runner entrypoints to Go
incrementally. The Make/CI-facing runners now live in `jepsenctl`; the remaining
shell is lower-level workload, nemesis, artifact collection, and lab
orchestration code that needs a separate port.

1. [x] Add a small Go CLI, `tools/jepsenctl`, with subcommands and table-driven
       tests.
   - [x] Keep it repo-local and runnable with `go run ./tools/jepsenctl ...`.
   - [x] Use only standard library packages at first unless a dependency removes
         real complexity.

2. [x] Move case registry validation to Go.
   - [x] Move the case registry into Go and expose it through
         `go run ./tools/jepsenctl cases list`.
   - [x] Verify every case has `jepsen-case-<slug>` and
         `jepsen-docker-case-<slug>` Make targets.
   - [x] Replace `jepsen/bin/check-case-targets` with
         `go run ./tools/jepsenctl cases validate`.

3. [x] Move cluster-shape validation to Go.
   - [x] Read `pacman-cluster-before*.json`.
   - [x] Assert exactly three data nodes: `alpha-1`, `alpha-2`, and `alpha-3`.
   - [x] Assert one healthy primary and two healthy replicas.
   - [x] Move cluster JSON collection into the Go harness.

4. [x] Move artifact summary and index generation to Go.
   - [x] Generate the artifact index currently assembled in
         `scripts/ci/run-jepsen.sh`.
   - [x] Produce a concise failure summary from `case-results.jsonl`,
         `nightly-failures.txt`, and checker JSON files.
   - [x] Keep GitHub Actions upload wiring in shell/YAML.

5. [x] Move JSON/JSONL checkers to Go one checker at a time.
   - [x] Start with DCS quorum checker.
   - [x] Move single-primary checker.
   - [x] Move acknowledged-write checker.
   - [x] Move timeline checker.
   - [x] Move old-primary rejoin checker.
   - [x] Move manual-switchover checker.
   - [x] Move VIP-routing checker.
   - [x] Keep golden fixtures and failure-case tests for currently moved
         checkers: DCS quorum, single-primary, acknowledged-write, and
         timeline.
   - [x] Keep golden fixtures and failure-case tests for old-primary rejoin.
   - [x] Keep golden fixtures and failure-case tests for manual-switchover.
   - [x] Keep golden fixtures and failure-case tests for VIP-routing.
   - [x] After checker migration is stable, decide whether to move live SQL
         collection into Go and remove intermediate TSV handoff files such as
         `final-primary-op-counts.tsv`.
         Decision: move live SQL collection into `jepsenctl`. Keep
         `final-primary-op-counts.tsv` as an explicit acknowledged-write checker
         artifact until the checker no longer needs a persisted final-primary
         row-count view.

6. [x] Move nemesis schedule validation to Go.
   - [x] Verify every nemesis records start, heal/stop, target, and command
         result.
   - [x] Validate schedule entries against the selected `workload:nemesis`
         profile.

7. [x] Revisit lab orchestration after checker migration is stable.
   - [x] Decide whether `bootstrap/reset/destroy`, `docker compose exec`, and
         nemesis execution should stay shell or move behind Go subcommands.
         Decision: move Jepsen-owned orchestration into `jepsenctl` while
         keeping `deploy/lab` as the product-supported local lab interface.
         Jepsen still invokes deploy/lab bootstrap/reset/destroy entrypoints,
         but Docker Compose execution, workload flow, nemesis flow, artifact
         collection, and checker handoffs are Go-owned.
   - [x] Do not rewrite long-running Docker orchestration until the Go
         validators have reduced real maintenance pain.

8. [x] Port remaining Jepsen shell workload orchestration to Go.
   - [x] Replace Make/CI entrypoints `scripts/ci/run-jepsen.sh` and
         `scripts/local/run-jepsen-docker.sh` with
         `go run ./tools/jepsenctl run ci|docker ...`.
   - [x] Move case listing and Make target validation behind
         `go run ./tools/jepsenctl cases list|validate`.
   - [x] Port `jepsen/bin/ci-smoke`, `jepsen/bin/ci-nightly`, and
         `jepsen/bin/ci-case` campaign orchestration to Go.
   - [x] Port `jepsen/lib/docker-lab.sh` artifact collection, cluster
         validation calls, lab bootstrap/destroy hooks, and EDN event writing
         to Go.
   - [x] Port `jepsen/lib/workloads/*.sh` SQL workload generation, nemesis
         execution, sampling, topology helpers, and remaining checker handoff
         logic to Go.
   - [x] After the Go workload engine is complete, delete the replaced Jepsen
         shell files and keep deploy/lab shell scripts only if they remain the
         product-supported local lab interface.

## Definition of Done

MVP-1 is done when:

- [x] The suite deploys and destroys a clean PACMAN three-data-node cluster.
- [x] Every MVP case is runnable individually.
- [x] Every MVP case writes machine-checkable history.
- [x] Every nemesis action records target, start, heal, and command result.
- [x] Every failed run produces enough logs and snapshots to explain the failure.
- [x] Checkers report:
  - [x] split-brain result;
  - [x] acknowledged write preservation result;
  - [x] timeline convergence result;
  - [x] failover/rejoin summary.
- [x] `append-smoke:none` is stable across repeated local runs.
- [x] `append-failover:kill` is stable enough to run as a manual CI smoke.
- [x] Unsupported configurations are documented separately from
      product regressions.

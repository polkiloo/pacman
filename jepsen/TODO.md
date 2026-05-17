# PACMAN Jepsen TODO

This file is a PACMAN Jepsen roadmap, not a requirement to implement every
possible PostgreSQL HA chaos test.

The goal is to prove PACMAN's core HA safety claims first:

- [x] PACMAN never exposes two writable PostgreSQL primaries at the same time.
- [x] Acknowledged writes are preserved, or any async-mode loss is explicitly
      measured and reported as allowed data loss.
- [ ] Failover promotes only eligible replicas.
- [ ] A former primary rejoins safely and does not silently continue on a
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

Do not implement these until the Jepsen lab has a real three-node DCS quorum.
The current Docker smoke target uses a single external etcd service, so DCS
majority tests would be artificial today.

- [ ] Three-node etcd target for PACMAN Jepsen.
- [ ] Kill one DCS node.
- [ ] Lose DCS majority.
- [ ] Isolate primary from DCS majority.
- [ ] Full DCS restart.
- [ ] Slow DCS / DCS latency campaign.

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

## Conditional Tracks

Only implement these when PACMAN exposes or claims the corresponding behavior.

### Patroni Baseline

Use Patroni to calibrate the Jepsen harness and checkers, not as the main product
target.

- [ ] Patroni three-data-node baseline target.
- [ ] Patroni `append-smoke:none`.
- [ ] Patroni `append-failover:kill`.
- [ ] Patroni `single-key-register:packet`.
- [ ] Store Patroni artifacts under `jepsen/store/patroni/...`.
- [ ] Compare PACMAN results only against matching workload/nemesis profiles.

### Synchronous Replication Semantics

Skip until PACMAN has explicit supported semantics for sync/strict-sync behavior.

- [ ] Sync-mode acknowledged write preservation.
- [ ] Strict-sync no-standby unavailability behavior.
- [ ] Synchronous standby kill.
- [ ] `synchronous_node_count > 1` behavior.

### Fencing and Watchdog

Post-MVP unless PACMAN ships a watchdog/fencing contract.

- [ ] Watchdog required but unavailable.
- [ ] PACMAN crash on primary while PostgreSQL remains alive.
- [ ] Slow PostgreSQL shutdown after leader loss.
- [ ] VM/container pause longer than DCS TTL.

### Patroni-Specific Configuration

These stay in the Patroni baseline track unless PACMAN adds equivalent config
surface.

- [ ] `synchronous_mode=true`.
- [ ] `synchronous_mode_strict=true`.
- [ ] `maximum_lag_on_failover`.
- [ ] `check_timeline=true`.
- [ ] `patronictl pause` / `resume`.
- [ ] Patroni dynamic config changes through DCS.

## Definition of Done

MVP-1 is done when:

- [ ] The suite deploys and destroys a clean PACMAN three-data-node cluster.
- [ ] Every MVP case is runnable individually.
- [ ] Every MVP case writes machine-checkable history.
- [ ] Every nemesis action records target, start, heal, and command result.
- [ ] Every failed run produces enough logs and snapshots to explain the failure.
- [ ] Checkers report:
  - [x] split-brain result;
  - [x] acknowledged write preservation result;
  - [x] timeline convergence result;
  - [ ] failover/rejoin summary.
- [ ] `append-smoke:none` is stable across repeated local runs.
- [x] `append-failover:kill` is stable enough to run as a manual CI smoke.
- [ ] Unsupported configurations are documented separately from
      product regressions.

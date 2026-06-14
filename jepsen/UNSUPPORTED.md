# Unsupported Jepsen Configurations

This document separates unsupported Jepsen configurations from PACMAN product
regressions. Use it when triaging failed local runs, CI artifacts, or Patroni
baseline comparisons.

## Product Regression Scope

A failure is a PACMAN product regression when all of these are true:

- The target is `pacman-3-data`.
- The workload and nemesis are listed by `go run ./tools/jepsenctl cases list`
  and are not marked Patroni-only in the case registry.
- The run uses the supported PACMAN Docker lab shape: three PostgreSQL data
  nodes, three external etcd DCS nodes, and the supported `deploy/lab` bootstrap.
- A checker reports a safety, availability, convergence, routing, or artifact
  failure for behavior PACMAN claims to support.

Examples in scope include `append-smoke:none`, the PACMAN failover profiles,
DCS quorum profiles, transaction/register profiles, VIP routing, and the
supported repeated-failure profile.

## Unsupported Or Calibration Scope

The following are not PACMAN product regressions by themselves:

- Workloads marked Patroni-only in the case registry:
  `append-sync`, `append-sync-two`, `append-strict-sync`, `append-max-lag`, and
  `append-check-timeline`.
- Patroni-specific controls and semantics without an equivalent PACMAN support
  contract: `synchronous_mode`, `synchronous_mode_strict`,
  `synchronous_node_count`, `maximum_lag_on_failover`, `check_timeline`,
  `patronictl pause` / `resume`, and Patroni dynamic configuration through DCS.
- Patroni baseline failures under `PACMAN_JEPSEN_TARGET=patroni-3-data`. These
  are calibration or harness issues unless the same exact `workload:nemesis`
  profile also fails against `pacman-3-data`.
- Unsupported target, workload, or nemesis selections rejected by
  `jepsenctl`. These are operator or harness configuration errors.
- Post-MVP exploratory profiles whose product contract is not yet documented,
  such as watchdog/fencing behavior, disk-full profiles, clock skew, and
  long-duration soak profiles.

`append-failover:primary-dcs-partition` is an opt-in exploratory case. It can
expose whether PACMAN fences an old primary isolated from DCS, but a failure is
only a product regression after PACMAN documents that fencing behavior as a
supported contract.

## Artifact And Issue Triage

Keep unsupported configuration evidence out of product regression issues unless
it reproduces on a supported PACMAN profile.

- File PACMAN regressions from `jepsen/store/pacman/...` artifacts.
- File Patroni calibration issues from `jepsen/store/patroni/...` artifacts.
- Compare only exact `workload:nemesis` profiles with
  `go run ./tools/jepsenctl artifacts compare-baseline`.
- For unsupported configurations, record the rejected or unsupported profile,
  target, command, and artifact path, but classify the issue as configuration
  coverage or harness calibration instead of a product regression.

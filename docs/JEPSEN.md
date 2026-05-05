# PACMAN Jepsen Evaluation

This note records the MVP decision for the Jepsen fault-injection track.

## Source Shape

The reference project, [Wolfsrudel/database-postgres-ha-patroni-testing-jepsen](https://github.com/Wolfsrudel/database-postgres-ha-patroni-testing-jepsen), is a Clojure/Leiningen Jepsen harness for PostgreSQL HA. Its README describes a Vagrant-created VM cluster, a k3s Kubernetes layer, and selectable HA targets under `cluster/`, including `single-node` and `patroni`. The test commands run Jepsen workloads such as append/register-style histories with isolation levels, concurrency, nemesis selection, and repeated long-running campaigns.

Jepsen itself runs from a control node, talks to database nodes over SSH, records operation histories, injects faults through a nemesis process, and writes analysis artifacts under `store/` for review.

## Decision

Build a PACMAN-specific Jepsen harness while reusing the reference workload and nemesis model.

Do not directly adapt the reference repository layout as the primary PACMAN harness. Its environment is centered on Vagrant, k3s, and Patroni deployment semantics. PACMAN already has process-mode, testcontainer, and Ansible/lab paths, plus HA semantics that differ from Patroni around witness quorum, fencing, explicit rejoin, and operation history. A direct port would make PACMAN Jepsen coverage depend on a Kubernetes substrate before the core HA product requires it, and would blur whether failures come from PACMAN, the Patroni-oriented deployment shape, or the lab substrate.

## Harness Direction

The PACMAN harness should still use Jepsen/Clojure and keep the proven shape of the reference tests:

- Jepsen control node with SSH access to data nodes.
- PostgreSQL clients that exercise reads, writes, transactions, and operation histories through the current primary endpoint.
- Workloads covering append/register-style histories, single-key stress, read committed, and serializable checks.
- Nemeses covering no fault, network partition, process kill, combined partition plus kill, slow network, and repeated-failure campaigns.
- Result archival with Jepsen `store/` output, histories, plots, and concise PACMAN-specific failure summaries.

The target deployment should be PACMAN-native:

- Start with a 3-data-node PACMAN cluster using the same operational shape as the Ansible/lab install path.
- Add optional witness coverage as a PACMAN-specific extension after the base 3-node campaign is stable.
- Drive failover observations through PACMAN APIs and PostgreSQL behavior, not Patroni labels or Patroni-specific Kubernetes metadata.
- Keep a Patroni baseline target as a separate calibration target, not as the foundation of PACMAN test execution.

## Patroni Baseline Target

The Jepsen harness must include a `patroni` baseline target before PACMAN-only
assertions are treated as trusted signal. This target exists to calibrate the lab,
workload generators, nemeses, client routing, and history checkers against a known
PostgreSQL HA implementation.

The baseline should be implemented as a separate target from `pacman`, not as a
shared deployment layer:

| Target | Purpose | Deployment shape | Primary discovery |
|---|---|---|---|
| `patroni` | Calibration baseline | 3 PostgreSQL data nodes managed by Patroni with the same DCS family used by the selected lab profile | Patroni REST API, PostgreSQL role checks, or the same service endpoint clients will use |
| `pacman` | Product under test | 3 PostgreSQL data nodes managed by PACMAN, with optional witness added after the base profile stabilizes | PACMAN native API plus PostgreSQL role checks |

The Patroni target should run the same workload and nemesis matrix planned for
PACMAN:

- smoke: `none` nemesis, short append/register run, verifies client and checker wiring;
- failover: process kill against the current primary;
- partition: packet loss / network split that isolates the primary;
- combined: packet plus process kill;
- soak: repeated slow-network plus kill campaign using archived seeds and histories.

PACMAN Jepsen assertions are trusted only after the baseline target proves that:

- the lab can complete a no-fault run without false positives;
- nemesis events are visible in logs and actually affect the intended node links or processes;
- clients consistently route writes to the observed primary endpoint;
- checker output and `store/` artifacts are archived and reviewable;
- known Patroni behavior under aggressive campaigns is recorded as baseline context rather than treated as PACMAN regression evidence.

Baseline results should be kept under a distinct Jepsen store path such as
`store/patroni/...`, while PACMAN runs use `store/pacman/...`. CI summaries should
report both target name and workload/nemesis profile so PACMAN failures are compared
against the matching Patroni calibration run.

## PACMAN Target Topology

The first PACMAN Jepsen target is `pacman-3-data`. It should model the production
Ansible/lab deployment rather than the Go integration-test fixture.

| Node | PACMAN role | PostgreSQL role at bootstrap | Services | Notes |
|---|---|---|---|---|
| `n1` | `data` | initial primary | `postgres`, `pacmand` | Preferred bootstrap primary and first client write target. |
| `n2` | `data` | replica | `postgres`, `pacmand` | Eligible failover candidate. |
| `n3` | `data` | replica | `postgres`, `pacmand` | Eligible failover candidate. |
| `dcs1`..`dcs3` | external DCS | none | `etcd` or selected DCS profile | Start as a 3-node DCS quorum so DCS loss can be modeled independently from data-node loss. |

Required topology properties:

- All data nodes run identical PACMAN builds and PostgreSQL major versions.
- `bootstrap.expectedMembers` includes only the three data members for the base target.
- `bootstrap.initialPrimary` is `n1`.
- Every data node has stable addresses for PostgreSQL, PACMAN API, and PACMAN control traffic.
- Client traffic is routed through Jepsen target code that discovers the current primary before write phases. The first implementation may poll `GET /api/v1/cluster` and verify with PostgreSQL `pg_is_in_recovery()`.
- The target exposes lifecycle hooks for install, configure, start, stop, restart, current-primary lookup, member status lookup, log collection, and destructive cleanup.
- Jepsen nemeses must be able to affect PACMAN/PostgreSQL processes independently from DCS processes.

The target should provide these node sets to workload and nemesis code:

- `:data-nodes` for PostgreSQL and `pacmand` process faults;
- `:dcs-nodes` for DCS faults;
- `:client-endpoints` for PostgreSQL connections;
- `:api-endpoints` for PACMAN observation and operation history capture.

Do not add witness behavior to the first passing smoke target. Add witness coverage as
`pacman-3-data-1-witness` after `pacman-3-data` passes the no-fault and single-primary
kill profiles.

| Node | PACMAN role | PostgreSQL role | Services | Notes |
|---|---|---|---|---|
| `w1` | `witness` | none | `pacmand` | Quorum voter only; no PostgreSQL client traffic and never eligible for promotion. |

Witness-specific expectations:

- The witness appears in PACMAN cluster status as role `witness`, healthy, and not failover-eligible.
- Loss of one data node with witness quorum available can permit safe failover according to PACMAN policy.
- Loss or partition of the witness must be visible in cluster status and must not create PostgreSQL write targets.
- Jepsen clients never connect to the witness as a database endpoint.
- Witness assertions are PACMAN-specific and are not compared directly with the Patroni baseline.

## Consequences

This choice keeps the valuable Jepsen parts: workload generators, nemesis schedule, history checking, and repeat-run campaigns. It avoids coupling PACMAN's first Jepsen campaign to k3s or Patroni-specific assumptions. The tradeoff is that PACMAN needs its own small Clojure target layer for install/start/stop, primary discovery, client connection routing, and artifact collection.

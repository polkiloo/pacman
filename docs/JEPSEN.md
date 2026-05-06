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

## Workload Coverage

Workloads should be shared between the `patroni` baseline and `pacman` targets.
Target-specific code may differ for primary discovery and observation, but the SQL
operations, history shape, checker, concurrency, nemesis schedule, and run duration
must stay comparable.

| Profile | Purpose | Operation model | PostgreSQL isolation | Checker |
|---|---|---|---|---|
| `append-smoke` | First end-to-end Jepsen validation and no-fault calibration | Many clients append unique values to a small set of list keys, then read full lists | `read committed` | Elle list-append / set-style history check plus no acknowledged write loss |
| `append-failover` | Core HA safety check under primary changes | Same as `append-smoke`, with longer duration and failover nemeses | `read committed` | Same as smoke, plus exactly one writable primary observed at any successful write point |
| `single-key-register` | Stress linearization around one hot key | Clients repeatedly write monotonic values to one row and read it back | `read committed` for baseline; optional `serializable` variant | Register checker with stale-read and lost-acknowledged-write detection |
| `read-committed-txn` | Verify ordinary transactional behavior during HA events | Multi-row transactions insert an operation id, update a counter, and read back committed state | `read committed` | Transaction history checker that rejects fractured reads, missing committed operation ids, and duplicate operation ids |
| `serializable-txn` | Strongest PostgreSQL transaction profile PACMAN should preserve across routing/failover | Contending transactions update the same logical account/register set and retry serialization failures | `serializable` | Elle/serializable checker; serialization failures are allowed only when reported as aborted/failed operations |

Common workload rules:

- Every successful write records a globally unique operation id, client id, logical key, value, target node, observed primary, transaction isolation, and PACMAN cluster epoch when available.
- Clients must reconnect and rediscover the current primary after connection loss, failover, or SQL read-only errors.
- Read-only replica responses are failed operations, not successful reads, unless a future workload explicitly targets replica reads.
- Unknown commit outcomes are recorded as indeterminate Jepsen operations, then reconciled by final reads where the checker supports it.
- Finalization must read all workload tables from the final primary and include PACMAN `/api/v1/history` and `/api/v1/cluster` snapshots in the Jepsen artifact bundle.

Implementation order:

1. `append-smoke` with `none` nemesis on Patroni and PACMAN.
2. `append-failover` with primary process kill.
3. `single-key-register` with network partition and process kill profiles.
4. `read-committed-txn` after basic primary rediscovery is stable.
5. `serializable-txn` after retry/error classification is explicit enough to avoid false positives.

## Nemesis Coverage

Nemeses should be shared between Patroni and PACMAN wherever the target shape permits
it. PACMAN-specific variants may add witness and explicit rejoin assertions, but the
fault schedule should remain comparable to the Patroni baseline.

| Profile | Fault model | Target set | First workload pairing | Required observations |
|---|---|---|---|---|
| `none` | No injected fault | none | `append-smoke` | Establishes no-fault baseline, confirms clients can discover the primary, and verifies artifact capture. |
| `kill` | Stop or `SIGTERM` the current primary's PostgreSQL and/or `pacmand` process, then allow restart | one node from `:data-nodes`, normally current primary | `append-failover` | Failure is logged, primary changes or safely remains stable, old primary is not writable after demotion/failure, PACMAN history records the transition when one occurs. |
| `packet` | Drop or reject traffic between one data node and the rest of the cluster | one data node, then current primary once smoke is stable | `single-key-register` | Partition is visible from both sides, isolated primary is not accepted as a safe write target by clients, reachable quorum either promotes safely or blocks writes according to policy. |
| `packet,kill` | Partition a node and kill one HA process while the partition is active | primary/data-node combinations only after individual profiles pass | `append-failover` and `single-key-register` | Combined fault does not produce two acknowledged writable primaries, and any former primary is marked for rejoin before reuse. |
| `slow-network` | Add latency, jitter, packet loss, or bandwidth limits without fully partitioning nodes | data-node links first, DCS links later | `read-committed-txn` | Detects timing-sensitive election/failover behavior without treating transient SQL retry errors as successful operations. |
| `repeated-failure` | Randomized sequence of kill, heal, slow-network, and partition operations over a longer run | data nodes, then DCS nodes after data-node profiles are stable | `serializable-txn` and soak profiles | Used for nondeterministic regressions; every run must archive seeds, schedule, histories, and target logs. |

Fault boundaries:

- Start with data-node faults. Add DCS-node faults only after data-node `none`, `kill`, and `packet` profiles have stable baseline results.
- Do not kill all DCS quorum members in the default profile; that belongs in an explicit DCS-loss campaign.
- Do not target witness nodes until `pacman-3-data-1-witness` exists and the base data-node campaigns pass.
- Do not mix Patroni and PACMAN semantics in the same checker. The nemesis can be shared, but witness and rejoin expectations are PACMAN-only.

Observability requirements:

- Every nemesis operation records target node, affected service or link, start time, heal time, and command/result details.
- The harness captures PACMAN `/api/v1/cluster`, `/api/v1/history`, PostgreSQL role state, and relevant process logs before fault, during fault, and after heal.
- Network nemeses must verify the link is actually impaired, for example by failed TCP probes or measured latency/packet loss from the affected peers.
- Kill nemeses must verify the intended process stopped and whether the deployment supervisor restarted it.
- Heal operations must be idempotent and verified before the checker finalizes the workload.

Nemesis rollout order:

1. `none` with `append-smoke`.
2. `kill` against current primary with `append-failover`.
3. `packet` isolating current primary with `single-key-register`.
4. `packet,kill` after individual `packet` and `kill` profiles are stable.
5. `slow-network` with `read-committed-txn`.
6. `repeated-failure` with archived seeds and the longer soak profile.

## Repeat-Run Soak Profile

The soak profile is for nondeterministic HA failures that usually do not appear in
single smoke runs. It is not part of the fast PR path.

| Profile | Target | Workload | Nemesis | Runs | Duration per run | Purpose |
|---|---|---|---|---:|---:|---|
| `soak-local` | `pacman-3-data` | `append-failover` or `single-key-register` | `packet,kill` | 3 | 30 minutes | Developer reproduction of timing-sensitive failover defects. |
| `soak-nightly` | `patroni`, then `pacman-3-data` | `read-committed-txn` | `slow-network` plus `kill` | 5 | 30 minutes | Scheduled baseline comparison and PACMAN regression detection. |
| `soak-extended` | `pacman-3-data`, optional `pacman-3-data-1-witness` | `serializable-txn` | `repeated-failure` | 10 | 30 minutes | Pre-release or manual campaign for rare split-brain, stale-read, and rejoin bugs. |

Seed and schedule rules:

- Every run must persist the random seed, nemesis schedule, workload profile, target profile, PACMAN commit, PostgreSQL version, DCS version, and node inventory.
- Re-running with the same seed must reproduce the same planned nemesis sequence, even if timings drift slightly because of process startup or network conditions.
- Seeds from failing runs are promoted to a regression seed list and rerun before closing the bug.
- Passing nightly seeds are retained for trend analysis but can be rotated after artifact retention expires.

Artifact layout:

```text
store/
  patroni/<profile>/<timestamp>-seed-<seed>/
  pacman/<profile>/<timestamp>-seed-<seed>/
    jepsen-history.edn
    results.edn
    nemesis-schedule.edn
    pacman-cluster-before.json
    pacman-cluster-after.json
    pacman-history.json
    node-logs/
    postgres-logs/
    dcs-logs/
```

Pass/fail interpretation:

- A single checker failure fails the campaign and preserves all artifacts.
- Infrastructure failures are retried once with the same seed; if the retry fails before workload start, classify it as lab failure rather than PACMAN regression.
- PACMAN regressions require the matching Patroni baseline profile to be either passing or documented as a known Patroni limitation.
- Intermittent failures must include the failing seed and artifact path in the issue or CI summary.

## Local Lab Prerequisites And Bootstrap

The local Jepsen lab needs a Clojure control environment, SSH-reachable database
nodes, and a repeatable way to generate both PACMAN and Patroni inventories.

Required local tools:

- JDK, pinned to the version supported by the chosen Jepsen dependency.
- Leiningen for running Jepsen/Clojure commands.
- SSH client with key-based access from the Jepsen control node to every lab node.
- PostgreSQL client tools for manual verification and final artifact inspection.
- A substrate provider: start with VM or container hosts that match the Ansible/lab
  deployment model; add `kind` or k3s only when Kubernetes-native Jepsen targets are
  introduced.
- PACMAN build artifacts for the exact commit under test.
- Patroni deployment artifacts for the baseline target.

Substrate guidance:

| Substrate | Use | Status |
|---|---|---|
| VM or container hosts with SSH and systemd-like process control | First PACMAN and Patroni Jepsen campaigns | Required for initial local lab. |
| Existing PACMAN Ansible/lab shape | Inventory, package installation, PostgreSQL bootstrap, PACMAN config rendering | Preferred source for first implementation. |
| `kind` or k3s | Future Kubernetes-native target and parity with the reference project's k3s path | Optional, after process/Ansible target is stable. |

Inventory generation should produce one machine-readable file consumed by Jepsen and
one Ansible-compatible inventory for deployment. The generated inventory must include:

- target name, profile, and run id;
- node name, SSH host, SSH user, and SSH key;
- PACMAN role and PostgreSQL role at bootstrap;
- PostgreSQL host/port and credentials for the Jepsen workload user;
- PACMAN API and control addresses;
- DCS node addresses and DCS profile;
- witness membership when the witness target is selected;
- artifact directory for each node.

Bootstrap flow:

1. Build PACMAN artifacts for the commit under test.
2. Provision or reset the lab substrate.
3. Generate the Jepsen inventory and matching Ansible inventory.
4. Deploy DCS nodes.
5. Deploy the selected target, either `patroni` baseline or `pacman`.
6. Wait for a healthy initial primary and all expected members.
7. Run the Jepsen smoke command with `append-smoke` and `none`.
8. Collect pre-run topology snapshots, run metadata, and node log locations.
9. Execute the selected workload/nemesis profile.
10. Collect final reads, PACMAN or Patroni cluster state, operation history, process logs, PostgreSQL logs, DCS logs, Jepsen history, and checker output.
11. Destroy or reset the lab before the next seed unless the run is explicitly marked for interactive debugging.

Artifact review checklist:

- Open the Jepsen `results.edn` and checker summary first.
- Inspect `jepsen-history.edn` for failed, indeterminate, and successful write operations around nemesis windows.
- Compare `nemesis-schedule.edn` with node logs to verify faults hit the intended processes or links.
- Review PACMAN `/api/v1/history` and `/api/v1/cluster` snapshots for failover, rejoin, maintenance, and witness-specific state.
- Confirm PostgreSQL logs from former primaries do not show accepted writes after demotion or isolation.
- Preserve the full store path and seed in any issue filed from the run.

## Local Docker Runner

For laptop runs, use the Dockerized Jepsen control-node wrapper. It builds
`deploy/jepsen/Dockerfile`, mounts the repository at `/workspace`, mounts the
host Docker socket when available, and executes the same campaign contract used
by CI.

Commands:

```bash
make jepsen-docker-smoke
make jepsen-docker-nightly
```

Equivalent direct commands:

```bash
scripts/local/run-jepsen-docker.sh smoke
scripts/local/run-jepsen-docker.sh nightly
```

The runner image includes JDK 21, Leiningen, Docker CLI/Compose, SSH client,
PostgreSQL client tools, and common network/process debugging tools. It is a
Jepsen control node, not a PACMAN data node. The Make targets build the PACMAN
runtime RPM into `bin/ansible-install-rpm/` before starting the runner so the
Docker lab can install the exact workspace build.

The current harness lives in `jepsen/` and provides:

```text
jepsen/bin/ci-smoke
jepsen/bin/ci-nightly
```

`ci-smoke` bootstraps the Docker Compose lab and runs the lab verification stage.
`ci-nightly` bootstraps the same lab, verifies it, runs a planned switchover, and
verifies it again. A missing `lein`, missing runner, checker failure, or lab
bootstrap failure fails the command and preserves the same summary/artifact
layout used by CI.

Useful overrides:

```bash
PACMAN_JEPSEN_DOCKER_BUILD=false make jepsen-docker-smoke
PACMAN_JEPSEN_DOCKER_IMAGE=pacman-jepsen-runner:dev make jepsen-docker-smoke
PACMAN_JEPSEN_DOCKER_DRY_RUN=true make jepsen-docker-smoke
```

## Automation Placement Decision

Jepsen runs must execute outside the fast default PR pipeline. The default PR
checks should keep validating unit tests, regular integration suites, lint,
coverage, Docker image buildability, and short contract tests. They should not
provision multi-node Jepsen labs, run long nemesis campaigns, or block normal
review on nondeterministic fault-injection timing.

Automation should be split into separate stages:

| Stage | Trigger | Profiles | Required before merge |
|---|---|---|---|
| `jepsen-smoke` | Manual workflow dispatch, release-candidate branches, or explicit maintainer label | `patroni` and `pacman-3-data` with `append-smoke` and `none` | No, except when a change directly edits Jepsen harness, lab bootstrap, failover, fencing, DCS, or PostgreSQL control-plane behavior. |
| `jepsen-nightly` | Scheduled nightly job on the main development branch | `soak-nightly` with Patroni baseline followed by PACMAN | No, but failures open or update a tracked regression issue with artifacts. |
| `jepsen-release` | Manual pre-release or release branch gate | `soak-extended`, including witness coverage when enabled | Yes for release promotion, not for ordinary PR merge. |

Execution rules:

- Keep Jepsen jobs in a separate CI/CD workflow or stage from the default PR
  workflow so normal code review is not delayed by 30-minute campaigns.
- Allow maintainers to run `jepsen-smoke` on demand for PRs that touch HA-critical
  code paths.
- Run nightly campaigns only on trusted branches with access to the required VM,
  container-host, or Kubernetes lab substrate.
- Calibrate PACMAN failures against the matching Patroni baseline before
  classifying the result as a PACMAN regression.
- Publish a concise status back to the PR or scheduled-run summary, but keep the
  full Jepsen store artifacts in the long-running workflow.
- Treat lab provisioning failures separately from Jepsen checker failures so
  infrastructure noise does not hide database correctness regressions.

The GitHub Actions entry point is `.github/workflows/jepsen.yml`. It exposes:

- `pull_request` and `push` with a lightweight `Jepsen CI gate` job that validates
  the workflow runner contract without starting a long Jepsen campaign;
- `workflow_dispatch` with `campaign=smoke`, which runs `make jepsen-smoke`;
- `workflow_dispatch` with `campaign=nightly`, which runs `make jepsen-nightly`;
- a scheduled nightly trigger that also runs `make jepsen-nightly`.

The workflow installs JDK and Leiningen only when a `jepsen/` harness directory is
present. Until the harness lands, the jobs skip with a notice instead of turning
the scheduled workflow red. Once the harness exists, it must provide executable
campaign runners at `jepsen/bin/ci-smoke` and `jepsen/bin/ci-nightly`; missing
runners fail the Jepsen workflow clearly.

Each campaign writes a concise CI summary to `bin/jepsen-ci/<campaign>/summary.md`
and appends the same content to the GitHub Actions step summary. The summary
records the campaign, status, harness path, store path, commit, review checklist,
and an artifact index for HTML reports, histories, checker results, nemesis
schedules, logs, and JSON snapshots. GitHub Actions uploads both
`bin/jepsen-ci/<campaign>/` and the full `jepsen/store/` tree so operators can
review the short failure summary first, then open the detailed Jepsen artifacts
needed for regression tracking.

## Consequences

This choice keeps the valuable Jepsen parts: workload generators, nemesis schedule, history checking, and repeat-run campaigns. It avoids coupling PACMAN's first Jepsen campaign to k3s or Patroni-specific assumptions. The tradeoff is that PACMAN needs its own small Clojure target layer for install/start/stop, primary discovery, client connection routing, and artifact collection.

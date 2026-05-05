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

## Consequences

This choice keeps the valuable Jepsen parts: workload generators, nemesis schedule, history checking, and repeat-run campaigns. It avoids coupling PACMAN's first Jepsen campaign to k3s or Patroni-specific assumptions. The tradeoff is that PACMAN needs its own small Clojure target layer for install/start/stop, primary discovery, client connection routing, and artifact collection.

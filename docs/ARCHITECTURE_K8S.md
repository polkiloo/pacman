# PACMAN Kubernetes Architecture

This document describes the Kubernetes-native deployment model for **PACMAN**.

For the platform-neutral architecture and the bare-metal / VM-oriented control-plane model, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Goals

The Kubernetes mode keeps the PostgreSQL-specific HA logic of PACMAN while reusing Kubernetes-native primitives for:

- desired and observed state storage,
- workload orchestration,
- stable pod identity,
- service discovery,
- controller leader election,
- and operational lifecycle.

The core design decision is:

**inside Kubernetes, PACMAN should act as a PostgreSQL-aware operator, not as a second distributed control plane competing with Kubernetes itself.**

---

## Design Principles

- Kubernetes API is the authoritative source of truth for cluster intent and cluster status.
- PACMAN keeps the PostgreSQL failover brain: role detection, candidate ranking, promotion safety, and rejoin logic.
- `StatefulSet` provides stable pod identity and persistent storage bindings for PostgreSQL members.
- A single logical PACMAN controller instance makes cluster-wide decisions; Kubernetes `Lease` objects handle controller leader election.
- A pod-local PACMAN agent executes PostgreSQL-local actions such as observe, promote, demote, and rejoin.
- Kubernetes health signals are necessary but not sufficient; PostgreSQL-specific signals remain the final input for failover decisions.

---

## Kubernetes Resource Model

### `PostgresCluster` Custom Resource

The main PACMAN API in Kubernetes is a `PostgresCluster` CRD.

`spec` should describe the desired cluster:

- number of PostgreSQL instances,
- PostgreSQL image and version policy,
- storage class and volume size,
- replication and failover policy,
- maintenance mode,
- service exposure options,
- and bootstrap settings.

`status` should describe the observed cluster:

- current phase,
- current primary member,
- current epoch / generation,
- per-member health and replication state,
- active topology operation,
- failover / switchover / rejoin conditions,
- and recent history summary.

### `StatefulSet`

Each PostgreSQL member is represented by a Pod in a `StatefulSet`.

`StatefulSet` is the right primitive because it gives:

- stable names such as `cluster-0`, `cluster-1`, `cluster-2`,
- stable network identities,
- and a durable `PersistentVolumeClaim` per member.

### Services

Kubernetes networking should be split into distinct Services:

- a headless Service for stable per-member DNS,
- a `primary` Service that points to the current writable primary,
- and a `replicas` Service that points to read-only standbys.

The PACMAN controller updates pod labels so that Service routing follows the authoritative cluster role assignment.

### Coordination and Safety Objects

The controller should also own:

- a `Lease` for operator leader election,
- a `PodDisruptionBudget` to limit voluntary disruptions,
- `Secrets` for credentials and TLS material,
- and `ConfigMaps` or generated files for rendered PostgreSQL and PACMAN configuration.

---

## Component Mapping

| PACMAN concept | Kubernetes-native form |
| --- | --- |
| cluster spec | `PostgresCluster.spec` |
| cluster status | `PostgresCluster.status` |
| control-plane leader election | `Lease` |
| cluster topology materialization | `StatefulSet` + `Pods` + `PVCs` |
| stable member identity | `StatefulSet` ordinal DNS and PVC binding |
| primary endpoint | dedicated `Service` |
| replica endpoint | dedicated `Service` |
| local node agent | pod-local `pacmand` sidecar |
| operation history / conditions | CR `status`, Kubernetes Events |

---

## Runtime Components

### 1. PACMAN Operator

The PACMAN operator runs as a controller deployment inside the cluster.

Responsibilities:

- watch `PostgresCluster` objects,
- reconcile `StatefulSet`, `Services`, `PDB`, `Secrets`, and related objects,
- collect PostgreSQL-specific observed state from pod-local agents,
- decide whether failover or switchover is allowed,
- choose the best promotion candidate,
- update cluster status and topology labels,
- and coordinate rejoin of a former primary.

Only the elected controller leader should make topology-changing decisions.

### 2. Pod-Local `pacmand`

For the Kubernetes MVP, the recommended layout is a `pacmand` sidecar in every PostgreSQL Pod.

Responsibilities:

- observe local PostgreSQL state,
- collect role, timeline, and WAL progress signals,
- detect readiness for promotion or rejoin,
- execute local actions such as promote, restart, and rewind,
- and expose member status to the controller.

This keeps PostgreSQL-specific execution close to the database process while letting the cluster-wide controller remain stateless.

### 3. PostgreSQL Workload

The PostgreSQL container remains the data plane.

The PACMAN operator should treat it as a managed stateful workload whose topology changes are driven by domain logic, not just by container health.

---

## Reconciliation Model

The controller reconcile loop should follow this pattern:

1. Read `PostgresCluster.spec` and current cluster objects.
2. Ensure base objects exist: `StatefulSet`, headless Service, role Services, `PDB`, `Secrets`, and config inputs.
3. Collect observed state from Pods and pod-local agents.
4. Build a PostgreSQL-aware cluster view from role, LSN, timeline, lag, and health signals.
5. Compare desired state and observed state.
6. If needed, execute topology actions such as switchover, failover, or rejoin.
7. Update `PostgresCluster.status`, pod role labels, and Kubernetes Events.

This is the point where PACMAN fits naturally into the standard Kubernetes operator pattern: desired state in the API, observed state from the cluster, and reconcile logic in the controller.

---

## Failover Flow Inside Kubernetes

Kubernetes can restart Pods, but it cannot decide which PostgreSQL standby is safest to promote.

PACMAN keeps that decision logic.

The failover flow should look like this:

1. The controller notices that the current primary is unhealthy or unreachable from combined Kubernetes and PostgreSQL signals.
2. The controller waits for policy-based failure confirmation so that transient pod or network issues do not trigger unsafe promotion.
3. Pod-local agents report each standby's timeline, replay state, WAL position, lag, and readiness.
4. The controller ranks failover candidates and selects the best member.
5. The controller records a failover intent in cluster status.
6. The target member is instructed to promote through its local PACMAN agent.
7. After promotion is confirmed, the controller relabels Pods and moves the `primary` Service to the new primary.
8. The former primary is marked as `needs_rejoin`.
9. When the former primary comes back, PACMAN decides between `pg_rewind` and full reclone, then rejoins it as a replica.

The key safety property is that failover completion depends on PostgreSQL state validation, not only on Pod readiness.

---

## Switchover Flow Inside Kubernetes

Planned switchovers follow the same structure but start from an explicit operator action:

1. validate that the requested standby is healthy and sufficiently caught up,
2. enter an operation state in cluster status,
3. demote or stop writes on the current primary,
4. promote the chosen standby,
5. switch the `primary` Service,
6. and update roles, epoch, and operation history.

This keeps the external client endpoint stable while the writable member changes underneath.

---

## Rejoin Model

When a former primary returns, PACMAN must not let it re-enter as writable on its old timeline.

The expected Kubernetes rejoin flow is:

1. the Pod becomes reachable again,
2. the controller identifies it as a former primary or divergent member,
3. the pod-local agent evaluates rewind versus reclone,
4. local state is repaired,
5. PostgreSQL is started as a replica,
6. and the controller clears the `needs_rejoin` condition once replication is healthy.

---

## What Kubernetes Solves and What It Does Not

Kubernetes solves:

- Pod scheduling and restart,
- stable identities through `StatefulSet`,
- persistent volume attachment,
- service discovery,
- and controller coordination primitives.

Kubernetes does not solve:

- PostgreSQL timeline and WAL safety,
- candidate ranking for promotion,
- rejoin semantics after divergence,
- or fencing policy strong enough to prevent every form of split-brain by itself.

That remaining logic is exactly where PACMAN adds value.

---

## MVP Boundary

The Kubernetes MVP should aim for:

- one `PostgresCluster` CRD,
- one leader-elected PACMAN controller,
- one PostgreSQL `StatefulSet` with a `pacmand` sidecar per Pod,
- `primary` and `replicas` Services,
- PostgreSQL-aware failover and rejoin,
- and repeatable end-to-end tests in a Kubernetes lab environment.

Advanced concerns such as richer fencing integrations, multi-cluster DR, and sharded control-plane designs can stay out of the first Kubernetes-native release.

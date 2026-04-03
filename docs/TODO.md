# PACMAN MVP TODO

This document tracks the initial MVP work for **PACMAN** — **Postgres Autonomous Cluster Manager**.

The goal of the MVP is to deliver a minimal but serious PostgreSQL HA control plane with local node agents, internal cluster coordination, automatic failover, planned switchover, and explicit rejoin of former primaries.

---

## 1. Repository Foundation

- [x] initialize Go module
- [x] create base project layout
- [x] add `cmd/pacmand`
- [x] add `cmd/pacmanctl`
- [x] add `internal/` package structure
- [x] add `Makefile`
- [x] add CI workflow
- [x] add lint configuration
- [x] add test workflow
- [x] add structured logging
- [x] add `testcontainers-go` integration test environment for `pacmand` and `pacmanctl` with `postgres:17`
- [ ] add metrics scaffolding
- [ ] add local development scripts

---

## 2. Core Domain Model

- [x] define cluster roles
- [x] define node roles
- [x] define `ClusterSpec`
- [x] define `ClusterStatus`
- [x] define `MemberSpec`
- [x] define `MemberStatus`
- [x] define epoch / generation model
- [x] define failover state machine
- [x] define switchover state machine
- [x] define rejoin flow model
- [x] define operation history model
- [x] define maintenance mode model

---

## 3. Configuration System

- [x] define bootstrap node configuration format
- [x] define config loader
- [x] define config validation
- [x] define defaults
- [x] define TLS configuration section
- [x] define PostgreSQL local config section
- [x] define cluster bootstrap config section
- [x] reject unsafe local-only overrides for cluster truth

---

## 4. Local Agent

- [x] implement daemon startup
- [x] implement local heartbeat loop
- [x] detect PostgreSQL availability
- [x] detect current PostgreSQL role
- [x] detect recovery state
- [x] collect system identifier
- [x] collect timeline information
- [x] collect receive / replay / flush LSN
- [x] collect replication lag signals
- [x] collect local process health
- [x] publish observed state to control plane

---

## 5. PostgreSQL Background Worker Extension

- [x] define extension boundary between `pacmand` process mode and in-PostgreSQL background-worker mode
- [x] extract reusable local-agent core from `pacmand` so the extension stays a thin bootstrap layer
- [x] define extension name, on-disk layout, and PostgreSQL version support policy
- [x] scaffold PostgreSQL extension sources, `.control` file, and SQL install/upgrade scripts
- [x] implement background worker registration via `shared_preload_libraries`
- [x] define GUC-based configuration bridge from PostgreSQL settings to PACMAN node-runtime config
- [x] wire extension startup, shutdown, and restart handling to the shared PACMAN local-agent lifecycle
- [x] add dedicated build target for the PostgreSQL extension artifact separate from `pacmand`
- [x] add packaging/install flow for extension binaries, control files, and SQL assets
- [x] add `testcontainers-go` fixture/image variant with the extension installed and preloaded
- [x] add Docker-backed integration tests for extension startup, shutdown, invalid config, and local state observation

---

## 6. PostgreSQL Integration Layer

- [x] implement PostgreSQL connection layer
- [x] implement health queries
- [x] implement role detection queries
- [x] implement recovery-state detection
- [x] implement system identifier lookup
- [x] implement WAL progress queries
- [x] implement lag estimation
- [x] integrate with `pg_ctl`
- [x] integrate with `pg_rewind`
- [x] implement local standby configuration rendering
- [x] implement promote action
- [x] implement restart / reload handling

---

## 7. Control Plane

- [x] implement member registration
- [x] implement member discovery
- [x] implement control-plane leader election
- [x] implement logical replicated state store
- [x] implement cluster source of truth model
- [x] implement desired state storage
- [x] implement observed state aggregation
- [x] implement desired vs observed reconciliation
- [x] implement maintenance mode
- [x] implement operation journal
- [x] implement member priorities
- [x] implement no-failover tags

---

## 8. Failover Engine

- [x] define failover eligibility rules
- [x] define candidate ranking rules
- [x] implement quorum-aware primary failure confirmation
- [x] implement failover intent creation
- [x] implement fencing hook interface
- [x] implement promotion orchestration
- [x] publish new epoch after failover
- [x] mark former primary as `needs_rejoin`
- [x] record failover history

---

## 9. Switchover Engine

- [x] define switchover validation rules
- [x] validate target standby readiness
- [x] implement planned topology transition
- [x] coordinate demotion of current primary
- [x] coordinate promotion of target standby
- [x] publish new epoch
- [x] record switchover result in history

---

## 10. Rejoin Flow

- [x] detect former primary state
- [x] detect divergence requirements
- [x] decide rewind vs reclone path
- [x] run `pg_rewind` workflow
- [x] render standby configuration
- [x] restart as standby
- [x] verify replication health after rejoin
- [x] mark node as healthy cluster member again

---

## 11. Pluggable DCS Layer

See [ARCHITECTURE_DCS.md](ARCHITECTURE_DCS.md) for full design.

### DCS Interface and Abstraction
- [ ] define `DCS` interface in `internal/dcs/dcs.go` (Get, Set, CompareAndSet, Delete, List, Campaign, Leader, Resign, Touch, Alive, Watch, Initialize, Close)
- [ ] define `KeyValue`, `LeaderLease`, `WatchEvent`, `SetOption` types
- [ ] define DCS error types (`ErrKeyNotFound`, `ErrRevisionMismatch`, etc.)
- [ ] define DCS configuration model (`backend`, `clusterName`, `ttl`, backend-specific blocks)
- [ ] add DCS config section to `internal/config/config.go` and validation
- [ ] define key space layout (`/pacman/<cluster>/config`, `members/`, `status/`, `operation`, `history/`, `maintenance`, `epoch`)

### DCS Conformance Test Suite
- [ ] create `internal/dcs/dcstest/` shared conformance test suite
- [ ] add Get/Set/Delete conformance tests
- [ ] add CompareAndSet conflict and success conformance tests
- [ ] add List prefix conformance tests
- [ ] add Campaign/Leader/Resign conformance tests
- [ ] add Touch/Alive TTL conformance tests
- [ ] add Watch event delivery conformance tests

### Memory Backend (testing)
- [ ] implement `internal/dcs/memory/memory.go` — in-memory `DCS` for unit tests
- [ ] pass all conformance tests

### ControlPlane Refactoring
- [ ] refactor `ControlPlane` to accept `dcs.DCS` instead of being `MemoryStateStore`
- [ ] implement `NodeStatePublisher` via `DCS.Set("status/<node>", ..., WithTTL)`
- [ ] implement `MemberRegistrar` via `DCS.Set("members/<node>", ...)`
- [ ] implement `MemberDiscovery` via `DCS.List("members/")` + `DCS.List("status/")`
- [ ] implement `LeaderElector` via `DCS.Campaign()` / `DCS.Leader()`
- [ ] implement `DesiredStateStore` via `DCS.Get("config")` / `DCS.CompareAndSet("config", ...)`
- [ ] implement `ObservedStateStore` via in-process aggregation from DCS data
- [ ] implement `Reconciler` via DCS reads + in-process aggregation
- [ ] implement `MaintenanceStore` via `DCS.CompareAndSet("maintenance", ...)`
- [ ] implement `OperationJournal` via `DCS.Get("operation")` + `DCS.List("history/")`
- [ ] implement local read cache with watch-driven invalidation
- [ ] verify all existing controlplane tests pass with memory DCS backend

### Embedded Raft Backend (`hashicorp/raft`)

Framework: **`github.com/hashicorp/raft`** + **`github.com/hashicorp/raft-boltdb/v2`**
- Full-stack Raft: log replication, leader election, snapshots, transport, membership changes
- Battle-tested in HashiCorp Vault (Integrated Storage), Consul, Nomad
- MPL 2.0 license, actively maintained, 2,289+ importers
- Alternatives considered and deferred: `etcd-io/raft` (too much custom code), `lni/dragonboat` (less production validation)

- [ ] add `hashicorp/raft` and `raft-boltdb/v2` dependencies
- [ ] implement `internal/dcs/raft/fsm.go` — Raft FSM with flat key-value state
- [ ] implement `internal/dcs/raft/transport.go` — TCP transport with TLS support
- [ ] implement `internal/dcs/raft/snapshot.go` — snapshot handling
- [ ] implement `internal/dcs/raft/raft.go` — `DCS` interface backed by Raft consensus
- [ ] implement `internal/dcs/raft/config.go` — Raft-specific configuration
- [ ] implement TTL expiration via background goroutine with Raft-applied deletes
- [ ] implement leader read path (`raft.VerifyLeader()` for linearizable reads)
- [ ] wire Raft bootstrap into `pacmand` startup when `dcs.backend: raft`
- [ ] pass all conformance tests
- [ ] add 3-node Raft integration tests with testcontainers

### etcd Backend
- [ ] add `go.etcd.io/etcd/client/v3` dependency
- [ ] implement `internal/dcs/etcd/etcd.go` — `DCS` backed by etcd v3
- [ ] implement CompareAndSet via etcd transactions
- [ ] implement leader election via `concurrency.Election`
- [ ] implement session/TTL via etcd leases
- [ ] implement Watch via etcd Watch
- [ ] pass all conformance tests
- [ ] add testcontainers-based etcd integration tests

### Post-MVP Backends (deferred)

Additional backends can be added after MVP by implementing the same `DCS` interface:
- ZooKeeper — znode versions for CAS, ephemeral znodes for TTL
- Consul — `ModifyIndex` for CAS, sessions for TTL, blocking queries for watch
- Kubernetes — ConfigMap + `resourceVersion` for CAS, Lease for leader election

---

## 12. API

### Contract
- [x] draft OpenAPI spec for control-plane API
- [x] review OpenAPI contract against domain model and failover safety rules
- [x] define API authentication and authorization model
- [x] define stable API versioning and compatibility policy

### Implementation
- [x] implement API router with Fiber
- [x] expose `GET /health`
- [x] expose `GET /liveness`
- [x] expose `GET /readiness`
- [x] expose `GET /primary`
- [x] expose `GET /replica`
- [x] expose `GET /api/v1/cluster`
- [x] expose `GET /api/v1/cluster/spec`
- [x] expose `GET /api/v1/nodes/{nodeName}`
- [x] expose `GET /api/v1/members`
- [x] add HTTP middleware for request IDs, auth hooks, and common API concerns
- [x] expose `GET /api/v1/history`
- [x] expose `GET /api/v1/maintenance`
- [x] expose `PUT /api/v1/maintenance`
- [x] expose `GET /api/v1/diagnostics`
- [x] expose `POST /api/v1/operations/switchover`
- [x] expose `DELETE /api/v1/operations/switchover`
- [x] expose `POST /api/v1/operations/failover`
- [x] serve published OpenAPI document from the API process

---

## 13. CLI (`pacmanctl`)

- [x] implement `cluster status`
- [x] implement `members list`
- [ ] implement `cluster switchover`
- [ ] implement `cluster failover`
- [ ] implement `cluster maintenance enable`
- [ ] implement `cluster maintenance disable`
- [ ] implement `history list`
- [ ] implement `cluster spec show`
- [ ] implement `node status`
- [ ] implement diagnostics commands
- [ ] add `patronictl`-compatible command aliases, flags, and output modes for automation scripts

---

## 14. Security

- [ ] add TLS for external endpoints
- [ ] add mTLS between cluster members
- [ ] implement certificate loading
- [ ] define admin authorization model
- [ ] secure sensitive config handling

---

## 15. Structured Logging (`slog`)

- [ ] expand `pacmand` runtime `slog` coverage and field consistency
- [ ] expand `pacmanctl` runtime `slog` coverage and field consistency
- [ ] add `slog`-backed HTTP access logging
- [ ] add structured event logs for cluster lifecycle and state transitions
- [ ] add request, node, member, and operation correlation fields across API and control-plane logs
- [ ] add audit logging for topology changes and maintenance mode changes
- [ ] add reconciliation debug logging with safe verbosity controls
- [ ] define embedded-worker logging, error propagation, and failure-isolation rules
- [ ] add secret redaction rules and logging-focused test coverage inspired by Patroni `tests/test_log.py` and `tests/test_utils.py`

---

## 16. Observability

- [ ] add Prometheus metrics
- [ ] add health endpoints
- [ ] add diagnostics dump
- [ ] add trace points for failover / switchover / rejoin

---

## 17. Packaging and Operations

- [ ] add systemd unit files
- [ ] add example configs
- [ ] add local lab environment
- [ ] add bootstrap scripts for test cluster
- [ ] add container image for lab/testing
- [ ] define local state directory layout
- [ ] define upgrade-safe persistent control-plane state path

---

## 18. Testing

### Testcontainers Environment
- [x] add Docker test image for `pacmand` and `pacmanctl` with PostgreSQL 17 client tools
- [x] add shared `testcontainers-go` network harness for multi-container tests
- [x] add PostgreSQL 17 sidecar fixture per `pacmand` node
- [x] add local `make test-integration` target for Docker-backed test runs
- [x] add cluster-layout smoke test for `pacmand`, `pacmanctl`, and `postgres:17`
- [ ] extend PostgreSQL-backed fixture with replication/bootstrap orchestration
- [ ] add CI job for Docker-backed integration and cluster smoke tests

### Cluster Configuration Validation Tasks
- [x] build the PACMAN test image and create an isolated Docker network for the scenario
- [x] start three `postgres:17` containers with stable aliases `pacmand-1-postgres`, `pacmand-2-postgres`, and `pacmand-3-postgres`
- [] start three `pacmand` containers on the same network, one per PostgreSQL node, with each `pacmand` pointed at its neighboring PostgreSQL container
- [] start one `pacmanctl` container on the same network so that control commands run from the same cluster context
- [] verify every `pacmand` node can execute the daemon binary and return build metadata through `pacmand -version`
- [] verify every `pacmand` container can resolve and probe its local `postgres:17` peer with `pg_isready` and `psql`
- [] verify every PostgreSQL sidecar reports a PostgreSQL 17 server version and remains attached to the shared cluster network with the expected alias mapping
- [] verify `pacmanctl -version` and `pacmanctl cluster status` execute successfully from the client container
- [ ] extend the same topology with replication/bootstrap wiring and assert switchover, failover, and rejoin flows after the control-plane API and local agent logic are implemented

### Unit Tests
- [ ] add cluster domain model unit tests
- [ ] add config validation unit tests
- [ ] add state machine unit tests
- [ ] add candidate ranking unit tests
- [ ] add failover policy unit tests

### Integration Tests
- [ ] add PostgreSQL role detection integration tests
- [ ] add promote workflow integration tests
- [ ] add standby configuration integration tests
- [ ] add rejoin / rewind integration tests
- [ ] add maintenance mode integration tests

### End-to-End Tests
- [ ] add 3-node cluster bootstrap end-to-end test
- [ ] add planned switchover end-to-end test
- [ ] add automatic failover end-to-end test
- [ ] add former primary rejoin end-to-end test
- [ ] add network partition end-to-end test
- [ ] add witness-assisted quorum end-to-end test


### Patroni-Inspired Coverage Groups
- [ ] add REST API and HTTP server coverage inspired by Patroni `tests/test_api.py`
- [ ] add daemon entrypoint and process lifecycle coverage inspired by Patroni `tests/test_patroni.py`
- [ ] add CLI command and presentation coverage inspired by Patroni `tests/test_ctl.py`
- [ ] add config parsing, generation, validator, and file-permission coverage inspired by Patroni `tests/test_config.py`, `tests/test_config_generator.py`, `tests/test_validator.py`, and `tests/test_file_perm.py`
- [ ] add async executor, callback executor, and cancellable subprocess coverage inspired by Patroni `tests/test_async_executor.py`, `tests/test_callback_executor.py`, and `tests/test_cancellable.py`
- [ ] add HA loop, quorum, synchronous replication, and slot-management coverage inspired by Patroni `tests/test_ha.py`, `tests/test_quorum.py`, `tests/test_sync.py`, and `tests/test_slots.py`
- [ ] add PostgreSQL bootstrap, local lifecycle, and postmaster coverage inspired by Patroni `tests/test_bootstrap.py`, `tests/test_postgresql.py`, and `tests/test_postmaster.py`
- [ ] add rewind and replica rejoin coverage inspired by Patroni `tests/test_rewind.py`
- [ ] add backup, restore, and cloud-integration coverage inspired by Patroni `tests/test_aws.py`, `tests/test_barman.py`, and `tests/test_wale_restore.py`
- [ ] add DCS backend contract coverage for Consul, etcd, etcd3, Exhibitor, Kubernetes, Raft, and ZooKeeper inspired by Patroni `tests/test_consul.py`, `tests/test_etcd.py`, `tests/test_etcd3.py`, `tests/test_exhibitor.py`, `tests/test_kubernetes.py`, `tests/test_raft.py`, `tests/test_raft_controller.py`, and `tests/test_zookeeper.py`
- [ ] add watchdog and fencing coverage inspired by Patroni `tests/test_watchdog.py`
- [ ] add distributed-topology and MPP coverage inspired by Patroni `tests/test_citus.py` and `tests/test_mpp.py`

### Jepsen Fault-Injection Campaigns
Inspired by [Wolfsrudel/database-postgres-ha-patroni-testing-jepsen](https://github.com/Wolfsrudel/database-postgres-ha-patroni-testing-jepsen), which uses Jepsen + Clojure/Leiningen with a Vagrant / k3s lab and a Patroni cluster target.
- [ ] evaluate whether to adapt the Jepsen harness shape directly or build a PACMAN-specific harness with the same workload / nemesis model
- [ ] add a Patroni baseline target so PACMAN Jepsen runs can be calibrated against a known HA implementation before PACMAN-specific assertions are trusted
- [ ] define a PACMAN Jepsen target topology, including 3 data nodes and optional witness coverage where PACMAN semantics differ from Patroni
- [ ] define Jepsen workload coverage for append/register-style histories, single-key stress, read committed checks, and serializable checks
- [ ] define Jepsen nemesis coverage for `none`, `packet`, `kill`, combined `packet,kill`, and slow-network / repeated-failure campaigns inspired by the Patroni repo
- [ ] add a repeat-run soak profile for non-deterministic failures, including multi-run 30-minute campaigns and archived failure seeds / histories
- [ ] document local Jepsen lab prerequisites and bootstrap flow, including `JDK`, `Leiningen`, VM/Kubernetes substrate, node inventory generation, and artifact review
- [ ] decide where Jepsen runs execute in automation, preferring separate long-running CI/CD stages or scheduled jobs instead of the fast default PR pipeline
- [ ] add separate CI/CD jobs for Jepsen smoke validation on demand and extended nightly / scheduled Jepsen campaigns
- [ ] publish Jepsen HTML/history artifacts and concise failure summaries from CI/CD for operator review and regression tracking
---

## 18. Kubernetes-Native MVP

This track captures the Kubernetes-native operator model described in [ARCHITECTURE_K8S.md](ARCHITECTURE_K8S.md).

### CRD and API Model
- [ ] define `PostgresCluster` CRD
- [ ] define `PostgresClusterSpec`
- [ ] define `PostgresClusterStatus`
- [ ] define `status.conditions` model for failover / switchover / rejoin
- [ ] define per-member status projection in CR `status`
- [ ] define maintenance and failover policy fields for Kubernetes mode

### Controller
- [ ] scaffold controller-manager
- [ ] implement leader election with Kubernetes `Lease`
- [ ] implement reconcile loop for `PostgresCluster`
- [ ] watch `StatefulSet`, `Pod`, `PVC`, `Service`, and `PodDisruptionBudget` objects
- [ ] aggregate observed PostgreSQL state from pod-local agents
- [ ] persist current primary and epoch into CR `status`
- [ ] emit Kubernetes Events for topology changes

### Workloads and Services
- [ ] render PostgreSQL `StatefulSet`
- [ ] add `pacmand` sidecar to each PostgreSQL Pod
- [ ] render headless Service for stable member DNS
- [ ] render `primary` Service
- [ ] render `replicas` Service
- [ ] render `PodDisruptionBudget`
- [ ] render `Secret` and `ConfigMap` inputs for bootstrap and replication config
- [ ] define pod labels and annotations used for role routing

### Failover and Rejoin in Kubernetes
- [ ] detect primary loss from combined Kubernetes and PostgreSQL signals
- [ ] rank promotion candidates from agent-reported timeline / LSN / lag
- [ ] implement promotion orchestration through the pod-local agent
- [ ] switch `primary` Service to the new primary after confirmed promotion
- [ ] mark former primary as `needs_rejoin` in CR `status`
- [ ] implement `pg_rewind` or reclone workflow for a returning former primary
- [ ] prevent unsafe failover completion without fencing confirmation
- [ ] handle node drain and eviction interactions with failover policy

### Kubernetes Packaging and Testing
- [ ] add RBAC manifests
- [ ] add operator deployment manifest
- [ ] add local `kind`-based lab environment
- [ ] add Kubernetes bootstrap end-to-end test
- [ ] add Kubernetes planned switchover end-to-end test
- [ ] add Kubernetes automatic failover end-to-end test
- [ ] add Kubernetes former primary rejoin end-to-end test
- [ ] add pod deletion and node drain scenario coverage

---

## Suggested Milestones

## Milestone 1 — Local Agent
- [ ] implement daemon process
- [ ] implement PostgreSQL state detection
- [ ] expose local API
- [ ] implement lifecycle management

## Milestone 2 — Cluster View
- [ ] implement multi-node membership
- [ ] implement cluster status aggregation
- [ ] implement leader election
- [ ] implement source of truth model

## Milestone 3 — Planned Switchover
- [ ] implement desired state model
- [ ] implement promote / demote orchestration
- [ ] implement operation history

## Milestone 4 — Automatic Failover
- [ ] implement quorum-based failure confirmation
- [ ] implement candidate selection
- [ ] implement controlled promotion
- [ ] implement epoch transition

## Milestone 5 — Rejoin and Hardening
- [ ] implement rejoin workflow
- [ ] integrate rewind support
- [ ] implement maintenance mode
- [ ] implement reliability improvements
- [ ] stabilize end-to-end tests
- [ ] establish separate Jepsen fault-injection validation outside the fast PR pipeline

## Milestone 6 — Kubernetes Operator MVP
- [ ] implement `PostgresCluster` CRD and status model
- [ ] implement leader-elected controller
- [ ] render `StatefulSet` plus `primary` and `replicas` Services
- [ ] implement failover and rejoin flow inside Kubernetes
- [ ] add repeatable `kind` end-to-end coverage

---

## Nice-to-Have After MVP

- [ ] add synchronous replication policies
- [ ] add richer fencing backends
- [ ] improve dedicated witness mode
- [ ] add standby-cluster / DR support
- [ ] automate endpoint management
- [ ] add web UI

### Cascading Replication
- [ ] define post-MVP product scope and safety rules for cascading replication, including when PACMAN may prefer direct vs cascaded upstreams
- [ ] extend the domain model with explicit replication upstream metadata so a replica can stream either from the current primary or from another replica
- [ ] extend config and API surfaces to declare cascade-eligible members, preferred upstreams, and topology constraints for WAN / AZ-aware layouts
- [ ] teach the local standby renderer to emit upstream-specific `primary_conninfo`, slot naming, and reconfiguration artifacts for cascade chains
- [ ] collect upstream identity and cascade health signals from local agents so control-plane decisions can see who each replica is following
- [ ] add topology validation rules that reject unsafe cascade graphs such as loops, disconnected branches, or chains that violate failover policy
- [ ] teach reconciliation to rewire cascaded replicas after switchover and failover so replicas move to the correct new upstream automatically
- [ ] extend failover candidate ranking to account for cascade depth, upstream health, and lag amplification across the replication tree
- [ ] define slot and retention policy for cascaded replicas so WAL retention remains sufficient for downstream branches during outages
- [ ] add `testcontainers-go` integration coverage for primary -> replica -> replica topologies, including upstream loss, promotion, and topology rewire scenarios

### Multiple Replicas and Topology Policies
- [ ] define the post-MVP product scope for clusters with many replicas, including supported replica counts, placement assumptions, and operator guarantees
- [ ] extend the domain model with per-replica priority, availability-zone / location metadata, and promotion constraints used by failover ranking
- [ ] add config and API support for replica tags such as `no_failover`, `no_sync`, `preferred_candidate`, and read-only routing hints
- [ ] implement control-plane aggregation that maintains a cluster-wide replica set view with lag, timeline, replay progress, and health per replica
- [ ] define candidate-ranking rules for many-replica clusters so failover prefers the safest and most current replica rather than a simple first-ready member
- [ ] implement dynamic synchronous-standby selection policies for clusters with multiple replicas and changing health / lag conditions
- [ ] add replication-slot and sender management rules for many-replica topologies so the primary can track and protect all attached standbys safely
- [ ] implement reconciliation for adding, removing, and replacing replicas without destabilizing the current primary or existing healthy replicas
- [ ] add API and CLI views that expose the full ordered replica set, promotion eligibility, and current upstream / sync state for each member
- [ ] add `testcontainers-go` integration coverage for clusters with 3+ replicas, including direct-replication fanout, replica replacement, sync-standby rotation, and candidate ranking scenarios

### Managed Logical Replication and Downstream Delivery
- [ ] define post-MVP product scope for `physical HA + managed logical replication`, including guarantees, non-goals, and failure model relative to core HA
- [ ] define cluster/domain model for logical publications, downstream subscriptions, delivery pipelines, and per-sink status
- [ ] add config/API model for managed logical replication pipelines, including publication selection, table filters, and sink credentials
- [ ] implement PostgreSQL publication and logical replication slot orchestration for PACMAN-managed downstream delivery
- [ ] implement failover-safe logical slot handling and readiness checks so planned switchover and failover preserve downstream continuity where PostgreSQL supports it
- [ ] implement logical change-consumption worker with durable offsets/checkpoints and backpressure handling
- [ ] implement Kafka sink for logical change delivery, including topic mapping, key selection, retry policy, and delivery state reporting
- [ ] implement ClickHouse sink for logical change delivery, including table mapping, batching policy, idempotency/deduplication strategy, and delivery state reporting
- [ ] implement topology reconciliation after promotion/rejoin so logical delivery resumes correctly after primary changes
- [ ] add `testcontainers-go` integration coverage for PostgreSQL + Kafka + ClickHouse delivery pipelines, including failover/switchover continuity scenarios

---

## MVP Definition of Done

Close the MVP only after validating that PACMAN can reliably demonstrate the following outcomes:

- [ ] validate bootstrap of a small PostgreSQL HA cluster
- [ ] validate visibility into current topology
- [ ] validate planned switchover
- [ ] validate automatic failover with quorum
- [ ] validate prevention of unsafe dual-primary behavior
- [ ] validate explicit rejoin of a former primary
- [ ] validate basic operator-facing CLI and API
- [ ] validate repeatable integration and end-to-end test coverage
- [ ] validate scheduled or on-demand Jepsen fault-injection coverage in separate CI/CD stages
- [ ] validate operator-managed Kubernetes deployment with `StatefulSet` and role-based Services
- [ ] validate repeatable Kubernetes failover test coverage in `kind`

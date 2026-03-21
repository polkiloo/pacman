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
- [ ] detect current PostgreSQL role
- [ ] detect recovery state
- [ ] collect system identifier
- [ ] collect timeline information
- [ ] collect receive / replay / flush LSN
- [ ] collect replication lag signals
- [ ] collect local process health
- [ ] publish observed state to control plane

---

## 5. PostgreSQL Background Worker Extension

- [ ] define extension boundary between `pacmand` process mode and in-PostgreSQL background-worker mode
- [ ] extract reusable local-agent core from `pacmand` so the extension stays a thin bootstrap layer
- [ ] define extension name, on-disk layout, and PostgreSQL version support policy
- [ ] scaffold PostgreSQL extension sources, `.control` file, and SQL install/upgrade scripts
- [ ] implement background worker registration via `shared_preload_libraries`
- [ ] define GUC-based configuration bridge from PostgreSQL settings to PACMAN node-runtime config
- [ ] wire extension startup, shutdown, and restart handling to the shared PACMAN local-agent lifecycle
- [ ] define logging, error propagation, and failure-isolation rules for the embedded worker
- [ ] add dedicated build target for the PostgreSQL extension artifact separate from `pacmand`
- [ ] add packaging/install flow for extension binaries, control files, and SQL assets
- [ ] add `testcontainers-go` fixture/image variant with the extension installed and preloaded
- [ ] add Docker-backed integration tests for extension startup, shutdown, invalid config, and local state observation

---

## 6. PostgreSQL Integration Layer

- [ ] implement PostgreSQL connection layer
- [ ] implement health queries
- [ ] implement role detection queries
- [ ] implement recovery-state detection
- [ ] implement system identifier lookup
- [ ] implement WAL progress queries
- [ ] implement lag estimation
- [ ] integrate with `pg_ctl`
- [ ] integrate with `pg_rewind`
- [ ] implement local standby configuration rendering
- [ ] implement promote action
- [ ] implement restart / reload handling

---

## 7. Control Plane

- [ ] implement member registration
- [ ] implement member discovery
- [ ] implement control-plane leader election
- [ ] implement logical replicated state store
- [ ] implement cluster source of truth model
- [ ] implement desired state storage
- [ ] implement observed state aggregation
- [ ] implement desired vs observed reconciliation
- [ ] implement maintenance mode
- [ ] implement operation journal
- [ ] implement member priorities
- [ ] implement no-failover tags

---

## 8. Failover Engine

- [ ] define failover eligibility rules
- [ ] define candidate ranking rules
- [ ] implement quorum-aware primary failure confirmation
- [ ] implement failover intent creation
- [ ] implement fencing hook interface
- [ ] implement promotion orchestration
- [ ] publish new epoch after failover
- [ ] mark former primary as `needs_rejoin`
- [ ] record failover history

---

## 9. Switchover Engine

- [ ] define switchover validation rules
- [ ] validate target standby readiness
- [ ] implement planned topology transition
- [ ] coordinate demotion of current primary
- [ ] coordinate promotion of target standby
- [ ] publish new epoch
- [ ] record switchover result in history

---

## 10. Rejoin Flow

- [ ] detect former primary state
- [ ] detect divergence requirements
- [ ] decide rewind vs reclone path
- [ ] run `pg_rewind` workflow
- [ ] render standby configuration
- [ ] restart as standby
- [ ] verify replication health after rejoin
- [ ] mark node as healthy cluster member again

---

## 11. API

### Contract
- [x] draft OpenAPI spec for control-plane API
- [ ] review OpenAPI contract against domain model and failover safety rules
- [ ] define API authentication and authorization model
- [ ] define stable API versioning and compatibility policy

### Implementation
- [ ] expose `GET /health`
- [ ] expose `GET /liveness`
- [ ] expose `GET /readiness`
- [ ] expose `GET /primary`
- [ ] expose `GET /replica`
- [ ] expose `GET /api/v1/cluster`
- [ ] expose `GET /api/v1/cluster/spec`
- [ ] expose `GET /api/v1/nodes/{nodeName}`
- [ ] expose `GET /api/v1/members`
- [ ] expose `GET /api/v1/history`
- [ ] expose `GET /api/v1/maintenance`
- [ ] expose `PUT /api/v1/maintenance`
- [ ] expose `GET /api/v1/diagnostics`
- [ ] expose `POST /api/v1/operations/switchover`
- [ ] expose `DELETE /api/v1/operations/switchover`
- [ ] expose `POST /api/v1/operations/failover`
- [ ] serve published OpenAPI document from the API process

---

## 12. CLI (`pacmanctl`)

- [ ] implement `cluster status`
- [ ] implement `members list`
- [ ] implement `cluster switchover`
- [ ] implement `cluster failover`
- [ ] implement `cluster maintenance enable`
- [ ] implement `cluster maintenance disable`
- [ ] implement `history list`
- [ ] implement `cluster spec show`
- [ ] implement `node status`
- [ ] implement diagnostics commands

---

## 13. Security

- [ ] add TLS for external endpoints
- [ ] add mTLS between cluster members
- [ ] implement certificate loading
- [ ] define admin authorization model
- [ ] add audit logging for topology changes
- [ ] secure sensitive config handling

---

## 14. Observability

- [ ] add Prometheus metrics
- [ ] add health endpoints
- [ ] add structured event log
- [ ] add diagnostics dump
- [ ] add trace points for failover / switchover / rejoin
- [ ] add useful debug logging for reconciliation

---

## 15. Packaging and Operations

- [ ] add systemd unit files
- [ ] add example configs
- [ ] add local lab environment
- [ ] add bootstrap scripts for test cluster
- [ ] add container image for lab/testing
- [ ] define local state directory layout
- [ ] define upgrade-safe persistent control-plane state path

---

## 16. Testing

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
- [ ] add logging and utility helper coverage inspired by Patroni `tests/test_log.py` and `tests/test_utils.py`
- [ ] add distributed-topology and MPP coverage inspired by Patroni `tests/test_citus.py` and `tests/test_mpp.py`
---

## 17. Kubernetes-Native MVP

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
- [ ] validate operator-managed Kubernetes deployment with `StatefulSet` and role-based Services
- [ ] validate repeatable Kubernetes failover test coverage in `kind`

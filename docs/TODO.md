# PACMAN MVP TODO

This document tracks the initial MVP work for **PACMAN** — **Postgres Autonomous Cluster Manager**.

The goal of the MVP is to deliver a minimal but serious PostgreSQL HA control plane with:

- local node agents,
- internal cluster coordination,
- automatic failover,
- planned switchover,
- and explicit rejoin of former primaries.

---

## 1. Repository Foundation

- [ ] initialize Go module
- [ ] create base project layout
- [ ] add `cmd/pacmand`
- [ ] add `cmd/pacmanctl`
- [ ] add `internal/` package structure
- [ ] add `Makefile`
- [ ] add CI workflow
- [ ] add lint configuration
- [ ] add test workflow
- [ ] add structured logging
- [ ] add metrics scaffolding
- [ ] add local development scripts

---

## 2. Core Domain Model

- [ ] define cluster roles
- [ ] define node roles
- [ ] define `ClusterSpec`
- [ ] define `ClusterStatus`
- [ ] define `MemberSpec`
- [ ] define `MemberStatus`
- [ ] define epoch / generation model
- [ ] define failover state machine
- [ ] define switchover state machine
- [ ] define rejoin flow model
- [ ] define operation history model
- [ ] define maintenance mode model

---

## 3. Configuration System

- [ ] define bootstrap node configuration format
- [ ] define config loader
- [ ] define config validation
- [ ] define defaults
- [ ] define TLS configuration section
- [ ] define PostgreSQL local config section
- [ ] define cluster bootstrap config section
- [ ] reject unsafe local-only overrides for cluster truth

---

## 4. Local Agent

- [ ] implement daemon startup
- [ ] implement local heartbeat loop
- [ ] detect PostgreSQL availability
- [ ] detect current PostgreSQL role
- [ ] detect recovery state
- [ ] collect system identifier
- [ ] collect timeline information
- [ ] collect receive / replay / flush LSN
- [ ] collect replication lag signals
- [ ] collect local process health
- [ ] publish observed state to control plane

---

## 5. PostgreSQL Integration Layer

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

## 6. Control Plane

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

## 7. Failover Engine

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

## 8. Switchover Engine

- [ ] define switchover validation rules
- [ ] validate target standby readiness
- [ ] implement planned topology transition
- [ ] coordinate demotion of current primary
- [ ] coordinate promotion of target standby
- [ ] publish new epoch
- [ ] record switchover result in history

---

## 9. Rejoin Flow

- [ ] detect former primary state
- [ ] detect divergence requirements
- [ ] decide rewind vs reclone path
- [ ] run `pg_rewind` workflow
- [ ] render standby configuration
- [ ] restart as standby
- [ ] verify replication health after rejoin
- [ ] mark node as healthy cluster member again

---

## 10. API

- [ ] expose cluster status endpoint
- [ ] expose node status endpoint
- [ ] expose member list endpoint
- [ ] expose operation history endpoint
- [ ] expose maintenance mode endpoint
- [ ] expose diagnostics endpoint
- [ ] expose switchover control endpoint
- [ ] expose health endpoint

---

## 11. CLI (`pacmanctl`)

- [ ] implement `cluster status`
- [ ] implement `members list`
- [ ] implement `cluster switchover`
- [ ] implement `cluster maintenance enable`
- [ ] implement `cluster maintenance disable`
- [ ] implement `history list`
- [ ] implement `cluster spec show`
- [ ] implement `node status`
- [ ] implement diagnostics commands

---

## 12. Security

- [ ] add TLS for external endpoints
- [ ] add mTLS between cluster members
- [ ] implement certificate loading
- [ ] define admin authorization model
- [ ] add audit logging for topology changes
- [ ] secure sensitive config handling

---

## 13. Observability

- [ ] add Prometheus metrics
- [ ] add health endpoints
- [ ] add structured event log
- [ ] add diagnostics dump
- [ ] add trace points for failover / switchover / rejoin
- [ ] add useful debug logging for reconciliation

---

## 14. Packaging and Operations

- [ ] add systemd unit files
- [ ] add example configs
- [ ] add local lab environment
- [ ] add bootstrap scripts for test cluster
- [ ] add container image for lab/testing
- [ ] define local state directory layout
- [ ] define upgrade-safe persistent control-plane state path

---

## 15. Testing

### Unit Tests
- [ ] cluster domain model tests
- [ ] config validation tests
- [ ] state machine tests
- [ ] candidate ranking tests
- [ ] failover policy tests

### Integration Tests
- [ ] PostgreSQL role detection tests
- [ ] promote workflow tests
- [ ] standby configuration tests
- [ ] rejoin / rewind tests
- [ ] maintenance mode tests

### End-to-End Tests
- [ ] 3-node cluster bootstrap
- [ ] planned switchover scenario
- [ ] automatic failover scenario
- [ ] former primary rejoin scenario
- [ ] network partition scenario
- [ ] witness-assisted quorum scenario

---

## 16. Kubernetes-Native MVP

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
- [ ] daemon process
- [ ] PostgreSQL state detection
- [ ] local API
- [ ] lifecycle management

## Milestone 2 — Cluster View
- [ ] multi-node membership
- [ ] cluster status aggregation
- [ ] leader election
- [ ] source of truth model

## Milestone 3 — Planned Switchover
- [ ] desired state model
- [ ] promote / demote orchestration
- [ ] operation history

## Milestone 4 — Automatic Failover
- [ ] quorum-based failure confirmation
- [ ] candidate selection
- [ ] controlled promotion
- [ ] epoch transition

## Milestone 5 — Rejoin and Hardening
- [ ] rejoin workflow
- [ ] rewind integration
- [ ] maintenance mode
- [ ] reliability improvements
- [ ] end-to-end test stabilization

## Milestone 6 — Kubernetes Operator MVP
- [ ] `PostgresCluster` CRD and status model
- [ ] leader-elected controller
- [ ] `StatefulSet` plus `primary` and `replicas` Services
- [ ] failover and rejoin flow inside Kubernetes
- [ ] repeatable `kind` end-to-end coverage

---

## Nice-to-Have After MVP

- [ ] synchronous replication policies
- [ ] richer fencing backends
- [ ] dedicated witness mode improvements
- [ ] standby-cluster / DR support
- [ ] endpoint automation
- [ ] web UI

---

## MVP Definition of Done

The MVP can be considered complete when PACMAN can reliably demonstrate:

- [ ] bootstrap of a small PostgreSQL HA cluster
- [ ] visibility into current topology
- [ ] planned switchover
- [ ] automatic failover with quorum
- [ ] prevention of unsafe dual-primary behavior
- [ ] explicit rejoin of a former primary
- [ ] basic operator-facing CLI and API
- [ ] repeatable integration and end-to-end test coverage
- [ ] operator-managed Kubernetes deployment with `StatefulSet` and role-based Services
- [ ] repeatable Kubernetes failover test coverage in `kind`

# PACMAN Demo Guide

Step-by-step walkthrough: deploy a PostgreSQL HA cluster, run day-2 operations, and validate the system.

---

## Overview

**PACMAN** (Postgres Autonomous Cluster Manager) manages PostgreSQL high availability:
- Automatic failover with quorum-based decisions
- Safe planned switchover
- Distributed control plane — no single point of failure
- Two deployment modes: `pacmand` (process) and `pacman_agent` (PostgreSQL extension)

This guide covers the **Docker Compose lab** (fastest path to a running demo), then the **production Ansible** path.

---

## Part 1 — Local Lab (Docker Compose)

The lab spins up a two-node cluster (primary + replica) plus an etcd DCS node on your local machine in under five minutes.

### Prerequisites

| Tool | Version |
|------|---------|
| Docker | 24+ |
| GNU Make | 3.81+ |
| Go | 1.26.1 (for local builds) |

### Step 1 — Build the RPM

The lab uses an RPM image. Build it first:

```bash
make rpm RPM_VERSION=0.1.0 RPM_RELEASE=1
```

Output lands in `bin/rpm/`.

### Step 2 — Bootstrap the cluster

```bash
deploy/lab/scripts/bootstrap-cluster.sh
```

This script:
1. Builds a container image from the RPM
2. Creates three containers on an internal Docker network:
   - `pacman-dcs` — single-node etcd on port `2379`
   - `pacman-primary` — PostgreSQL + pacmand (DB `:5433`, API `:8081`)
   - `pacman-replica` — PostgreSQL + pacmand (DB `:5434`, API `:8082`)
3. Runs the Ansible roles inside the containers
4. Waits for health probes to pass

### Step 3 — Verify cluster health

```bash
# Check the primary node
curl http://localhost:8081/health
# {"status":"healthy","role":"primary"}

# Check the replica
curl http://localhost:8082/health
# {"status":"healthy","role":"replica"}

# Full cluster topology
curl http://localhost:8081/api/v1/cluster | jq .
```

### Step 4 — Inspect members

```bash
curl http://localhost:8081/api/v1/members | jq .members[]
```

Expected output shows one primary and one replica with their lag and timeline.

### Step 5 — View Prometheus metrics

```bash
curl -s http://localhost:8081/metrics | grep pacman_cluster
```

Key metrics:
- `pacman_cluster_phase` — current phase (`running`, `bootstrapping`, …)
- `pacman_cluster_members_observed` — members seen by the control plane
- `pacman_cluster_spec_members_desired` — expected member count

---

## Part 2 — Day-2 Operations

### Planned Switchover

Promote the replica to primary safely (old primary becomes standby).

```bash
# Via pacmanctl
pacmanctl -api-url http://localhost:8081 cluster switchover -candidate pacman-replica

# Via curl
curl -s -X POST http://localhost:8081/api/v1/operations/switchover \
  -H "Content-Type: application/json" \
  -d '{"candidate":"pacman-replica","reason":"demo-switchover","requestedBy":"demo"}' | jq .
```

Watch the topology flip:

```bash
watch -n1 "curl -s http://localhost:8081/api/v1/members | jq '.members[] | {member, role}'"
```

PACMAN enforces pre-conditions before accepting the request:
- Candidate must be a known, healthy member
- Replication lag must be within threshold
- No active maintenance window

### Failover (simulated failure)

Stop the primary container to simulate a crash, then trigger failover:

```bash
docker stop pacman-primary

# PACMAN detects the failure automatically (TTL expiry in DCS).
# To trigger immediately via the API:
curl -s -X POST http://localhost:8082/api/v1/operations/failover \
  -H "Content-Type: application/json" \
  -d '{"reason":"demo-failover","requestedBy":"demo"}' | jq .
```

PACMAN blocks this when the primary is still healthy (split-brain guard). The replica becomes primary only after the DCS lock expires or an explicit force is issued.

Restart the former primary as a new standby:

```bash
docker start pacman-primary
# pacmand detects it is no longer primary, wipes data dir, and re-joins as standby.
```

### Maintenance Mode

Pause all topology operations before planned maintenance (upgrades, config changes).

```bash
# Enable
curl -s -X PUT http://localhost:8081/api/v1/maintenance \
  -H "Content-Type: application/json" \
  -d '{"enabled":true,"reason":"planned-upgrade"}' | jq .

# Any switchover attempted during maintenance returns 412:
curl -s -X POST http://localhost:8081/api/v1/operations/switchover \
  -H "Content-Type: application/json" \
  -d '{"candidate":"pacman-replica"}' | jq .
# {"error":"cluster is in maintenance mode"}

# Disable
curl -s -X PUT http://localhost:8081/api/v1/maintenance \
  -H "Content-Type: application/json" \
  -d '{"enabled":false}' | jq .
```

### Operation History

```bash
curl -s http://localhost:8081/api/v1/history | jq .
```

Every switchover, failover, and rejoin is recorded with timestamps, who requested it, and the outcome.

---

## Part 3 — Production Deployment (Ansible)

### Topology

```
┌─────────────────────────────────────────────────────────────┐
│  DCS Layer                                                  │
│  etcd-1  etcd-2  etcd-3   (3-node etcd, any network reach) │
└──────────────────────┬──────────────────────────────────────┘
                       │ http/https :2379
┌──────────────────────┴──────────────────────────────────────┐
│  Cluster Layer                                              │
│  alpha-1 (primary)   alpha-2 (standby)   alpha-3 (standby) │
│  pacmand :8080       pacmand :8080        pacmand :8080     │
│  postgres :5432      postgres :5432       postgres :5432    │
└─────────────────────────────────────────────────────────────┘
```

### Step 1 — Prepare inventory

Copy the example inventory:

```bash
cp -r deploy/ansible/examples/etcd-ha deploy/ansible/examples/my-cluster
```

Edit `deploy/ansible/examples/my-cluster/hosts.ini`:

```ini
[pacman_nodes]
alpha-1 ansible_host=10.0.0.11
alpha-2 ansible_host=10.0.0.12
alpha-3 ansible_host=10.0.0.13

[etcd_nodes]
etcd-1 ansible_host=10.0.0.21
etcd-2 ansible_host=10.0.0.22
etcd-3 ansible_host=10.0.0.23

[pacman_nodes:vars]
pacman_initial_primary=alpha-1
pacman_cluster_name=alpha
```

### Step 2 — Configure group variables

Edit `deploy/ansible/examples/my-cluster/group_vars/all.yml`:

```yaml
pacman_version: "0.1.0"
pacman_rpm_url: "https://packages.pacman.io/rpm/el/9/x86_64/pacman-0.1.0-1.el9.x86_64.rpm"

etcd_cluster_name: "etcd-alpha"
etcd_nodes:
  - { name: etcd-1, ip: 10.0.0.21 }
  - { name: etcd-2, ip: 10.0.0.22 }
  - { name: etcd-3, ip: 10.0.0.23 }

postgres_version: 17
postgres_data_dir: /var/lib/postgresql/17/main
```

For TLS and token overrides, copy and fill:

```bash
cp deploy/ansible/examples/security-overrides.yml.example \
   deploy/ansible/examples/my-cluster/security.yml
```

### Step 3 — Run the playbook

```bash
cd deploy/ansible
ansible-playbook -i examples/my-cluster/hosts.ini site.yml
```

The playbook:
1. Installs etcd on `etcd_nodes`, starts a quorum cluster
2. Installs PostgreSQL 17 + PACMAN RPM on `pacman_nodes`
3. Bootstraps `alpha-1` as primary (runs `initdb`, starts postgres)
4. Streams basebackup to `alpha-2` and `alpha-3` via `pg_basebackup`
5. Writes per-node `pacmand.yaml` configs
6. Starts and enables `pacmand.service` on all nodes
7. Waits for `/health` to pass on each node

### Step 4 — Validate deployment

```bash
# Check cluster from any node
pacmanctl -api-url http://10.0.0.11:8080 cluster status

# Expected output:
# CLUSTER   MEMBERS   PHASE
# alpha     3         running
#
# MEMBER    ROLE      LAG    STATE
# alpha-1   primary   0B     healthy
# alpha-2   standby   0B     healthy
# alpha-3   standby   0B     healthy
```

### Step 5 — Retrieve admin token

The RPM post-install hook generated a token on first install:

```bash
ssh 10.0.0.11 cat /etc/pacman/admin-token
```

Use it for authenticated requests:

```bash
pacmanctl -api-url http://10.0.0.11:8080 -api-token $(cat admin-token) cluster switchover -candidate alpha-2
```

Or set environment variables:

```bash
export PACMAN_API_URL=http://10.0.0.11:8080
export PACMAN_API_TOKEN=$(cat admin-token)
pacmanctl cluster switchover -candidate alpha-2
```

---

## Part 4 — Configuration Reference

### Minimal etcd-backed node config

```yaml
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
  apiAddress: 0.0.0.0:8080
postgres:
  dataDir: /var/lib/postgresql/17/main
  listenAddress: 127.0.0.1
  port: 5432
dcs:
  backend: etcd
  clusterName: alpha
  etcd:
    endpoints:
      - http://etcd-1:2379
      - http://etcd-2:2379
      - http://etcd-3:2379
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-1
  expectedMembers:
    - alpha-1
    - alpha-2
    - alpha-3
```

### Embedded Raft (no external DCS)

```yaml
dcs:
  backend: raft
  clusterName: alpha
  raft:
    dataDir: /var/lib/pacman/raft
    bindAddress: 10.0.0.11:7100
    peers:
      - 10.0.0.11:7100
      - 10.0.0.12:7100
      - 10.0.0.13:7100
```

### Common tuning knobs

```yaml
dcs:
  ttl: 30s             # DCS lock TTL — lower = faster failover, higher = more stable
  retryTimeout: 10s    # DCS retry window

postgres:
  parameters:
    max_replication_slots: "16"
    max_wal_senders: "16"
    wal_keep_size: "2048MB"
    synchronous_commit: "remote_apply"   # strongest durability
```

---

## Part 5 — Pacmand Startup Validation

pacmand validates the config document at startup and exits non-zero with a descriptive message on any error.

| Bad config | Exit message |
|---|---|
| `dcs.backend: postgres` (unsupported) | `dcs: backend is invalid` |
| `dcs.backend: etcd` with no endpoints | `dcs: etcd endpoints must not be empty` |
| Endpoint without URL scheme (`etcd:2379`) | `dcs: etcd endpoint … missing scheme` |
| Missing `bootstrap.clusterName` | `validate config document` |

Config validation currently happens during normal startup; PACMAN does not yet
provide a standalone `--validate-only` mode:

```bash
pacmand -config /etc/pacman/pacmand.yaml
# startup succeeds → config decoded and validated
# startup fails early → config error printed to stderr
```

---

## Part 6 — Monitoring Integration

### Prometheus scrape config

```yaml
scrape_configs:
  - job_name: pacman
    static_configs:
      - targets:
          - alpha-1:8080
          - alpha-2:8080
          - alpha-3:8080
    metrics_path: /metrics
```

### Key metrics

| Metric | Description |
|--------|-------------|
| `pacman_cluster_phase` | Current phase (0=bootstrapping, 1=running, 2=degraded) |
| `pacman_cluster_members_observed` | Members seen in DCS |
| `pacman_cluster_spec_members_desired` | Expected members from cluster spec |
| `pacman_node_role` | Node role (primary/standby) |
| `pacman_replication_lag_bytes` | Replication lag per standby |
| `pacman_dcs_last_write_seconds` | Time since last successful DCS write |

### HAProxy health check endpoints

```
backend postgres_primary
    option httpchk GET /primary
    server alpha-1 10.0.0.11:8080 check port 8080

backend postgres_replicas
    option httpchk GET /replica
    server alpha-2 10.0.0.12:8080 check port 8080
    server alpha-3 10.0.0.13:8080 check port 8080
```

Patroni-compatible probes are also available at `/leader`, `/standby-leader`, `/read-only`, `/synchronous`.

---

## Part 7 — Tear Down

### Lab

```bash
deploy/lab/scripts/destroy-cluster.sh    # stop containers, keep state
deploy/lab/scripts/reset-state.sh        # wipe all state
```

### Production (Ansible)

```bash
ansible-playbook -i examples/my-cluster/hosts.ini site.yml --tags uninstall
```

---

## Quick Reference

```bash
# Cluster status
pacmanctl cluster status

# Planned switchover
pacmanctl cluster switchover -candidate alpha-2

# Enable maintenance
pacmanctl cluster maintenance enable

# Operation history
pacmanctl history list

# Raw API — cluster topology
curl http://localhost:8080/api/v1/cluster | jq .

# Raw API — members
curl http://localhost:8080/api/v1/members | jq .

# Raw API — diagnostics
curl http://localhost:8080/api/v1/diagnostics | jq .

# Prometheus metrics
curl -s http://localhost:8080/metrics | grep pacman_

# Patroni-compatible status (for drop-in compatibility)
curl http://localhost:8080/patroni | jq .
```

---

## Further Reading

- [Architecture overview](ARCHITECTURE.md)
- [DCS backend design](ARCHITECTURE_DCS.md)
- [Kubernetes deployment model](ARCHITECTURE_K8S.md)
- [PostgreSQL extension mode](POSTGRES_EXTENSION.md)
- [API contract](api-contract.md)
- [OpenAPI spec](openapi.yaml)
- [Ansible deployment](../deploy/ansible/README.md)
- [Docker Compose lab](../deploy/lab/README.md)
- [RPM packaging](../packaging/rpm/README.md)

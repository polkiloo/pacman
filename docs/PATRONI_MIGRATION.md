# Patroni Migration Guide

This document covers migrating from Patroni to PACMAN for clusters that match the upstream
Patroni reference topology (`postgres0.yml`, `postgres1.yml`, `postgres2.yml`).

---

## Supported Migration Scope

PACMAN's migration path targets the **minimum viable Patroni config subset** present in the
three upstream example configs. Fields outside this subset either have no PACMAN equivalent
or require explicit attention before migration.

### Patroni reference topology

The three upstream files describe a single etcd-backed cluster of one coordinator and two
standbys, all on localhost:

| File | Patroni node | Role | REST port |
|------|-------------|------|-----------|
| `postgres0.yml` | `postgresql0` | coordinator (primary) | 8008 |
| `postgres1.yml` | `postgresql1` | standby | 8009 |
| `postgres2.yml` | `postgresql2` | standby (with REST auth) | 8010 |

All three share:
- `scope: batman` — single etcd cluster, single logical cluster name
- `etcd.host: 127.0.0.1:2379` — single etcd endpoint
- Identical `bootstrap.dcs` block — DCS parameters written once, read by all nodes

---

## Minimum Supported Patroni Config Subset

The fields listed below are **translatable** to PACMAN configuration. Everything else is
either managed internally by PACMAN, not applicable to its architecture, or requires an
explicit migration warning (see [Unsupported Fields](#unsupported-fields)).

### Top-level identity

| Patroni field | Type | Notes |
|---------------|------|-------|
| `scope` | string | Logical cluster name shared across all nodes |
| `name` | string | Unique node name within the cluster |

### REST API endpoint

| Patroni field | Type | Notes |
|---------------|------|-------|
| `restapi.listen` | `host:port` | Listen address for the HTTP API |
| `restapi.connect_address` | `host:port` | Externally reachable address — see mapping notes |

### DCS (etcd backend only)

| Patroni field | Type | Notes |
|---------------|------|-------|
| `etcd.host` | `host:port` | Single etcd endpoint (no scheme in Patroni) |
| `etcd.hosts` | list of `host:port` | Multi-endpoint form |
| `etcd.username` | string | etcd username |
| `etcd.password` | string | etcd password |

### DCS bootstrap parameters

| Patroni field | Type | Notes |
|---------------|------|-------|
| `bootstrap.dcs.ttl` | integer (seconds) | DCS lock TTL |
| `bootstrap.dcs.retry_timeout` | integer (seconds) | DCS retry window |

### PostgreSQL local settings

| Patroni field | Type | Notes |
|---------------|------|-------|
| `postgresql.data_dir` | path | PostgreSQL data directory |
| `postgresql.bin_dir` | path | PostgreSQL binary directory |
| `postgresql.listen` | `host:port` | PostgreSQL listen address and port |
| `postgresql.connect_address` | `host:port` | External PostgreSQL address — see notes |
| `postgresql.parameters` | map | `postgresql.conf` parameter overrides |

---

## Field-by-Field Mapping

### Identity

| Patroni key | PACMAN key | Notes |
|-------------|-----------|-------|
| `scope` | `dcs.clusterName` and `bootstrap.clusterName` | Write the same value to both PACMAN fields |
| `name` | `node.name` | Direct 1:1 |

### REST / HTTP API

| Patroni key | PACMAN key | Notes |
|-------------|-----------|-------|
| `restapi.listen` | `node.apiAddress` | Same `host:port` format |
| `restapi.connect_address` | — | Not required. PACMAN does not distinguish listen vs. connect address for its API. Handled at the load-balancer or HAProxy layer. |
| `restapi.authentication.username` / `.password` | `security.adminBearerTokenFile` or `security.adminBearerToken` | Patroni uses HTTP Basic Auth for its REST API; PACMAN uses a bearer token. Generate a token and either write it to `adminBearerTokenFile` or inject it as `adminBearerToken`. |

### DCS — etcd backend

| Patroni key | PACMAN key | Notes |
|-------------|-----------|-------|
| `etcd.host` | `dcs.etcd.endpoints[0]` | Patroni omits the URL scheme; PACMAN requires `http://` or `https://`. Prepend `http://` unless TLS is in use. |
| `etcd.hosts` | `dcs.etcd.endpoints` | Same transformation — prepend scheme to each entry. |
| `etcd.username` | `dcs.etcd.username` | Direct 1:1 |
| `etcd.password` | `dcs.etcd.password` | Direct 1:1. PACMAN currently supports the password as an inline config value only; render it from your secret management system before start. |
| `etcd3.*` | `dcs.etcd.*` | PACMAN's etcd client speaks the v3 protocol natively; no separate `etcd3` block. |
| `raft.*` (Patroni embedded) | `dcs.raft.*` | Different wire protocol and config shape. See [Embedded Raft](#embedded-raft-backend) below. |

### DCS bootstrap parameters

| Patroni key | PACMAN key | Notes |
|-------------|-----------|-------|
| `bootstrap.dcs.ttl` | `dcs.ttl` | Patroni value is in seconds (integer); PACMAN uses Go duration string. Example: `30` → `"30s"`. |
| `bootstrap.dcs.retry_timeout` | `dcs.retryTimeout` | Same unit conversion as `ttl`. |
| `bootstrap.dcs.loop_wait` | — | Internal Patroni HA loop cadence. No equivalent; PACMAN uses its own internal reconciliation interval. |
| `bootstrap.dcs.maximum_lag_on_failover` | — | Not yet a configurable PACMAN field. PACMAN enforces replication lag checks internally; see [Unsupported Fields](#unsupported-fields). |

### PostgreSQL local settings

| Patroni key | PACMAN key | Notes |
|-------------|-----------|-------|
| `postgresql.data_dir` | `postgres.dataDir` | Direct 1:1 |
| `postgresql.bin_dir` | `postgres.binDir` | Direct 1:1 |
| `postgresql.listen` | `postgres.listenAddress` + `postgres.port` | Patroni uses a single `host:port` string; split into two PACMAN fields. Example: `127.0.0.1:5432` → `listenAddress: 127.0.0.1`, `port: 5432`. |
| `postgresql.connect_address` | — | Not required. Route client traffic through HAProxy or another load balancer using PACMAN's `/primary` and `/replica` health probes. |
| `postgresql.parameters` | `postgres.parameters` | Direct map copy. All values must be quoted strings in PACMAN YAML. |

### Bootstrap and cluster membership

| Patroni key | PACMAN key | Notes |
|-------------|-----------|-------|
| (no equivalent) | `node.controlAddress` | Required for PACMAN peer/control-plane traffic. Choose a unique `host:port` on every node. |
| (no equivalent) | `bootstrap.initialPrimary` | Set to the `name` value of the node that will bootstrap as primary — equivalent to the first node listed in Patroni's DCS. |
| (no equivalent) | `bootstrap.expectedMembers` | List all `name` values for the cluster. PACMAN uses this to wait for full quorum before marking the cluster healthy. |
| (no equivalent) | `bootstrap.seedAddresses` | List the `controlAddress` values of all nodes. PACMAN validates a non-empty seed list after defaults; even on etcd-backed clusters, explicit values are recommended so the config does not rely on loader defaults. |

### Security and TLS

| Patroni key | PACMAN key | Notes |
|-------------|-----------|-------|
| `restapi.cafile` / `certfile` / `keyfile` | `tls.caFile` / `tls.certFile` / `tls.keyFile` | Direct path mapping |
| `restapi.verify_client` | `security.memberMTLSEnabled` | Patroni's `verify_client` enforces client cert checks; PACMAN's `memberMTLSEnabled` does the same for peer traffic. |
| `etcd.cacert` / `cert` / `key` | `tls.caFile` / `tls.certFile` / `tls.keyFile` | PACMAN uses a single TLS block for both API and DCS traffic. |

---

## Unsupported Fields

The following Patroni fields have no PACMAN equivalent in the current release. Attempting to
translate them silently would produce a misleading config. Each one must be addressed
explicitly before migration is complete.

| Patroni field | Reason | Migration action |
|---------------|--------|-----------------|
| `bootstrap.dcs.maximum_lag_on_failover` | Not yet a configurable threshold | Remove. PACMAN enforces a built-in lag check; configurable threshold is on the roadmap. |
| `bootstrap.dcs.postgresql.use_pg_rewind` | No direct local-config translation | Remove the Patroni key from node config. PACMAN exposes `pg_rewind` as cluster rejoin policy rather than a `bootstrap.dcs` field, so verify the migrated cluster policy matches your rewind expectations. |
| `bootstrap.dcs.postgresql.pg_hba` | PACMAN does not manage `pg_hba.conf` entries | Manage `pg_hba.conf` externally (Ansible template, config management). |
| `bootstrap.initdb` | PACMAN manages `initdb` automatically on first boot | Remove the block. Set locale/encoding at the OS level before first start. |
| `postgresql.pgpass` | PACMAN manages replication credentials internally | Remove. No `.pgpass` file is needed. |
| `postgresql.authentication.*` | PostgreSQL user management is external to PACMAN | Create the replication and superuser accounts before starting PACMAN. |
| `postgresql.basebackup` | No direct PACMAN node-config equivalent | Remove it from the PACMAN node config. Replica cloning and full reclone workflows are handled by deployment automation / operational tooling, not by translating Patroni basebackup flags 1:1. |
| `tags.noloadbalance` | PACMAN does not parse Patroni tags | Configure load-balancer exclusion at the HAProxy/PgBouncer layer instead. |
| `tags.clonefrom` | No equivalent | Remove. |
| `tags.nostream` | No equivalent | Remove. |
| `bootstrap.dcs.loop_wait` | Internal Patroni reconcile cadence | Remove. |

---

## Embedded Raft Backend

The Patroni `raft` DCS backend uses a different embedded Raft topology than PACMAN. There is
no direct field-level translation. If migrating from Patroni Raft, use PACMAN's embedded Raft
config and re-bootstrap the DCS from scratch:

```yaml
dcs:
  backend: raft
  clusterName: alpha
  ttl: 30s
  retryTimeout: 10s
  raft:
    dataDir: /var/lib/pacman/raft
    bindAddress: 10.0.0.11:7100
    peers:
      - 10.0.0.11:7100
      - 10.0.0.12:7100
      - 10.0.0.13:7100
```

For clusters already running Patroni with an etcd backend, prefer migrating to PACMAN with
the same etcd backend — no DCS re-bootstrap is needed.

---

## Migration Checklists

### Pre-migration checks

- [ ] All nodes are healthy in Patroni (`patronictl list` shows all members)
- [ ] etcd cluster is healthy (`etcdctl endpoint health`)
- [ ] Replication lag is zero on all standbys
- [ ] No pending Patroni operations (switchover, reinitialize)
- [ ] If translating Patroni `restapi.authentication.*`, generate a PACMAN bearer token and store it at `/etc/pacman/admin-token`
- [ ] `pg_hba.conf` allows replication between all nodes (not managed by PACMAN)
- [ ] Replication user exists in PostgreSQL (not managed by PACMAN)
- [ ] Unique PACMAN `node.controlAddress` values chosen for all nodes

### Per-node config translation

- [ ] `scope` → `dcs.clusterName` and `bootstrap.clusterName`
- [ ] `name` → `node.name`
- [ ] `restapi.listen` → `node.apiAddress`
- [ ] `etcd.host` → `dcs.etcd.endpoints[0]` (add `http://` or `https://` scheme)
- [ ] `bootstrap.dcs.ttl` → `dcs.ttl` (append `s`, e.g. `30` → `"30s"`)
- [ ] `bootstrap.dcs.retry_timeout` → `dcs.retryTimeout` (same conversion)
- [ ] `postgresql.data_dir` → `postgres.dataDir`
- [ ] `postgresql.bin_dir` → `postgres.binDir`
- [ ] `postgresql.listen` → `postgres.listenAddress` + `postgres.port` (split `host:port`)
- [ ] `postgresql.parameters` → `postgres.parameters` (quote all values)
- [ ] Set `bootstrap.initialPrimary` to the name of the current Patroni leader
- [ ] Set `bootstrap.expectedMembers` to all node names in the cluster
- [ ] Remove all unsupported fields (see table above)

### Post-migration validation

- [ ] `pacmand -config /etc/pacman/pacmand.yaml` starts successfully on all nodes (PACMAN does not currently provide a standalone `--validate-only` flag)
- [ ] `curl http://<node>:8080/health` returns `200` on all nodes
- [ ] `pacmanctl cluster status` shows all members with correct roles
- [ ] Replication lag is zero (`pacmanctl members list`)
- [ ] Planned switchover succeeds (`pacmanctl cluster switchover -candidate <name>`)
- [ ] Prometheus metrics endpoint returns cluster metrics (`curl .../metrics | grep pacman_cluster`)

---

## Reference Examples

See [docs/examples/](examples/) for PACMAN configs that mirror the Patroni
`postgres0/1/2.yml` topology:

- [`pacman-compat-node0.yaml`](examples/pacman-compat-node0.yaml) — primary node (mirrors `postgres0.yml`, no REST auth)
- [`pacman-compat-node1.yaml`](examples/pacman-compat-node1.yaml) — first standby (mirrors `postgres1.yml`, no REST auth)
- [`pacman-compat-node2.yaml`](examples/pacman-compat-node2.yaml) — second standby (mirrors `postgres2.yml`, bearer-token replacement for Patroni REST auth)

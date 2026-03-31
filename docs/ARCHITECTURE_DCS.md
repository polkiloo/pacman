
# PACMAN DCS Architecture

**Pluggable Distributed Configuration Store for PACMAN**

This document describes how PACMAN separates its high-level HA orchestration logic from the low-level distributed storage backend, allowing operators to choose the DCS that fits their infrastructure.

---

## Design Principle

The current `MemoryStateStore` conflates two fundamentally different concerns:

1. **Storage primitives** — key-value get/set, compare-and-swap, leader election, TTL-based sessions, watches. These vary per backend.

2. **HA orchestration** — failover state machine, switchover planning, rejoin assessment, candidate ranking, member aggregation, reconciliation. These are PACMAN's core intelligence and must remain identical regardless of which DCS is running underneath.

The correct architectural split is a **thin DCS interface** that abstracts storage primitives, and a **ControlPlane struct** that implements all HA logic on top of any DCS. This is the same pattern that made Patroni successful — Patroni's `AbstractDCS` is ~15 methods, not hundreds.

---

## Supported Backends (MVP)

| Backend | Deployment Model | External Dependency | Best For |
|---|---|---|---|
| **Embedded Raft** | Self-contained in `pacmand` | None | Bare-metal, simple ops, no external infra |
| **etcd** | External etcd cluster | etcd v3 | Teams already running etcd / Kubernetes |

The DCS interface is designed so that additional backends (ZooKeeper, Consul, Kubernetes Lease/ConfigMap) can be added post-MVP without changing the ControlPlane or any HA logic.

---

## Package Layout

```
internal/
├── dcs/                          # DCS abstraction layer
│   ├── dcs.go                    # DCS interface + types
│   ├── config.go                 # DCS configuration model
│   ├── errors.go                 # DCS error types
│   ├── dcstest/                  # Shared conformance test suite
│   │   └── conformance.go
│   ├── memory/                   # In-memory backend (testing)
│   │   └── memory.go
│   ├── raft/                     # Embedded Raft backend (hashicorp/raft)
│   │   ├── raft.go               # DCS implementation
│   │   ├── fsm.go                # Raft finite state machine
│   │   ├── transport.go          # TCP transport + TLS
│   │   ├── snapshot.go           # Snapshot handling
│   │   └── config.go             # Raft-specific config
│   └── etcd/                     # etcd v3 backend
│       ├── etcd.go
│       └── config.go
├── controlplane/                 # HA orchestration (unchanged logic)
│   ├── controlplane.go           # ControlPlane struct wrapping DCS
│   ├── source_of_truth.go        # High-level interfaces (preserved)
│   ├── failover_*.go             # Failover logic (unchanged)
│   ├── switchover_*.go           # Switchover logic (unchanged)
│   ├── rejoin_*.go               # Rejoin logic (unchanged)
│   └── ...
```

---

## DCS Interface

The DCS interface defines the **minimal storage contract** that every backend must satisfy. It intentionally avoids any HA-specific concepts — no failover, no switchover, no PostgreSQL awareness. Just distributed state primitives.

```go
package dcs

import (
    "context"
    "time"
)

// DCS is the distributed configuration store contract.
// Every backend (etcd, Raft, etc.) implements this.
type DCS interface {
    // Identity returns the DCS backend name (e.g., "raft", "etcd").
    Identity() string

    // --- Key-Value Operations ---

    // Get reads a key from the store. Returns ErrKeyNotFound if absent.
    Get(ctx context.Context, key string) (KeyValue, error)

    // Set writes a key unconditionally.
    Set(ctx context.Context, key string, value []byte, opts ...SetOption) error

    // CompareAndSet writes a key only if the current revision matches.
    // Returns ErrRevisionMismatch on conflict. This is the fundamental
    // building block for safe concurrent updates.
    CompareAndSet(ctx context.Context, key string, value []byte, expectedRevision int64) error

    // Delete removes a key. Returns ErrKeyNotFound if absent.
    Delete(ctx context.Context, key string) error

    // List returns all keys under a prefix, sorted lexicographically.
    List(ctx context.Context, prefix string) ([]KeyValue, error)

    // --- Leader Election ---

    // Campaign attempts to acquire or renew the leader lease for the given
    // node. Returns the current lease state and whether this node holds it.
    //
    // Semantics:
    //   - If no leader exists or the lease has expired, the candidate wins.
    //   - If the candidate already holds the lease, it is renewed.
    //   - If another node holds an active lease, the candidate loses.
    //
    // Implementations must guarantee that at most one node holds the lease
    // at any point in time within the configured TTL.
    Campaign(ctx context.Context, candidate string) (LeaderLease, bool, error)

    // Leader returns the currently active leader. Returns false if no leader.
    Leader(ctx context.Context) (LeaderLease, bool, error)

    // Resign voluntarily releases the leader lease held by this node.
    Resign(ctx context.Context) error

    // --- Session / Liveness ---

    // Touch refreshes the TTL for this node's session key.
    // Other nodes can detect a peer's liveness by checking if its session
    // key exists and is not expired. If the node crashes without renewing,
    // the backend's native TTL/session mechanism will expire the key.
    Touch(ctx context.Context, member string) error

    // Alive returns true if the given member's session key is still live.
    Alive(ctx context.Context, member string) (bool, error)

    // --- Watch ---

    // Watch observes changes under a key prefix. The returned channel
    // emits events until the context is cancelled. Backends may coalesce
    // rapid changes. The initial state is NOT replayed — use List first
    // if you need the current snapshot.
    Watch(ctx context.Context, prefix string) (<-chan WatchEvent, error)

    // --- Lifecycle ---

    // Initialize performs one-time backend setup (create Raft cluster,
    // etc.). Idempotent.
    Initialize(ctx context.Context) error

    // Close releases backend resources.
    Close() error
}
```

### Supporting Types

```go
// KeyValue represents a versioned key-value pair.
type KeyValue struct {
    Key      string
    Value    []byte
    Revision int64    // Monotonically increasing version per key.
    TTL      time.Duration
}

// LeaderLease describes the current leadership state in the DCS.
type LeaderLease struct {
    Leader    string    // Node name of the current leader.
    Term      uint64    // Monotonically increasing election term.
    Acquired  time.Time
    Renewed   time.Time
    ExpiresAt time.Time
}

// WatchEvent represents a change observed by Watch.
type WatchEvent struct {
    Type     EventType // EventPut, EventDelete, EventExpired
    Key      string
    Value    []byte
    Revision int64
}

type EventType int

const (
    EventPut EventType = iota
    EventDelete
    EventExpired
)

// SetOption configures a Set operation.
type SetOption func(*setOptions)

type setOptions struct {
    TTL time.Duration
}

// WithTTL attaches a time-to-live to the key.
func WithTTL(d time.Duration) SetOption {
    return func(o *setOptions) { o.TTL = d }
}
```

### Error Types

```go
var (
    ErrKeyNotFound        = errors.New("dcs: key not found")
    ErrRevisionMismatch   = errors.New("dcs: revision mismatch")
    ErrNotLeader          = errors.New("dcs: not the leader")
    ErrNoLeader           = errors.New("dcs: no leader elected")
    ErrSessionExpired     = errors.New("dcs: session expired")
    ErrBackendUnavailable = errors.New("dcs: backend unavailable")
)
```

---

## Key Space Layout

Every DCS backend stores PACMAN state under a scoped prefix:

```
/pacman/<cluster-name>/
├── config                     # ClusterSpec JSON (desired state)
├── leader                     # Leader lease (managed by Campaign/Leader)
├── members/
│   ├── <node-1>               # MemberRegistration JSON
│   ├── <node-2>
│   └── <node-3>
├── status/
│   ├── <node-1>               # NodeStatus JSON (heartbeat, TTL-based)
│   ├── <node-2>
│   └── <node-3>
├── operation                  # Active Operation JSON (if any)
├── history/
│   ├── <op-id-1>              # HistoryEntry JSON
│   ├── <op-id-2>
│   └── ...
├── maintenance                # MaintenanceModeStatus JSON
└── epoch                      # Current epoch counter
```

The node status keys use TTL so that if a node crashes, its status expires automatically — the control plane observes this as the member becoming unreachable.

---

## ControlPlane Refactoring

The current `MemoryStateStore` gets refactored into two pieces:

### Before (current)

```
MemoryStateStore implements:
  - NodeStatePublisher       (publishes heartbeats → in-memory map)
  - MemberRegistrar          (registers members → in-memory map)
  - MemberDiscovery          (reads members → in-memory map)
  - LeaderElector             (campaigns → in-memory lease)
  - DesiredStateStore         (stores spec → in-memory pointer)
  - ObservedStateStore        (reads status → in-memory pointer)
  - Reconciler                (aggregates → in-memory)
  - MaintenanceStore          (updates maint → in-memory)
  - OperationJournal          (journals ops → in-memory slice)
  - SwitchoverEngine          (plans + executes switchover)
  - RejoinEngine              (plans + executes rejoin)
  - FailoverEngine            (plans + executes failover)
  - SourceOfTruthStore        (reads snapshot)
```

### After (refactored)

```
DCS (thin interface) provides:
  - Get / Set / CompareAndSet / Delete / List
  - Campaign / Leader / Resign
  - Touch / Alive
  - Watch
  - Initialize / Close

ControlPlane (struct, wraps DCS) implements:
  - NodeStatePublisher        → DCS.Set("status/<node>", ..., WithTTL)
  - MemberRegistrar           → DCS.Set("members/<node>", ...)
  - MemberDiscovery           → DCS.List("members/") + DCS.List("status/")
  - LeaderElector              → DCS.Campaign() / DCS.Leader()
  - DesiredStateStore          → DCS.Get("config") / DCS.CompareAndSet("config", ...)
  - ObservedStateStore         → aggregation from DCS.List("status/")
  - Reconciler                 → in-process aggregation (reads from DCS)
  - MaintenanceStore           → DCS.Get("maintenance") / DCS.CompareAndSet(...)
  - OperationJournal           → DCS.Get("operation") + DCS.List("history/")
  - SwitchoverEngine           → pure logic + DCS writes
  - RejoinEngine               → pure logic + DCS writes
  - FailoverEngine             → pure logic + DCS writes
  - SourceOfTruthStore         → in-process snapshot cache
```

The critical insight is that **all HA logic stays in the `controlplane` package**. The DCS just stores and retrieves bytes. The ControlPlane serializes domain types to/from JSON and uses CompareAndSet for safe concurrent updates.

---

## ControlPlane Struct

```go
package controlplane

import "github.com/polkiloo/pacman/internal/dcs"

// ControlPlane implements all high-level HA interfaces on top of any DCS backend.
type ControlPlane struct {
    dcs         dcs.DCS
    clusterName string
    nodeName    string
    codec       StateCodec       // JSON marshal/unmarshal
    cache       localCache        // in-process snapshot for fast reads
    logger      *slog.Logger
}

// NewControlPlane constructs the HA orchestration layer over the given DCS.
func NewControlPlane(store dcs.DCS, clusterName, nodeName string, logger *slog.Logger) *ControlPlane {
    return &ControlPlane{
        dcs:         store,
        clusterName: clusterName,
        nodeName:    nodeName,
        codec:       JSONCodec{},
        logger:      logger,
    }
}
```

The `ControlPlane` struct still implements the exact same interfaces (`ReplicatedStateStore`), so the agent daemon and future API handlers don't change at all. The only difference is that reads/writes flow through the DCS instead of in-memory maps.

---

## Backend-Specific Design Notes

### Embedded Raft (`internal/dcs/raft/`)

The recommended default backend. Uses **`hashicorp/raft`** (`github.com/hashicorp/raft`) for consensus, with **`raft-boltdb/v2`** (`github.com/hashicorp/raft-boltdb/v2`) for durable log storage.

**Why `hashicorp/raft`:**

- Full-stack library: manages Raft log replication, leader election, snapshots, transport, and membership changes — minimal custom code required.
- Battle-tested in production at HashiCorp Vault (Integrated Storage), Consul, and Nomad.
- Clean Go API with pluggable storage backends (BoltDB for logs, file-based snapshots).
- Built-in membership changes: `AddVoter`, `RemoveServer`, `DemoteVoter`, `AddNonvoter`.
- MPL 2.0 license — compatible with PACMAN's use case.
- Actively maintained (latest release March 2025, imported by 2,289+ Go projects).

**Alternatives considered:**

- `etcd-io/raft` — higher performance but requires implementing transport, log store, and snapshot store from scratch. Too much custom code for MVP.
- `lni/dragonboat` — outstanding throughput (9M writes/sec) but smaller community and less production validation. Better suited as a post-MVP optimization if performance bottlenecks emerge.

```
┌──────────────────────────────────────────────────┐
│                    pacmand                        │
│                                                   │
│  ┌─────────────┐    ┌─────────────────────────┐  │
│  │ Agent       │    │ Raft DCS                 │  │
│  │ (heartbeat) │    │                          │  │
│  │             │───>│  hashicorp/raft.Raft     │  │
│  │             │    │  ├── FSM (state machine) │  │
│  │             │    │  ├── LogStore (BoltDB)   │  │
│  │             │    │  ├── SnapshotStore       │  │
│  │             │    │  └── TCP Transport       │  │
│  └─────────────┘    └─────────────────────────┘  │
│         │                      │                  │
│         v                      v                  │
│  ┌─────────────┐    ┌─────────────────────────┐  │
│  │ PostgreSQL  │    │ Peer Raft nodes          │  │
│  └─────────────┘    └─────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

**FSM design:** The Raft FSM holds a `map[string]keyEntry` (flat key-value state). Every write (Set, CompareAndSet, Delete) is a Raft log entry applied through the FSM. Reads can be served from the local FSM state after a `raft.VerifyLeader()` check (linearizable reads) or directly from the local FSM (stale reads, acceptable for heartbeats).

**Leader election:** Raft's built-in leader election maps directly to `Campaign()`/`Leader()`. The Raft leader is the PACMAN control-plane leader. This avoids a separate leader election mechanism.

**Session/TTL:** The FSM tracks TTL per key. A background goroutine periodically scans for expired keys and applies delete commands through Raft. This keeps TTL expiration consistent across all nodes.

**Membership changes:** `hashicorp/raft` has built-in `AddVoter`/`RemoveServer`. The DCS `Initialize()` method bootstraps the initial Raft cluster. New nodes join via `AddVoter`.

**Patroni Raft lessons applied:**

Patroni's PySyncObj Raft mode (deprecated, never left beta) failed due to: NAT incompatibility, single-port limitation, and DNS resolution happening only once at object creation. PACMAN's embedded Raft avoids all three: explicit peer addresses from config, TLS transport support via `hashicorp/raft`'s `StreamLayer`, and address changes handled through Raft membership operations (`AddVoter`/`RemoveServer`).

### etcd (`internal/dcs/etcd/`)

Uses the etcd v3 client API (`go.etcd.io/etcd/client/v3`).

- **Key-value:** Direct mapping to etcd `Put`/`Get`/`Delete`/`Txn`.
- **CompareAndSet:** Uses etcd transactions: `If(ModRevision == expected).Then(Put).Else(Get)`.
- **Leader election:** Uses `concurrency.Election` from the etcd client.
- **Session/TTL:** Uses etcd leases (`Grant` + `KeepAlive`).
- **Watch:** Direct mapping to etcd `Watch`.

---

## Configuration Model

The node configuration gains a `dcs` section:

```yaml
apiVersion: pacman/v1
kind: NodeConfig
node:
  name: pacmand-1
  role: data
  apiAddress: "10.0.0.1:8080"
  controlAddress: "10.0.0.1:8081"

dcs:
  # Backend selection: raft | etcd
  backend: raft

  # Common settings
  clusterName: my-cluster
  ttl: 30s               # Session/key TTL
  retryTimeout: 10s      # DCS operation retry budget

  # Backend-specific configuration (only one block active)
  raft:
    dataDir: /var/lib/pacman/raft
    bindAddress: "10.0.0.1:8300"
    peers:
      - "10.0.0.1:8300"
      - "10.0.0.2:8300"
      - "10.0.0.3:8300"
    snapshotInterval: 120s
    snapshotThreshold: 8192
    trailingLogs: 10240

  etcd:
    endpoints:
      - "https://etcd-1:2379"
      - "https://etcd-2:2379"
      - "https://etcd-3:2379"
    username: ""
    password: ""

tls:
  # ... TLS config reused for DCS transport where applicable
```

---

## Migration Path

### Phase 1: Extract DCS Interface

1. Create `internal/dcs/dcs.go` with the `DCS` interface and types.
2. Create `internal/dcs/memory/memory.go` — a `DCS` implementation backed by in-memory maps. This is a direct extraction from `MemoryStateStore`'s storage logic, used for testing.
3. Refactor `ControlPlane` to accept `dcs.DCS` instead of being `MemoryStateStore`.
4. Existing tests keep working by passing the memory DCS.

**No behavior changes. All existing tests pass.**

### Phase 2: Implement Embedded Raft

1. Add `hashicorp/raft` + `raft-boltdb/v2` dependencies.
2. Implement `internal/dcs/raft/` — the `DCS` interface backed by Raft consensus.
3. Wire Raft bootstrap into `pacmand` startup when `dcs.backend: raft`.
4. Add integration tests with 3-node Raft cluster using testcontainers.

### Phase 3: Implement etcd Backend

1. Add `go.etcd.io/etcd/client/v3` dependency.
2. Implement `internal/dcs/etcd/` — the `DCS` interface backed by etcd v3.
3. Add integration tests with testcontainers-based etcd cluster.

---

## ControlPlane → DCS Mapping

How each current `ReplicatedStateStore` method maps to DCS calls:

| ControlPlane Method | DCS Operations |
|---|---|
| `RegisterMember(reg)` | `Set("members/<node>", marshal(reg))` |
| `RegisteredMember(name)` | `Get("members/<name>")` → unmarshal |
| `RegisteredMembers()` | `List("members/")` → unmarshal each |
| `PublishNodeStatus(status)` | `Set("status/<node>", marshal(status), WithTTL(ttl))` |
| `NodeStatus(name)` | `Get("status/<name>")` → unmarshal |
| `CampaignLeader(node)` | `Campaign(node)` |
| `Leader()` | `Leader()` |
| `ClusterSpec()` | `Get("config")` → unmarshal |
| `StoreClusterSpec(spec)` | `Get("config")` → `CompareAndSet("config", marshal(spec), rev)` |
| `ClusterStatus()` | In-process aggregation from cached member + status data |
| `Reconcile()` | `List("members/")` + `List("status/")` + `Get("config")` → aggregate |
| `MaintenanceStatus()` | `Get("maintenance")` → unmarshal |
| `UpdateMaintenanceMode(req)` | `CompareAndSet("maintenance", ..., rev)` |
| `ActiveOperation()` | `Get("operation")` → unmarshal |
| `History()` | `List("history/")` → unmarshal each |
| `JournalOperation(op)` | `CompareAndSet("operation", ...)` or `Set("history/<id>", ...)` |

---

## Read Path Optimization

For high-frequency reads (heartbeat loop calling `Members()`, `ClusterStatus()`), hitting the DCS on every call is too expensive. The ControlPlane maintains a **local read cache** refreshed by:

1. **Explicit refresh** on every `Reconcile()` call.
2. **Watch-driven invalidation** — a background goroutine watches the DCS prefix and updates the cache on changes.
3. **TTL-based staleness** — cached values are considered valid for a configurable window (default: 1 second). Reads within the window use the cache; expired reads trigger a DCS fetch.

For the embedded Raft backend, reads from the local FSM are already fast (no network hop), so the cache primarily benefits the etcd backend.

---

## Write Path Safety

All mutating operations use **CompareAndSet** to prevent lost updates:

```go
func (cp *ControlPlane) StoreClusterSpec(ctx context.Context, spec cluster.ClusterSpec) (cluster.ClusterSpec, error) {
    // 1. Read current from DCS
    kv, err := cp.dcs.Get(ctx, cp.key("config"))
    if err != nil && !errors.Is(err, dcs.ErrKeyNotFound) {
        return cluster.ClusterSpec{}, err
    }

    // 2. Merge / validate
    merged := cp.mergeClusterSpec(kv, spec)
    data, err := cp.codec.Marshal(merged)
    if err != nil {
        return cluster.ClusterSpec{}, err
    }

    // 3. Write with CAS
    if err := cp.dcs.CompareAndSet(ctx, cp.key("config"), data, kv.Revision); err != nil {
        return cluster.ClusterSpec{}, fmt.Errorf("store cluster spec: %w", err)
    }

    return merged, nil
}
```

For the Raft backend, CompareAndSet is implemented as a conditional Raft log entry — the FSM checks the revision before applying.

---

## Testing Strategy

| Layer | What | How |
|---|---|---|
| DCS interface | Contract compliance | Shared test suite (`dcs/dcstest`) run against every backend |
| Memory backend | Fast unit tests | Direct instantiation, no containers |
| Raft backend | Consensus correctness | 3-node testcontainers cluster |
| etcd backend | Client integration | testcontainers etcd |
| ControlPlane | HA logic (unchanged) | Inject memory DCS, same tests as today |
| End-to-end | Full stack | 3x pacmand + chosen DCS backend |

A shared conformance test suite (`internal/dcs/dcstest/`) runs the same test cases against every backend implementation, ensuring they all behave identically for the operations the ControlPlane depends on.

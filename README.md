# PACMAN

**PACMAN** — **Postgres Autonomous Cluster Manager**

PACMAN is a Go-based high-availability cluster manager for PostgreSQL.

It focuses on a small but important goal: provide safe and understandable PostgreSQL HA with automatic failover, controlled switchover, and explicit rejoin of failed primaries.

---

## Why

PACMAN is built around a few core ideas:

- PostgreSQL HA should be treated as a distributed system problem
- cluster-wide decisions must not be made by a single node in isolation
- topology changes should be explicit state transitions
- the cluster must have one authoritative source of truth
- failover must be quorum-based and fencing-aware

---

## Architecture

PACMAN has two main parts:

- **Node agent** — runs on each PostgreSQL node, observes local state, and executes commands
- **Control plane** — maintains cluster state, elects a leader, and decides failover/switchover

```mermaid
flowchart TB
    subgraph Cluster["PACMAN Cluster"]
        subgraph NodeA["Node A"]
            A["pacmand<br/>agent + control-plane member"]
            APG["PostgreSQL"]
            A --> APG
        end

        subgraph NodeB["Node B"]
            B["pacmand<br/>agent + control-plane member"]
            BPG["PostgreSQL"]
            B --> BPG
        end

        subgraph NodeC["Node C / Witness"]
            C["pacmand<br/>witness + control-plane member"]
        end

        A <--> B
        B <--> C
        A <--> C

        subgraph Truth["Cluster Source of Truth"]
            T["Logical replicated state store<br/><br/>Contains:<br/>- cluster spec<br/>- current primary<br/>- current epoch<br/>- member roles<br/>- failover policy<br/>- maintenance mode<br/>- operation history"]
        end

        A -. quorum replication .-> T
        B -. quorum replication .-> T
        C -. quorum replication .-> T
    end

    CLI["pacmanctl / API"] --> A
    CLI --> B
    CLI --> C
```

## API Contract

A draft OpenAPI contract for the control-plane API lives in [docs/openapi.yaml](docs/openapi.yaml).
It is intentionally inspired by Patroni's operational REST patterns, but adapted to PACMAN's explicit cluster-centric model.

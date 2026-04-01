
# PACMAN Architecture Overview

**PACMAN** — **Postgres Autonomous Cluster Manager**

PACMAN is a Go-based high-availability cluster manager for PostgreSQL.

Its purpose is to manage a PostgreSQL cluster as a distributed system with:

- a local agent on each node,
- a cluster-wide control plane,
- a replicated source of truth,
- and explicit state transitions for failover, switchover, and rejoin.

---

## Goals

PACMAN is designed to provide:

- automatic failover,
- safe planned switchover,
- explicit rejoin of failed primaries,
- deterministic cluster state transitions,
- and a simpler operational model for PostgreSQL HA.

The main design principle is that **cluster-wide decisions must never be made by a single node in isolation**.

---

## High-Level Architecture

PACMAN consists of two main layers:

### 1. Node Agent

A local daemon running on every PostgreSQL node.

Responsibilities:

- observe local PostgreSQL state,
- collect health and replication information,
- manage PostgreSQL lifecycle,
- execute promote / demote / rejoin actions,
- report observed state to the control plane.

### 2. Cluster Control Plane

A distributed control component responsible for cluster-wide decisions.

Responsibilities:

- maintain the cluster source of truth,
- elect a control-plane leader,
- evaluate cluster health,
- decide when failover is allowed,
- select the best promotion candidate,
- coordinate topology transitions,
- track operation history.

---

## Architecture Diagram

```mermaid
flowchart LR
    classDef user fill:#FEF3C7,stroke:#D97706,stroke-width:2px,color:#4A2F00
    classDef daemon fill:#DCFCE7,stroke:#15803D,stroke-width:2px,color:#123524
    classDef witness fill:#FFE4E6,stroke:#BE123C,stroke-width:2px,color:#4C0519
    classDef database fill:#DBEAFE,stroke:#2563EB,stroke-width:2px,color:#172554
    classDef control fill:#ECFEFF,stroke:#0F766E,stroke-width:2px,color:#073B3A
    classDef store fill:#EDE9FE,stroke:#7C3AED,stroke-width:2px,color:#2E1065

    subgraph Entry["Client Entry Points"]
        direction TB
        CTL[[pacmanctl]]
        PTL[[patronictl]]
        API[[Node HTTP API<br/>PACMAN + Patroni-compatible]]
        CTL --> API
        PTL --> API
    end

    subgraph Cluster["PACMAN Cluster"]
        direction LR

        subgraph NodeA["Node A"]
            direction TB
            A[[pacmand<br/>agent + control-plane member]]
            APG[(PostgreSQL<br/>primary)]
            A -->|observe + act| APG
        end

        subgraph NodeB["Node B"]
            direction TB
            B[[pacmand<br/>agent + control-plane member]]
            BPG[(PostgreSQL<br/>standby)]
            B -->|observe + act| BPG
        end

        subgraph NodeC["Node C / Witness"]
            direction TB
            C{{pacmand<br/>witness + voter}}
        end
    end

    subgraph Plane["Distributed Control Plane"]
        direction TB
        L{{Leader election<br/>quorum checks<br/>topology orchestration}}
        T[(Replicated cluster state<br/><br/>cluster spec<br/>leader lease + epoch<br/>health + member roles<br/>failover, switchover, rejoin<br/>maintenance mode + history)]
        L --> T
    end

    API --> A
    API --> B
    API --> C

    A <-->|participates in control plane| L
    B <-->|participates in control plane| L
    C <-->|participates in control plane| L

    A -. publish heartbeats .-> T
    B -. publish heartbeats .-> T
    C -. publish votes .-> T

    class CTL,PTL,API user
    class A,B daemon
    class C witness
    class APG,BPG database
    class L control
    class T store
```

For the pluggable DCS (distributed configuration store) design, see [ARCHITECTURE_DCS.md](ARCHITECTURE_DCS.md).

For the Kubernetes-native deployment model, see [ARCHITECTURE_K8S.md](ARCHITECTURE_K8S.md).

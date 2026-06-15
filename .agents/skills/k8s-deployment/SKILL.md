---
name: k8s-deployment
description: Kubernetes and operator design rules for PACMAN deployment topology and member behavior.
---

# Kubernetes Deployment

Use when discussing or editing Kubernetes/operator design, manifests, topology,
or member identity behavior.

Primary references:

- `docs/ARCHITECTURE_K8S.md`: Kubernetes architecture.
- `deploy/**`: current deployment/lab assets.
- `internal/cluster/spec.go`: cluster spec and member expectations.
- `internal/controlplane`, `internal/agent`: behavior that deployment must
  preserve.

Rules:

- Stable member identity matters; do not treat pods as interchangeable for
  PostgreSQL data nodes.
- Separate control-plane/DCS availability from PostgreSQL data safety.
- Persistent volumes, service identity, and readiness must reflect PostgreSQL
  role and recovery state.
- Rolling changes must preserve quorum and avoid simultaneous unsafe restarts.
- Fencing, failover, and rejoin assumptions must be explicit in the deployment
  model.
- Prefer documenting desired operator behavior before adding generated manifests.

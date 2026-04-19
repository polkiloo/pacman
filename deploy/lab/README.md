# Local Lab Environment

This directory provides a small Docker Compose lab for the same deployment
shape that the Ansible automation targets:

- one external etcd node: `pacman-dcs`
- two PostgreSQL + `pacmand` nodes: `pacman-primary`, `pacman-replica`

The lab is intentionally close to the install integration test, but it is
operator-facing instead of test-only:

- containers stay up after bootstrap
- node state is stored on host bind mounts under `deploy/lab/.local/`
- the checked-in Ansible roles are applied directly to the running lab nodes

## Prerequisites

- Docker with `docker compose`
- a locally built PACMAN runtime RPM in `bin/ansible-install-rpm/`

Build the RPM first:

```bash
make rpm
```

## Files

- `compose.yml`
  three-node container topology for the lab
- `inventory.ini`
  local-connection Ansible inventory used inside the containers
- `vars.yml`
  lab-specific Ansible overrides
- `scripts/bootstrap-cluster.sh`
  creates runtime directories, starts containers, applies Ansible, and starts
  etcd + `pacmand`
- `scripts/demo.sh`
  step-by-step local demo runner that executes verification and operations
  through `docker compose exec` inside the lab containers
- `scripts/destroy-cluster.sh`
  stops the lab containers but preserves local state
- `scripts/reset-state.sh`
  removes the persisted lab state under `.local/`
- `scripts/prepare-runtime-dirs.sh`
  pre-creates the host bind-mount layout

## Quick Start

```bash
deploy/lab/scripts/bootstrap-cluster.sh
```

For a stage-driven local demo runbook:

```bash
deploy/lab/scripts/demo.sh list
deploy/lab/scripts/demo.sh prepare
deploy/lab/scripts/demo.sh bootstrap
deploy/lab/scripts/demo.sh verify
deploy/lab/scripts/demo.sh metrics
deploy/lab/scripts/demo.sh maintenance-enable
deploy/lab/scripts/demo.sh maintenance-disable
deploy/lab/scripts/demo.sh switchover alpha-2
deploy/lab/scripts/demo.sh watch-members 5
deploy/lab/scripts/demo.sh history
```

The runtime demo stages intentionally run from inside the lab containers:

- health and metrics are queried with container-local Python
- cluster status, member listing, maintenance mode, switchover, and history use
  `pacmanctl` inside `pacman-primary`
- the host only needs Docker, `make`, and standard shell tools for the demo

That flow:

- finds the latest PACMAN runtime RPM in `bin/ansible-install-rpm/`
- builds the lab image from `test/docker/pacman-ansible-install.Dockerfile`
- starts the three-node compose environment
- applies the Ansible deployment to each container with `ansible_connection=local`
- starts the external etcd daemon and both `pacmand` daemons
- verifies etcd and PACMAN health endpoints

Useful host endpoints after bootstrap:

- etcd: `http://127.0.0.1:2379`
- primary PACMAN API: `http://127.0.0.1:8081`
- replica PACMAN API: `http://127.0.0.1:8082`
- primary PostgreSQL: `127.0.0.1:5433`
- replica PostgreSQL: `127.0.0.1:5434`

Destroy containers but keep state:

```bash
deploy/lab/scripts/destroy-cluster.sh
```

Wipe persisted state:

```bash
deploy/lab/scripts/reset-state.sh
```

## State Layout

The lab persists host-side state under `deploy/lab/.local/`:

```text
.local/
  vars.generated.yml
  alpha-dcs/
    etc/pacman/
    var/lib/etcd/pacman
    var/log/
  alpha-1/
    etc/pacman/
    var/lib/pacman/
    var/lib/pacman/raft/
    var/lib/pgsql/17/data/
    var/log/
  alpha-2/
    etc/pacman/
    var/lib/pacman/
    var/lib/pacman/raft/
    var/lib/pgsql/17/data/
    var/log/
```

Persistent control-plane state paths:

- external etcd lab:
  authoritative cluster state lives in `alpha-dcs/var/lib/etcd/pacman`
- embedded Raft deployments:
  the upgrade-safe local control-plane state path is `/var/lib/pacman/raft`
  on each PACMAN node, represented here by
  `alpha-1/var/lib/pacman/raft` and `alpha-2/var/lib/pacman/raft`

The bind mounts intentionally preserve `/etc/pacman`, `/var/lib/pacman`, and
the DCS data directory across container rebuilds so operators can rehearse
upgrade and rollback flows without losing state.

## Notes

- The lab currently uses the same external-etcd topology as the Ansible
  examples, not embedded Raft.
- `pacmand` is started manually by the bootstrap script because the lab
  containers do not run `systemd` as PID 1.
- Canonical host-side `systemd` assets live in `deploy/systemd/`.

# Ansible Deployment Automation

This directory contains the current PACMAN Ansible automation for RPM-oriented
hosts. The lab shape stays intentionally small, but it now covers the next
useful layer beyond package installation:

- reusable roles for `external_etcd`, `postgresql`, and `pacman`
- package-name and direct-RPM install examples
- optional PACMAN TLS material distribution and bearer-token rotation
- PostgreSQL streaming-replication bootstrap from the declared initial primary
- both single-node and three-node external etcd layouts
- automated syntax and inventory validation for the checked-in examples

The automation still targets process-mode `pacmand` nodes. The PostgreSQL
extension package is not used here.

## Layout

- `site.yml`
  role-based entrypoint that applies `external_etcd`, `postgresql`, and
  `pacman` to the appropriate host groups
- `group_vars/all.yml`
  shared default variables for the lab and example inventories
- `roles/external_etcd/`
  installs etcd and renders a systemd override that can form either a single
  lab node or a multi-node external etcd cluster
- `roles/postgresql/`
  installs PostgreSQL, configures replication prerequisites, bootstraps the
  initial primary, and clones standby nodes with `pg_basebackup`
- `roles/pacman/`
  installs PACMAN, renders `pacmand.yaml`, distributes admin-token/TLS
  material, and installs the local `pacmand.service` unit
- `examples/package-name/`
  example inventory and vars that install PACMAN by package name
- `examples/rpm-url/`
  example inventory and vars that install PACMAN from a direct RPM URL
- `examples/etcd-ha/`
  example inventory and vars for a three-node external etcd cluster
- `examples/security-overrides.yml.example`
  example variable file for TLS secret distribution and staged bearer-token
  rotation
- `validate.sh`
  validates the checked-in inventories with `ansible-inventory`,
  `ansible-playbook --syntax-check`, and `ansible-playbook --list-tasks`

Related deployment assets:

- `deploy/lab/`
  local Docker Compose lab that applies this Ansible automation to three
  persistent container nodes
- `deploy/systemd/`
  canonical `systemd` service assets for non-RPM or vendored installs

`inventory.ini.example` remains as a simple top-level compatibility inventory
for the original two-postgres-plus-one-etcd lab.

## Example Runs

Package-name install:

```bash
cd deploy/ansible
ansible-playbook -i examples/package-name/hosts.ini site.yml
```

Direct RPM URL install:

```bash
cd deploy/ansible
ansible-playbook -i examples/rpm-url/hosts.ini site.yml
```

Three-node external etcd variant:

```bash
cd deploy/ansible
ansible-playbook -i examples/etcd-ha/hosts.ini site.yml
```

If your inventory hostnames differ from PACMAN member names, set
`pacman_initial_primary_host` to the inventory host that corresponds to
`pacman_initial_primary`.

## Secret Distribution And Rotation

The `pacman` role now supports two admin-token modes:

- inline lab token via `pacman_admin_token_inline`
- staged file distribution via `pacman_admin_token_source_files`

The staged mode exists so operators can distribute both the current and next
bearer token, then switch the active symlink by changing
`pacman_admin_token_active_id` in a later run.

Example:

```bash
cd deploy/ansible
ansible-playbook \
  -i examples/package-name/hosts.ini \
  -e @examples/security-overrides.yml.example \
  site.yml
```

The security example file shows:

- PACMAN API/control-plane TLS certificate, key, and CA distribution
- member mTLS enablement for PACMAN peer traffic
- staged admin-token rollout with an explicit active token ID

Current limit:

- the Ansible automation configures PACMAN endpoint TLS and PACMAN secret
  distribution only
- etcd itself is still rendered as an HTTP-only lab/external cluster here
  because PACMAN's current etcd config surface does not yet model etcd client
  TLS

## PostgreSQL Replication Bootstrap

The `postgresql` role now bootstraps a simple physical-replication topology:

- the host identified by `pacman_initial_primary` / `pacman_initial_primary_host`
  is initialized as the writable primary
- PostgreSQL replication prerequisites are configured:
  `wal_level`, `max_wal_senders`, `max_replication_slots`, `hot_standby`,
  `listen_addresses`, and `pg_hba.conf` entries
- the role creates or updates the replication user on the primary
- every non-primary PostgreSQL host is recloned with `pg_basebackup -R -X stream`
  if it is not already a standby, then verified to enter streaming recovery

This is still bootstrap automation, not a full day-2 lifecycle:

- it does not perform reinit or divergence repair
- it does not configure synchronous replication policy
- it assumes fresh or explicitly replaceable standby data for initial clone

## Validation

Local validation:

```bash
make ansible-validate
```

That target runs:

- `bash -n deploy/ansible/validate.sh`
- inventory resolution for each checked-in example
- playbook syntax validation for each example
- task-list expansion for each example so role wiring and templating stay
  resolvable in CI

# Ansible Lab Deployment

This directory contains a simple Ansible example for a basic PACMAN lab on
RPM-based hosts:

- two PostgreSQL nodes that each run `pacmand`
- one separate external etcd node used as the PACMAN DCS backend

This is intentionally a small bootstrap example, not a production deployment.

Current scope:

- installs PostgreSQL packages on the database nodes
- installs an external etcd package on the DCS node
- installs a PACMAN RPM by package name or by downloaded RPM URL
- writes a minimal `pacmand` config on the PostgreSQL nodes
- writes a single-node etcd systemd override on the DCS node
- installs a simple `pacmand.service` unit on the PostgreSQL nodes

Not included yet:

- PostgreSQL streaming replication bootstrap
- TLS or mTLS material distribution
- secret management beyond a demo bearer-token file
- HA etcd clustering
- distro-specific tuning beyond the default RPM-oriented variables

Important:

- The single external etcd node layout is for a lab only. It is a single point
  of failure and does not provide production quorum.
- The default package and service names target a PostgreSQL 17 / RPM-style
  layout and should be overridden for your distro when needed.

## Files

- `inventory.ini.example`: sample three-node inventory
- `group_vars/all.yml`: default variables for the lab
- `site.yml`: playbook entrypoint
- `templates/`: rendered PACMAN, systemd, and etcd templates

Important inventory variables:

- `pacman_cluster_name`: shared PACMAN cluster name
- `pacman_initial_primary`: bootstrap primary member name
- `pacman_node_name`: per-node PACMAN member name on PostgreSQL hosts
- `etcd_name`: per-node etcd member name on the external DCS host

## Example Run

```bash
cd deploy/ansible
cp inventory.ini.example inventory.ini
ansible-playbook -i inventory.ini site.yml \
  -e pacman_package_url=https://repo.example/pacman-0.1.0-1.el9.x86_64.rpm
```

If PACMAN is already published in a configured RPM repository, skip
`pacman_package_url` and override `pacman_package_name` instead.

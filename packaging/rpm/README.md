# RPM Packaging

This directory contains the initial RPM packaging scaffold for PACMAN on EL9
compatible systems such as Rocky Linux, AlmaLinux, and RHEL.

## Build

The supported entrypoint is the top-level Make target:

```bash
make rpm RPM_VERSION=0.1.0 RPM_RELEASE=1
```

That flow:

- builds an EL9 builder container from [`Containerfile`](./Containerfile)
- stages a deterministic source tarball with a fixed `SOURCE_DATE_EPOCH`
- runs `rpmbuild` inside the container so the host does not need Go, PGXS, or
  RPM tooling installed
- emits the resulting RPMs into `bin/rpm/`

Validation entrypoint:

```bash
make rpm-validate
```

That target builds release `1` and `2` variants of the RPMs and then validates
clean install, upgrade, downgrade, and removal in a fresh EL9 container.

## Package Split Decision

The process-mode runtime and the PostgreSQL extension ship as separate RPMs:

- `pacman`
  ships `pacmand`, `pacmanctl`, the packaged systemd unit, and the default
  process-mode config skeleton
- `pacman-postgresql17-agent`
  ships the `pacman_agent` PostgreSQL 17 extension assets and depends on the
  matching `pacman` version

This split is intentional:

- the PostgreSQL extension is tied to a specific PostgreSQL major version
- pure process-mode deployments should not pull PGXS assets they do not use
- future PostgreSQL majors can ship as parallel extension subpackages without
  forcing a runtime package split

## Filesystem Layout

The RPM layout is:

- `/usr/bin/pacmand`
- `/usr/bin/pacmanctl`
- `/usr/lib/systemd/system/pacmand.service`
- `/etc/sysconfig/pacmand`
- `/etc/pacman/pacmand.yaml`
- `/var/lib/pacman`
- `/var/lib/pacman/raft`
- `/var/log/pacman`
- `/usr/pgsql-17/lib/pacman_agent.so`
- `/usr/pgsql-17/share/extension/pacman_agent.control`
- `/usr/pgsql-17/share/extension/pacman_agent--0.1.0.sql`

The package deliberately does not ship a static shared secret in the payload.
The default config points to `/etc/pacman/admin-token`, and the `%post`
lifecycle hook generates that token file on first install when it is absent.

## Upgrade-Safe Config Policy

The shipped config files are marked as `%config(noreplace)` in the spec:

- `/etc/pacman/pacmand.yaml`
- `/etc/sysconfig/pacmand`

That means local edits are preserved on upgrade. RPM will install package
updates as `.rpmnew` files when the on-host copy has diverged, which keeps
operator-managed configuration intact.

The admin bearer token file is owned as a `%ghost %config(noreplace)` path.
The `%post` script generates `/etc/pacman/admin-token` only on first install if
it does not already exist, then preserves it across upgrades.

## Lifecycle Hooks

The main `pacman` RPM now includes package lifecycle handling for:

- creating a dedicated `pacman` system user and group during `%pre`
- creating `/var/lib/pacman`, `/var/lib/pacman/raft`, and `/var/log/pacman`
  through `tmpfiles.d`
- generating a first-install admin bearer token at `/etc/pacman/admin-token`
  when no token file exists yet
- running the standard systemd package macros so unit metadata is reloaded on
  install, upgrade, and erase

The packaged unit is intentionally not auto-enabled. Operators should enable it
explicitly only after reviewing the shipped config skeleton and adjusting the
local PostgreSQL and DCS settings for the target node role.

## Dependency Policy

Hard runtime dependencies:

- `pacman`
  requires `systemd` and `shadow-utils` because the package owns a systemd unit
  and creates the `pacman` service account during lifecycle hooks
- `pacman-postgresql17-agent`
  requires `pacman` and `postgresql17-server` because the extension is tied to
  PostgreSQL 17 and installs into the PGDG `17` server layout

Optional and weak dependencies:

- `pacman`
  suggests `postgresql17-server` for process-mode data-node installs that use
  the default PGDG PostgreSQL 17 paths from the shipped config skeleton
- `pacman`
  suggests `etcd` for simple lab and external-DCS installs, but does not hard
  require it because production clusters may use another DCS backend or a
  separately managed etcd deployment

## Repository Layout and Signing

The documented public repository layout for EL consumers is:

- `https://packages.pacman.io/rpm/el/$releasever/$basearch/`
- `https://packages.pacman.io/rpm/el/$releasever/SRPMS/`
- `https://packages.pacman.io/keys/RPM-GPG-KEY-pacman`

A sample repository file is provided in
[`pacman.repo.example`](./pacman.repo.example).

Repository consumers should expect:

- `gpgcheck=1` for package signatures
- `repo_gpgcheck=1` for signed repository metadata
- one published armored public key served over HTTPS
- package and repository metadata signatures to be rotated with explicit
  overlap periods rather than silent key replacement

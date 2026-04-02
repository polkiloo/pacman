# `pacman_agent` PostgreSQL Extension

This directory contains the initial PACMAN PostgreSQL background-worker
extension scaffold.

## Layout

- [`pacman_agent.c`](./pacman_agent.c): PostgreSQL module entrypoint, GUC
  definitions, and background-worker registration
- [`pacman_agent.control`](./pacman_agent.control): extension metadata
- [`sql/pacman_agent--0.1.0.sql`](./sql/pacman_agent--0.1.0.sql): SQL install
  script
- [`Makefile`](./Makefile): PGXS build/install entrypoint

## Support Policy

The scaffold currently targets PostgreSQL `17.x` only. The C sources fail the
build outside that major range until PACMAN validates and expands support.

## Usage

Build:

```sh
make build-pg-extension
```

Stage the full install payload:

```sh
make package-pg-extension
```

Install into the `pg_config`-selected PostgreSQL instance:

```sh
make install-pg-extension
```

Enable in `postgresql.conf`:

```conf
shared_preload_libraries = 'pacman_agent'
pacman.node_name = 'alpha-1'
pacman.helper_path = 'pacmand'
pacman.postgres_data_dir = '/var/lib/postgresql/data'
pacman.cluster_name = 'alpha'
```

Further lifecycle wiring is documented in
[`docs/POSTGRES_EXTENSION.md`](../../docs/POSTGRES_EXTENSION.md).

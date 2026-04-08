# Configuration Examples

This directory contains full PACMAN configuration examples for the runtime
surfaces that exist in the repository today.

Available examples:

- `pacmand-raft-data.yaml`: process-mode `pacmand` on a data node using the
  embedded Raft DCS backend.
- `pacmand-raft-witness.yaml`: process-mode `pacmand` on a witness node using
  the embedded Raft DCS backend.
- `pacmand-etcd-data.yaml`: process-mode `pacmand` on a data node using an
  external etcd cluster.
- `pacman-agent.postgresql.conf`: PostgreSQL background-worker bootstrap
  example for `pacman_agent`.

Notes:

- The YAML examples are validated in tests to keep them in sync with the
  current config loader and validator.
- The PostgreSQL extension bridge currently exposes only the node, PostgreSQL,
  and bootstrap settings shown in `pacman-agent.postgresql.conf`. It does not
  currently bridge the full YAML `dcs`, `tls`, or `security` sections.
- `pacmand` runtime DCS backend selection is still being wired. The examples
  document the supported config shape and intended deployment inputs.

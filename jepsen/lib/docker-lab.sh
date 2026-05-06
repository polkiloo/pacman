#!/usr/bin/env bash

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lab_dir="${repo_root}/deploy/lab"
compose_file="${lab_dir}/compose.yml"

timestamp_utc() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

require_tool() {
  local tool=$1
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "${tool} is required for the PACMAN Jepsen Docker lab harness" >&2
    exit 1
  fi
}

run_dir_for() {
  local campaign=$1
  local artifact_root="${PACMAN_JEPSEN_ARTIFACT_DIR:-${repo_root}/jepsen/store}"
  local run_id="${PACMAN_JEPSEN_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

  printf '%s/pacman/%s/%s\n' "${artifact_root}" "${campaign}" "${run_id}"
}

write_edn_event() {
  local history_file=$1
  local op=$2
  local status=$3
  local value=$4

  printf '{:time "%s" :process :bootstrap :type :%s :f :%s :value %s}\n' \
    "$(timestamp_utc)" "${status}" "${op}" "${value}" >>"${history_file}"
}

compose_exec() {
  local service=$1
  shift
  docker compose -f "${compose_file}" exec -T "${service}" "$@"
}

collect_artifacts() {
  local run_dir=$1
  local status=$2

  mkdir -p \
    "${run_dir}/node-logs" \
    "${run_dir}/postgres-logs" \
    "${run_dir}/dcs-logs"

  docker compose -f "${compose_file}" ps >"${run_dir}/docker-compose-ps.txt" 2>&1 || true
  docker compose -f "${compose_file}" logs --no-color >"${run_dir}/docker-compose.log" 2>&1 || true

  compose_exec pacman-primary /bin/sh -lc \
    "cat /var/log/pacman/pacmand.log 2>/dev/null || true" \
    >"${run_dir}/node-logs/alpha-1-pacmand.log" 2>&1 || true
  compose_exec pacman-replica /bin/sh -lc \
    "cat /var/log/pacman/pacmand.log 2>/dev/null || true" \
    >"${run_dir}/node-logs/alpha-2-pacmand.log" 2>&1 || true
  compose_exec pacman-dcs /bin/sh -lc \
    "cat /var/log/etcd.log 2>/dev/null || true" \
    >"${run_dir}/dcs-logs/alpha-dcs-etcd.log" 2>&1 || true

  compose_exec pacman-primary /bin/sh -lc \
    "if [ -d /var/lib/pgsql/17/data/log ]; then find /var/lib/pgsql/17/data/log -maxdepth 1 -type f -print -exec cat {} \\; 2>/dev/null; fi" \
    >"${run_dir}/postgres-logs/alpha-1-postgres.log" 2>&1 || true
  compose_exec pacman-replica /bin/sh -lc \
    "if [ -d /var/lib/pgsql/17/data/log ]; then find /var/lib/pgsql/17/data/log -maxdepth 1 -type f -print -exec cat {} \\; 2>/dev/null; fi" \
    >"${run_dir}/postgres-logs/alpha-2-postgres.log" 2>&1 || true

  compose_exec pacman-primary /bin/sh -lc \
    "PACMANCTL_API_URL=http://pacman-primary:8080 PACMANCTL_API_TOKEN=lab-admin-token pacmanctl cluster status -o json" \
    >"${run_dir}/pacman-cluster-after.json" 2>&1 || true
  compose_exec pacman-primary /bin/sh -lc \
    "PACMANCTL_API_URL=http://pacman-primary:8080 PACMANCTL_API_TOKEN=lab-admin-token pacmanctl history list -o json" \
    >"${run_dir}/pacman-history.json" 2>&1 || true

  cat >"${run_dir}/results.edn" <<EOF
{:valid? ${status}
 :campaign "${PACMAN_JEPSEN_CAMPAIGN:-smoke}"
 :target "pacman-docker-lab"
 :checked-at "$(timestamp_utc)"}
EOF

  cat >"${run_dir}/index.html" <<EOF
<!doctype html>
<html>
<head><meta charset="utf-8"><title>PACMAN Jepsen ${PACMAN_JEPSEN_CAMPAIGN:-smoke}</title></head>
<body>
<h1>PACMAN Jepsen ${PACMAN_JEPSEN_CAMPAIGN:-smoke}</h1>
<p>Status: ${status}</p>
<ul>
<li><a href="results.edn">results.edn</a></li>
<li><a href="jepsen-history.edn">jepsen-history.edn</a></li>
<li><a href="nemesis-schedule.edn">nemesis-schedule.edn</a></li>
<li><a href="pacman-cluster-after.json">pacman-cluster-after.json</a></li>
<li><a href="pacman-history.json">pacman-history.json</a></li>
</ul>
</body>
</html>
EOF
}

bootstrap_lab() {
  PACMAN_LAB_AUTO_PREPARE=false \
    PACMAN_LAB_WAIT_FOR_OBSERVABILITY=false \
    "${repo_root}/deploy/lab/scripts/bootstrap-cluster.sh"
}

verify_lab() {
  "${repo_root}/deploy/lab/scripts/demo.sh" verify
}

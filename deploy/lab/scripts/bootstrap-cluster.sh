#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)
repo_root=$(cd "${lab_dir}/../.." && pwd)
compose_file="${lab_dir}/compose.yml"
runtime_dir="${lab_dir}/.local"
rpm_dir="${PACMAN_LAB_RPM_DIR:-${repo_root}/bin/ansible-install-rpm}"
lab_image="${PACMAN_LAB_IMAGE:-pacman-lab:local}"
lab_base_image="${PACMAN_LAB_BASE_IMAGE:-rockylinux:9}"
vip_address="${PACMAN_LAB_VIP_ADDRESS:-172.28.0.100}"
auto_prepare="${PACMAN_LAB_AUTO_PREPARE:-true}"
prometheus_internal_url="${PACMAN_LAB_PROMETHEUS_INTERNAL_URL:-http://prometheus:9090/-/ready}"
prometheus_url="${PACMAN_LAB_PROMETHEUS_URL:-http://127.0.0.1:9093}"
grafana_internal_url="${PACMAN_LAB_GRAFANA_INTERNAL_URL:-http://grafana:3000/api/health}"
grafana_url="${PACMAN_LAB_GRAFANA_URL:-http://127.0.0.1:3000}"
grafana_admin_user="${PACMAN_LAB_GRAFANA_ADMIN_USER:-admin}"
grafana_admin_password="${PACMAN_LAB_GRAFANA_ADMIN_PASSWORD:-pacman-demo}"
wait_for_observability="${PACMAN_LAB_WAIT_FOR_OBSERVABILITY:-true}"

export PACMAN_LAB_IMAGE="${lab_image}"
export PACMAN_LAB_BASE_IMAGE="${lab_base_image}"

dcs_services=(pacman-dcs pacman-dcs-2 pacman-dcs-3)
dcs_members=(alpha-dcs alpha-dcs-2 alpha-dcs-3)
dcs_client_endpoints="http://pacman-dcs:2379,http://pacman-dcs-2:2379,http://pacman-dcs-3:2379"
dcs_initial_cluster="alpha-dcs=http://pacman-dcs:2380,alpha-dcs-2=http://pacman-dcs-2:2380,alpha-dcs-3=http://pacman-dcs-3:2380"

find_runtime_rpm() {
  local candidate candidate_mtime name newest newest_mtime

  if [[ ! -d "${rpm_dir}" ]]; then
    return 1
  fi

  newest=""
  newest_mtime=0
  while IFS= read -r -d '' candidate; do
    name=$(basename "${candidate}")
    if [[ ! -f "${candidate}" || "${name}" == pacman-postgresql17-agent-* || "${name}" == *.src.rpm ]]; then
      continue
    fi
    if ! candidate_mtime=$(stat -c '%Y' "${candidate}" 2>/dev/null); then
      candidate_mtime=$(stat -f '%m' "${candidate}")
    fi
    if [[ -z "${newest}" || "${candidate_mtime}" -gt "${newest_mtime}" || ( "${candidate_mtime}" -eq "${newest_mtime}" && "${candidate}" > "${newest}" ) ]]; then
      newest=${candidate}
      newest_mtime=${candidate_mtime}
    fi
  done < <(find "${rpm_dir}" -maxdepth 1 -type f -name 'pacman-*.rpm' ! -name 'pacman-postgresql17-agent-*' ! -name '*.src.rpm' -print0)

  if [[ -n "${newest}" ]]; then
    printf '%s\n' "${newest}"
    return 0
  fi

  return 1
}

prepare_runtime_rpm() {
  if [[ "${auto_prepare}" != "true" ]]; then
    return 0
  fi

  printf 'Refreshing PACMAN runtime RPM in %s\n' "${rpm_dir}"
  make -C "${repo_root}" rpm "RPM_OUTPUT_DIR=${rpm_dir}"
}

write_generated_vars() {
  local rpm_path=$1

  mkdir -p "${runtime_dir}"
  cat >"${runtime_dir}/vars.generated.yml" <<EOF
---
pacman_package_name: /artifacts/$(basename "${rpm_path}")
pacman_disable_gpg_check: true
EOF
}

compose_exec() {
  local service=$1
  shift
  docker compose -f "${compose_file}" exec -T "${service}" "$@"
}

compose_exec_detached() {
  local service=$1
  shift
  docker compose -f "${compose_file}" exec -d "${service}" "$@"
}

retry_command() {
  local label=$1
  shift
  local attempts=${PACMAN_LAB_DOCKER_RETRY_ATTEMPTS:-5}
  local delay=${PACMAN_LAB_DOCKER_RETRY_DELAY_SECONDS:-10}
  local attempt

  for ((attempt=1; attempt<=attempts; attempt++)); do
    if "$@"; then
      return 0
    fi

    printf '%s failed on attempt %s/%s\n' "${label}" "${attempt}" "${attempts}" >&2
    if [[ "${attempt}" -lt "${attempts}" ]]; then
      sleep "${delay}"
    fi
  done

  return 1
}

ensure_lab_base_image() {
  if docker image inspect "${lab_base_image}" >/dev/null 2>&1; then
    return 0
  fi

  retry_command "pull ${lab_base_image}" docker pull "${lab_base_image}"
}

start_compose_lab() {
  ensure_lab_base_image
  retry_command "build ${lab_image}" docker compose -f "${compose_file}" build
  docker compose -f "${compose_file}" up -d --no-build
}

apply_playbook() {
  local service=$1
  local limit=$2

  compose_exec "${service}" \
    ansible-playbook \
    -i /workspace/deploy/lab/inventory.ini \
    /workspace/deploy/ansible/site.yml \
    -e @/workspace/deploy/lab/vars.yml \
    -e @/workspace/deploy/lab/.local/vars.generated.yml \
    --limit "${limit}"
}

wait_for_etcd_health() {
  local deadline=$((SECONDS + 60))
  local service

  until compose_exec pacman-dcs /bin/bash -lc \
    "ETCDCTL_API=3 etcdctl --endpoints=${dcs_client_endpoints} endpoint health" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      for service in "${dcs_services[@]}"; do
        compose_exec "${service}" /bin/sh -lc "ps -ef | grep '[e]tcd' || true"
        compose_exec "${service}" /bin/sh -lc 'cat /var/log/etcd.log || true'
      done
      return 1
    fi
    sleep 2
  done
}

wait_for_pacmand_health() {
  local service=$1
  local url=$2
  local deadline=$((SECONDS + 90))

  until compose_exec "${service}" python3 -c \
    "import urllib.request; urllib.request.urlopen('${url}', timeout=3)" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      compose_exec "${service}" /bin/sh -lc 'ss -lnt || true'
      compose_exec "${service}" /bin/sh -lc "ps -ef | grep '[p]acmand' || true"
      compose_exec "${service}" /bin/sh -lc 'cat /var/log/pacman/pacmand.log || true'
      return 1
    fi
    sleep 2
  done
}

wait_for_postgres_vip() {
  local deadline=$((SECONDS + 90))
  local service

  until compose_exec pacman-primary /bin/sh -lc \
    "/usr/pgsql-17/bin/pg_isready -h '${vip_address}' -p 5432 -d postgres >/dev/null" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      for service in pacman-primary pacman-replica pacman-replica-2; do
        compose_exec "${service}" /bin/sh -lc "ip -brief addr show dev eth0 || true"
        compose_exec "${service}" /bin/sh -lc "ps -ef | grep '[v]ip-manager' || true"
        compose_exec "${service}" /bin/sh -lc 'cat /var/log/pacman/vip-manager.log || true'
      done
      return 1
    fi
    sleep 2
  done
}

wait_for_internal_http() {
  local url=$1
  local label=$2
  local log_service=$3
  local deadline=$((SECONDS + 90))

  until compose_exec pacman-primary python3 -c \
    "import urllib.request; urllib.request.urlopen('${url}', timeout=3)" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      docker compose -f "${compose_file}" logs --tail=100 "${log_service}" || true
      return 1
    fi
    sleep 2
  done
}

start_etcd() {
  local index member service

  if compose_exec pacman-dcs /bin/bash -lc \
    "ETCDCTL_API=3 etcdctl --endpoints=${dcs_client_endpoints} endpoint health" >/dev/null 2>&1; then
    wait_for_etcd_health
    return
  fi

  for index in "${!dcs_services[@]}"; do
    service=${dcs_services[${index}]}
    member=${dcs_members[${index}]}

    if compose_exec "${service}" pgrep -f "/usr/bin/etcd .*--name ${member}" >/dev/null 2>&1; then
      continue
    fi

    compose_exec_detached "${service}" \
      /bin/bash -lc \
      "exec /usr/bin/etcd \
        --name ${member} \
        --data-dir /var/lib/etcd/pacman \
        --listen-client-urls http://0.0.0.0:2379 \
        --advertise-client-urls http://${service}:2379 \
        --listen-peer-urls http://0.0.0.0:2380 \
        --initial-advertise-peer-urls http://${service}:2380 \
        --initial-cluster ${dcs_initial_cluster} \
        --initial-cluster-state new \
        --initial-cluster-token pacman-cluster \
        >>/var/log/etcd.log 2>&1"
  done

  wait_for_etcd_health
}

start_pacmand() {
  local service=$1
  local host=$2

  compose_exec "${service}" /bin/sh -lc \
    "pkill -u postgres -f '/usr/bin/pacmand -config /etc/pacman/pacmand.yaml' 2>/dev/null || true"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 20 )); while pgrep -u postgres -f '/usr/bin/pacmand -config /etc/pacman/pacmand.yaml' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for pacmand to stop' >&2; exit 1; fi; sleep 1; done"

  compose_exec_detached "${service}" \
    /bin/bash -lc \
    "cd /var/lib/pacman && exec runuser -u postgres -- /bin/bash -lc '. /etc/sysconfig/pacmand 2>/dev/null || true; export PACMAND_CONFIG PACMAND_EXTRA_ARGS PGPASSWORD; cd /var/lib/pacman && exec /usr/bin/pacmand -config \"\${PACMAND_CONFIG:-/etc/pacman/pacmand.yaml}\" \${PACMAND_EXTRA_ARGS:-}' >>/var/log/pacman/pacmand.log 2>&1"

  wait_for_pacmand_health "${service}" "http://${host}:8080/health"
}

install_lab_walg_shim() {
  local service

  for service in pacman-primary pacman-replica pacman-replica-2; do
    docker compose -f "${compose_file}" exec -T "${service}" /bin/sh -c 'cat >/usr/local/bin/pacman-lab-wal-g && chmod 0755 /usr/local/bin/pacman-lab-wal-g' <<'PACMAN_LAB_WALG'
#!/usr/bin/env bash
set -euo pipefail

command=${1:-}

case "${command}" in
  backup-fetch)
    data_dir=${2:?backup-fetch requires a data directory}
    backup_name=${3:-LATEST}

    rm -rf "${data_dir}"
    mkdir -p "${data_dir}"
    export PGPASSWORD="${PACMAN_LAB_REINIT_REPLICATION_PASSWORD:-replicator}"
    /usr/pgsql-17/bin/pg_basebackup \
      -h "${PACMAN_LAB_REINIT_PRIMARY_HOST:-172.28.0.100}" \
      -p "${PACMAN_LAB_REINIT_PRIMARY_PORT:-5432}" \
      -U "${PACMAN_LAB_REINIT_REPLICATION_USER:-replicator}" \
      -D "${data_dir}" \
      -Fp \
      -Xs \
      -R
    if [[ "$(id -u)" -eq 0 ]]; then
      chown -R postgres:postgres "${data_dir}"
      chmod 0700 "${data_dir}"
    fi
    printf 'pacman lab wal-g shim restored %s into %s\n' "${backup_name}" "${data_dir}" >&2
    ;;
  wal-fetch)
    wal_name=${2:?wal-fetch requires a WAL file name}
    wal_path=${3:?wal-fetch requires a destination path}
    prefix=${WALG_FILE_PREFIX:-/var/lib/pacman/walg}

    if [[ -f "${prefix}/wal/${wal_name}" ]]; then
      cp "${prefix}/wal/${wal_name}" "${wal_path}"
      exit 0
    fi
    printf 'pacman lab wal-g shim missing WAL %s in %s\n' "${wal_name}" "${prefix}/wal" >&2
    exit 1
    ;;
  *)
    printf 'unsupported pacman lab wal-g shim command: %s\n' "${command}" >&2
    exit 2
    ;;
esac
PACMAN_LAB_WALG
  done
}

start_vip_manager() {
  local service=$1
  local vip_manager_pattern='/usr/local/bin/[v]ip-manager --config /etc/pacman/vip-manager.yml'

  compose_exec "${service}" /bin/sh -lc \
    "pids=\$(pgrep -f '${vip_manager_pattern}' 2>/dev/null || true); if [ -n \"\${pids}\" ]; then kill \${pids}; fi"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 20 )); while pgrep -f '${vip_manager_pattern}' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for vip-manager to stop' >&2; exit 1; fi; sleep 1; done"

  compose_exec "${service}" \
    /bin/bash -lc \
    "mkdir -p /var/log/pacman && nohup /usr/local/bin/vip-manager --config /etc/pacman/vip-manager.yml </dev/null >>/var/log/pacman/vip-manager.log 2>&1 &"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 20 )); while ! pgrep -f '${vip_manager_pattern}' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for vip-manager to start' >&2; cat /var/log/pacman/vip-manager.log 2>/dev/null || true; exit 1; fi; sleep 1; done"
}

main() {
  local rpm_path

  prepare_runtime_rpm

  if ! rpm_path=$(find_runtime_rpm); then
    printf 'no PACMAN runtime RPM found in %s\n' "${rpm_dir}" >&2
    printf 'build one first with: make rpm or rerun with PACMAN_LAB_AUTO_PREPARE=true\n' >&2
    exit 1
  fi

  "${script_dir}/prepare-runtime-dirs.sh"
  write_generated_vars "${rpm_path}"

  start_compose_lab

  apply_playbook pacman-dcs alpha-dcs
  apply_playbook pacman-dcs-2 alpha-dcs-2
  apply_playbook pacman-dcs-3 alpha-dcs-3
  apply_playbook pacman-primary alpha-1
  apply_playbook pacman-replica alpha-2
  apply_playbook pacman-replica-2 alpha-3

  install_lab_walg_shim
  start_etcd
  start_pacmand pacman-primary pacman-primary
  start_pacmand pacman-replica pacman-replica
  start_pacmand pacman-replica-2 pacman-replica-2
  start_vip_manager pacman-primary
  start_vip_manager pacman-replica
  start_vip_manager pacman-replica-2
  wait_for_postgres_vip
  if [[ "${wait_for_observability}" == "true" ]]; then
    wait_for_internal_http "${prometheus_internal_url}" "Prometheus" prometheus
    wait_for_internal_http "${grafana_internal_url}" "Grafana" grafana
  fi

  printf 'PACMAN lab bootstrapped successfully.\n'
  printf 'Primary API: http://127.0.0.1:8081\n'
  printf 'Replica APIs: http://127.0.0.1:8082, http://127.0.0.1:8083\n'
  printf 'etcd quorum: http://127.0.0.1:2379 (published first member), %s\n' "${dcs_client_endpoints}"
  printf 'Writable PostgreSQL VIP: %s:5432\n' "${vip_address}"
  if [[ "${wait_for_observability}" == "true" ]]; then
    printf 'Prometheus: %s\n' "${prometheus_url}"
    printf 'Grafana: %s (login: %s / %s)\n' "${grafana_url}" "${grafana_admin_user}" "${grafana_admin_password}"
  else
    printf 'Prometheus/Grafana readiness skipped by PACMAN_LAB_WAIT_FOR_OBSERVABILITY=false\n'
  fi
}

main "$@"

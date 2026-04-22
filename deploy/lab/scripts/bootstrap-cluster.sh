#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)
repo_root=$(cd "${lab_dir}/../.." && pwd)
compose_file="${lab_dir}/compose.yml"
runtime_dir="${lab_dir}/.local"
rpm_dir="${PACMAN_LAB_RPM_DIR:-${repo_root}/bin/ansible-install-rpm}"
lab_image="${PACMAN_LAB_IMAGE:-pacman-lab:local}"
vip_address="${PACMAN_LAB_VIP_ADDRESS:-172.28.0.100}"
auto_prepare="${PACMAN_LAB_AUTO_PREPARE:-true}"
prometheus_internal_url="${PACMAN_LAB_PROMETHEUS_INTERNAL_URL:-http://prometheus:9090/-/ready}"
prometheus_url="${PACMAN_LAB_PROMETHEUS_URL:-http://127.0.0.1:9093}"
grafana_internal_url="${PACMAN_LAB_GRAFANA_INTERNAL_URL:-http://grafana:3000/api/health}"
grafana_url="${PACMAN_LAB_GRAFANA_URL:-http://127.0.0.1:3000}"
grafana_admin_user="${PACMAN_LAB_GRAFANA_ADMIN_USER:-admin}"
grafana_admin_password="${PACMAN_LAB_GRAFANA_ADMIN_PASSWORD:-pacman-demo}"

export PACMAN_LAB_IMAGE="${lab_image}"

find_runtime_rpm() {
  local candidate name
  local candidates=()

  while IFS= read -r candidate; do
    candidates+=("${candidate}")
  done < <(find "${rpm_dir}" -maxdepth 1 -type f -name 'pacman-*.rpm' ! -name 'pacman-postgresql17-agent-*' ! -name '*.src.rpm' -print | sort)

  for (( candidate_index=${#candidates[@]} - 1; candidate_index >= 0; candidate_index-- )); do
    candidate=${candidates[${candidate_index}]}
    name=$(basename "${candidate}")
    if [[ -f "${candidate}" && "${name}" != pacman-postgresql17-agent-* && "${name}" != *.src.rpm ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

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

  until compose_exec pacman-dcs /bin/bash -lc \
    "ETCDCTL_API=3 etcdctl --endpoints=http://127.0.0.1:2379 endpoint health" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      compose_exec pacman-dcs /bin/sh -lc "ps -ef | grep '[e]tcd' || true"
      compose_exec pacman-dcs /bin/sh -lc 'cat /var/log/etcd.log || true'
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

  until compose_exec pacman-primary /bin/sh -lc \
    "/usr/pgsql-17/bin/pg_isready -h '${vip_address}' -p 5432 -d postgres >/dev/null" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      compose_exec pacman-primary /bin/sh -lc "ip -brief addr show dev eth0 || true"
      compose_exec pacman-replica /bin/sh -lc "ip -brief addr show dev eth0 || true"
      compose_exec pacman-primary /bin/sh -lc "ps -ef | grep '[v]ip-manager' || true"
      compose_exec pacman-replica /bin/sh -lc "ps -ef | grep '[v]ip-manager' || true"
      compose_exec pacman-primary /bin/sh -lc 'cat /var/log/pacman/vip-manager.log || true'
      compose_exec pacman-replica /bin/sh -lc 'cat /var/log/pacman/vip-manager.log || true'
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
  if compose_exec pacman-dcs pgrep -f "/usr/bin/etcd --name alpha-dcs" >/dev/null 2>&1; then
    wait_for_etcd_health
    return
  fi

  compose_exec_detached pacman-dcs \
    /bin/bash -lc \
    "exec /usr/bin/etcd \
      --name alpha-dcs \
      --data-dir /var/lib/etcd/pacman \
      --listen-client-urls http://0.0.0.0:2379 \
      --advertise-client-urls http://pacman-dcs:2379 \
      --listen-peer-urls http://0.0.0.0:2380 \
      --initial-advertise-peer-urls http://pacman-dcs:2380 \
      --initial-cluster alpha-dcs=http://pacman-dcs:2380 \
      --initial-cluster-state new \
      --initial-cluster-token pacman-cluster \
      >>/var/log/etcd.log 2>&1"

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

start_vip_manager() {
  local service=$1
  local vip_manager_pattern='[v]ip-manager --config /etc/pacman/vip-manager.yml'

  compose_exec "${service}" /bin/sh -lc \
    "pkill -f '/usr/local/bin/vip-manager --config /etc/pacman/vip-manager.yml' 2>/dev/null || true"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 20 )); while ps -ef | grep '${vip_manager_pattern}' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for vip-manager to stop' >&2; exit 1; fi; sleep 1; done"

  compose_exec "${service}" \
    /bin/bash -lc \
    "mkdir -p /var/log/pacman && nohup /usr/local/bin/vip-manager --config /etc/pacman/vip-manager.yml </dev/null >>/var/log/pacman/vip-manager.log 2>&1 &"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 20 )); while ! ps -ef | grep '${vip_manager_pattern}' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for vip-manager to start' >&2; cat /var/log/pacman/vip-manager.log 2>/dev/null || true; exit 1; fi; sleep 1; done"
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

  docker compose -f "${compose_file}" up -d --build

  apply_playbook pacman-dcs alpha-dcs
  apply_playbook pacman-primary alpha-1
  apply_playbook pacman-replica alpha-2

  start_etcd
  start_pacmand pacman-primary pacman-primary
  start_pacmand pacman-replica pacman-replica
  start_vip_manager pacman-primary
  start_vip_manager pacman-replica
  wait_for_postgres_vip
  wait_for_internal_http "${prometheus_internal_url}" "Prometheus" prometheus
  wait_for_internal_http "${grafana_internal_url}" "Grafana" grafana

  printf 'PACMAN lab bootstrapped successfully.\n'
  printf 'Primary API: http://127.0.0.1:8081\n'
  printf 'Replica API: http://127.0.0.1:8082\n'
  printf 'etcd: http://127.0.0.1:2379\n'
  printf 'Writable PostgreSQL VIP: %s:5432\n' "${vip_address}"
  printf 'Prometheus: %s\n' "${prometheus_url}"
  printf 'Grafana: %s (login: %s / %s)\n' "${grafana_url}" "${grafana_admin_user}" "${grafana_admin_password}"
}

main "$@"

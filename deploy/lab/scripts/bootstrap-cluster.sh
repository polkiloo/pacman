#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)
repo_root=$(cd "${lab_dir}/../.." && pwd)
compose_file="${lab_dir}/compose.yml"
runtime_dir="${lab_dir}/.local"
rpm_dir="${PACMAN_LAB_RPM_DIR:-${repo_root}/bin/ansible-install-rpm}"
lab_image="${PACMAN_LAB_IMAGE:-pacman-lab:local}"

export PACMAN_LAB_IMAGE="${lab_image}"

find_runtime_rpm() {
  local candidate name
  for candidate in "${rpm_dir}"/pacman-*.rpm; do
    name=$(basename "${candidate}")
    if [[ -f "${candidate}" && "${name}" != pacman-postgresql17-agent-* && "${name}" != *.src.rpm ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
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

  if ! compose_exec "${service}" pgrep -u postgres -f "/usr/bin/pacmand -config /etc/pacman/pacmand.yaml" >/dev/null 2>&1; then
    compose_exec_detached "${service}" \
      /bin/bash -lc \
      "cd /var/lib/pacman && exec runuser -u postgres -- /usr/bin/pacmand -config /etc/pacman/pacmand.yaml >>/var/log/pacman/pacmand.log 2>&1"
  fi

  wait_for_pacmand_health "${service}" "http://${host}:8080/health"
}

main() {
  local rpm_path

  if ! rpm_path=$(find_runtime_rpm); then
    printf 'no PACMAN runtime RPM found in %s\n' "${rpm_dir}" >&2
    printf 'build one first with: make rpm\n' >&2
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

  printf 'PACMAN lab bootstrapped successfully.\n'
  printf 'Primary API: http://127.0.0.1:8081\n'
  printf 'Replica API: http://127.0.0.1:8082\n'
  printf 'etcd: http://127.0.0.1:2379\n'
}

main "$@"

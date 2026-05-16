#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)

"${script_dir}/destroy-cluster.sh" || true

docker rm -f \
  pacman-lab-grafana \
  pacman-lab-prometheus \
  pacman-lab-replica-2-node-exporter \
  pacman-lab-replica-node-exporter \
  pacman-lab-primary-node-exporter \
  pacman-lab-dcs-node-exporter \
  pacman-lab-replica \
  pacman-lab-replica-2 \
  pacman-lab-primary \
  pacman-lab-dcs >/dev/null 2>&1 || true

docker network rm pacman-lab_pacman-lab-net >/dev/null 2>&1 || true

local_state="${lab_dir}/.local"
if [[ -e "${local_state}" ]]; then
  trash_state="${lab_dir}/.local.reset-$$"
  if mv "${local_state}" "${trash_state}" 2>/dev/null; then
    rm -rf "${trash_state}" 2>/dev/null || true
  else
    rm -rf "${local_state}" 2>/dev/null || true
  fi
fi

rm -rf "${lab_dir}"/.local.reset-* 2>/dev/null || true

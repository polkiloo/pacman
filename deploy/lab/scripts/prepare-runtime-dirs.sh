#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)
runtime_dir="${lab_dir}/.local"

mkdir -p \
  "${runtime_dir}/alpha-dcs/etc/pacman" \
  "${runtime_dir}/alpha-dcs/var/lib/etcd/pacman" \
  "${runtime_dir}/alpha-dcs/var/log" \
  "${runtime_dir}/alpha-1/etc/pacman" \
  "${runtime_dir}/alpha-1/var/lib/pacman/raft" \
  "${runtime_dir}/alpha-1/var/lib/pgsql/17/data" \
  "${runtime_dir}/alpha-1/var/log" \
  "${runtime_dir}/alpha-2/etc/pacman" \
  "${runtime_dir}/alpha-2/var/lib/pacman/raft" \
  "${runtime_dir}/alpha-2/var/lib/pgsql/17/data" \
  "${runtime_dir}/alpha-2/var/log"

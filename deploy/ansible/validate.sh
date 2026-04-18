#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

examples=(
  "examples/package-name/hosts.ini"
  "examples/rpm-url/hosts.ini"
  "examples/etcd-ha/hosts.ini"
)

for inventory in "${examples[@]}"; do
  ansible-inventory -i "${inventory}" --list >/dev/null
  ansible-playbook -i "${inventory}" --syntax-check site.yml >/dev/null
  ansible-playbook -i "${inventory}" --list-tasks site.yml >/dev/null
done

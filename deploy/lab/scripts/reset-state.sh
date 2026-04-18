#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)

"${script_dir}/destroy-cluster.sh" || true
rm -rf "${lab_dir}/.local"

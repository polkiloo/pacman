#!/usr/bin/env bash

workloads_lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/workloads" && pwd)"

# shellcheck source=workloads/config.sh
source "${workloads_lib_dir}/config.sh"
# shellcheck source=workloads/util.sh
source "${workloads_lib_dir}/util.sh"
# shellcheck source=workloads/db.sh
source "${workloads_lib_dir}/db.sh"
# shellcheck source=workloads/capture.sh
source "${workloads_lib_dir}/capture.sh"
# shellcheck source=workloads/topology.sh
source "${workloads_lib_dir}/topology.sh"
# shellcheck source=workloads/sampling.sh
source "${workloads_lib_dir}/sampling.sh"
# shellcheck source=workloads/checkers.sh
source "${workloads_lib_dir}/checkers.sh"
# shellcheck source=workloads/profiles.sh
source "${workloads_lib_dir}/profiles.sh"
# shellcheck source=workloads/nemesis.sh
source "${workloads_lib_dir}/nemesis.sh"
# shellcheck source=workloads/runner.sh
source "${workloads_lib_dir}/runner.sh"

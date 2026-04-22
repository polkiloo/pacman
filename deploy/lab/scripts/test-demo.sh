#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
demo_script="${script_dir}/demo.sh"

assert_contains() {
	local haystack=$1
	local needle=$2

	if [[ "${haystack}" != *"${needle}"* ]]; then
		printf 'expected output to contain %q\n' "${needle}" >&2
		printf 'actual output:\n%s\n' "${haystack}" >&2
		exit 1
	fi
}

run_and_capture() {
	local output
	output=$("$@")
	printf '%s' "${output}"
}

printf '==> bash syntax check\n'
bash -n "${demo_script}"

printf '==> list stages\n'
list_output=$(run_and_capture "${demo_script}" list)
assert_contains "${list_output}" "prepare"
assert_contains "${list_output}" "full-demo"
assert_contains "${list_output}" "watch-members"
assert_contains "${list_output}" "postgres-config"
assert_contains "${list_output}" "observability"

printf '==> dry-run prepare\n'
prepare_output=$(run_and_capture "${demo_script}" --dry-run prepare)
assert_contains "${prepare_output}" "make rpm"
assert_contains "${prepare_output}" "RPM_OUTPUT_DIR="

printf '==> dry-run bootstrap\n'
bootstrap_output=$(run_and_capture "${demo_script}" --dry-run bootstrap)
assert_contains "${bootstrap_output}" "bootstrap-cluster.sh"

printf '==> dry-run verify\n'
verify_output=$(run_and_capture "${demo_script}" --dry-run verify)
assert_contains "${verify_output}" "docker compose"
assert_contains "${verify_output}" "exec -T pacman-primary"
assert_contains "${verify_output}" "exec -T pacman-replica"
assert_contains "${verify_output}" "pacmanctl cluster status"
assert_contains "${verify_output}" "pacmanctl members list"
assert_contains "${verify_output}" "vip-manager PostgreSQL VIP"

printf '==> dry-run metrics\n'
metrics_output=$(run_and_capture "${demo_script}" --dry-run metrics)
assert_contains "${metrics_output}" "/metrics"
assert_contains "${metrics_output}" "pacman_cluster_"
assert_contains "${metrics_output}" "docker compose"

printf '==> dry-run observability\n'
observability_output=$(run_and_capture "${demo_script}" --dry-run observability)
assert_contains "${observability_output}" "/api/v1/targets"
assert_contains "${observability_output}" "Grafana UI: http://127.0.0.1:3000"
assert_contains "${observability_output}" "PACMAN Demo Overview"

printf '==> dry-run postgres-config\n'
postgres_config_output=$(run_and_capture "${demo_script}" --dry-run postgres-config shared_buffers 256MB)
assert_contains "${postgres_config_output}" "pacmanctl cluster spec show -o json"
assert_contains "${postgres_config_output}" "etcdctl put"
assert_contains "${postgres_config_output}" "shared_buffers=256MB"

printf '==> dry-run maintenance enable\n'
maintenance_enable_output=$(run_and_capture "${demo_script}" --dry-run maintenance-enable)
assert_contains "${maintenance_enable_output}" "pacmanctl cluster maintenance enable"
assert_contains "${maintenance_enable_output}" "demo-maintenance"

printf '==> dry-run maintenance disable\n'
maintenance_disable_output=$(run_and_capture "${demo_script}" --dry-run maintenance-disable)
assert_contains "${maintenance_disable_output}" "pacmanctl cluster maintenance disable"
assert_contains "${maintenance_disable_output}" "demo-maintenance-complete"

printf '==> dry-run switchover\n'
switchover_output=$(run_and_capture "${demo_script}" --dry-run switchover alpha-2)
assert_contains "${switchover_output}" "pacmanctl cluster switchover"
assert_contains "${switchover_output}" "-candidate"
assert_contains "${switchover_output}" "alpha-2"

printf '==> dry-run switchover with fallback candidate\n'
switchover_fallback_output=$(run_and_capture env PACMAN_DEMO_SWITCHOVER_CANDIDATE=alpha-1 "${demo_script}" --dry-run switchover)
assert_contains "${switchover_fallback_output}" "pacmanctl cluster switchover"
assert_contains "${switchover_fallback_output}" "-candidate"
assert_contains "${switchover_fallback_output}" "alpha-1"

printf '==> dry-run history\n'
history_output=$(run_and_capture "${demo_script}" --dry-run history)
assert_contains "${history_output}" "pacmanctl history list"

printf '==> dry-run load-on\n'
load_on_output=$(run_and_capture "${demo_script}" --dry-run load-on)
assert_contains "${load_on_output}" "start pgbench background load"
assert_contains "${load_on_output}" "until load-off"
assert_contains "${load_on_output}" "pgbench supervisor started"

printf '==> dry-run load-on with duration cap\n'
timed_load_on_output=$(run_and_capture env PACMAN_DEMO_PGBENCH_DURATION=30 "${demo_script}" --dry-run load-on)
assert_contains "${timed_load_on_output}" "30s"

printf '==> dry-run watch-members\n'
watch_output=$(run_and_capture "${demo_script}" --dry-run watch-members 2)
assert_contains "${watch_output}" "pacmanctl members list"
assert_contains "${watch_output}" "sleep"

printf '==> dry-run destroy/reset\n'
destroy_output=$(run_and_capture "${demo_script}" --dry-run destroy)
assert_contains "${destroy_output}" "destroy-cluster.sh"
reset_output=$(run_and_capture "${demo_script}" --dry-run reset)
assert_contains "${reset_output}" "reset-state.sh"

printf '==> dry-run full demo\n'
full_demo_output=$(run_and_capture "${demo_script}" --dry-run full-demo)
assert_contains "${full_demo_output}" "bootstrap-cluster.sh"
assert_contains "${full_demo_output}" "pacmanctl cluster switchover"
assert_contains "${full_demo_output}" "pacmanctl history list"
assert_contains "${full_demo_output}" "vip-manager PostgreSQL VIP"
assert_contains "${full_demo_output}" "Prometheus UI: http://127.0.0.1:9093"

printf 'demo script dry-run verification passed\n'

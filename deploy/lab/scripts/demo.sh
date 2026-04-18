#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)
repo_root=$(cd "${lab_dir}/../.." && pwd)

dry_run=false

api_url="${PACMAN_DEMO_API_URL:-http://127.0.0.1:8081}"
replica_api_url="${PACMAN_DEMO_REPLICA_API_URL:-http://127.0.0.1:8082}"
api_token="${PACMAN_DEMO_API_TOKEN:-lab-admin-token}"
rpm_dir="${PACMAN_DEMO_RPM_DIR:-${repo_root}/bin/ansible-install-rpm}"
default_candidate="${PACMAN_DEMO_SWITCHOVER_CANDIDATE:-alpha-2}"
watch_iterations="${PACMAN_DEMO_WATCH_ITERATIONS:-10}"
watch_delay="${PACMAN_DEMO_WATCH_DELAY:-2}"

print_usage() {
	cat <<EOF
Usage:
  $(basename "$0") [--dry-run] <stage> [stage-args]

Stages:
  list
  prepare
  bootstrap
  probes
  cluster
  members
  metrics
  verify
  maintenance-enable
  maintenance-disable
  switchover [candidate]
  watch-members [iterations]
  history
  full-demo
  destroy
  reset

Environment:
  PACMAN_DEMO_API_URL              default: ${api_url}
  PACMAN_DEMO_REPLICA_API_URL      default: ${replica_api_url}
  PACMAN_DEMO_API_TOKEN            default: ${api_token}
  PACMAN_DEMO_RPM_DIR              default: ${rpm_dir}
  PACMAN_DEMO_SWITCHOVER_CANDIDATE default: ${default_candidate}
  PACMAN_DEMO_WATCH_ITERATIONS     default: ${watch_iterations}
  PACMAN_DEMO_WATCH_DELAY          default: ${watch_delay}

Notes:
  - Native /api/v1/* endpoints require the lab bearer token.
  - The local lab member names are alpha-1 and alpha-2.
  - This demo script intentionally avoids failover + rejoin because current
    rejoin execution is an explicit multi-stage workflow, not a simple restart.
EOF
}

print_stage_list() {
	cat <<'EOF'
prepare
bootstrap
probes
cluster
members
metrics
verify
maintenance-enable
maintenance-disable
switchover
watch-members
history
full-demo
destroy
reset
EOF
}

log() {
	printf '==> %s\n' "$*"
}

render_command() {
	local command=()
	local part

	for part in "$@"; do
		command+=("$(printf '%q' "${part}")")
	done

	printf '%s\n' "${command[*]}"
}

run_command() {
	if "${dry_run}"; then
		render_command "$@"
		return 0
	fi

	"$@"
}

require_tool() {
	local tool=$1

	if "${dry_run}"; then
		return 0
	fi

	if ! command -v "${tool}" >/dev/null 2>&1; then
		printf 'required tool is missing: %s\n' "${tool}" >&2
		exit 1
	fi
}

show_json() {
	local method=$1
	local url=$2
	local filter=$3
	local body=${4:-}
	local response

	require_tool curl

	if "${dry_run}"; then
		if [[ -n "${body}" ]]; then
			render_command curl -fsS -X "${method}" -H "Authorization: Bearer ${api_token}" -H "Accept: application/json" -H "Content-Type: application/json" --data "${body}" "${url}"
		else
			render_command curl -fsS -X "${method}" -H "Authorization: Bearer ${api_token}" -H "Accept: application/json" "${url}"
		fi

		printf '# jq filter: %s\n' "${filter}"
		return 0
	fi

	if [[ -n "${body}" ]]; then
		response=$(curl -fsS -X "${method}" \
			-H "Authorization: Bearer ${api_token}" \
			-H "Accept: application/json" \
			-H "Content-Type: application/json" \
			--data "${body}" \
			"${url}")
	else
		response=$(curl -fsS -X "${method}" \
			-H "Authorization: Bearer ${api_token}" \
			-H "Accept: application/json" \
			"${url}")
	fi

	if command -v jq >/dev/null 2>&1; then
		printf '%s\n' "${response}" | jq "${filter}"
		return 0
	fi

	printf '%s\n' "${response}"
}

show_probe() {
	local url=$1
	local label=$2

	require_tool curl

	log "${label}"
	if "${dry_run}"; then
		render_command curl -fsS "${url}"
		printf '# jq filter: .\n'
		return 0
	fi

	local response
	response=$(curl -fsS "${url}")
	if command -v jq >/dev/null 2>&1; then
		printf '%s\n' "${response}" | jq .
		return 0
	fi

	printf '%s\n' "${response}"
}

stage_prepare() {
	require_tool make

	log "build runtime RPM into ${rpm_dir}"
	run_command make rpm "RPM_OUTPUT_DIR=${rpm_dir}"
}

stage_bootstrap() {
	require_tool docker

	log "bootstrap local lab"
	run_command "${script_dir}/bootstrap-cluster.sh"
}

stage_probes() {
	show_probe "${api_url}/health" "primary /health"
	show_probe "${replica_api_url}/health" "replica /health"
}

stage_cluster() {
	log "cluster topology"
	show_json GET "${api_url}/api/v1/cluster" '{clusterName, phase, currentPrimary, currentEpoch, maintenance, members: [.members[] | {name, role, state, healthy}]}'
}

stage_members() {
	log "members"
	show_json GET "${api_url}/api/v1/members" '.items | map({name, role, state, healthy, lagBytes, timeline})'
}

stage_metrics() {
	require_tool curl
	log "key prometheus metrics"

	if "${dry_run}"; then
		render_command curl -fsS "${api_url}/metrics"
		printf '# grep: ^pacman_cluster_|^pacman_member_info|^pacman_node_info\n'
		return 0
	fi

	local metrics
	metrics=$(curl -fsS "${api_url}/metrics")
	printf '%s\n' "${metrics}" | grep -E '^pacman_cluster_|^pacman_member_info|^pacman_node_info'
}

stage_verify() {
	stage_probes
	stage_cluster
	stage_members
}

stage_maintenance_enable() {
	log "enable maintenance mode"
	show_json PUT "${api_url}/api/v1/maintenance" '.' '{"enabled":true,"reason":"demo-maintenance","requestedBy":"demo-script"}'
}

stage_maintenance_disable() {
	log "disable maintenance mode"
	show_json PUT "${api_url}/api/v1/maintenance" '.' '{"enabled":false,"reason":"demo-maintenance-complete","requestedBy":"demo-script"}'
}

stage_switchover() {
	local candidate=${1:-${default_candidate}}

	log "request switchover to ${candidate}"
	show_json POST "${api_url}/api/v1/operations/switchover" '.' "{\"candidate\":\"${candidate}\",\"reason\":\"demo-switchover\",\"requestedBy\":\"demo-script\"}"
}

stage_watch_members() {
	local iterations=${1:-${watch_iterations}}
	local iteration

	require_tool sleep

	for ((iteration = 1; iteration <= iterations; iteration++)); do
		log "member watch ${iteration}/${iterations}"
		stage_members

		if (( iteration < iterations )); then
			if "${dry_run}"; then
				render_command sleep "${watch_delay}"
			else
				sleep "${watch_delay}"
			fi
		fi
	done
}

stage_history() {
	log "operation history"
	show_json GET "${api_url}/api/v1/history" '.items'
}

stage_full_demo() {
	stage_prepare
	stage_bootstrap
	stage_verify
	stage_metrics
	stage_maintenance_enable
	stage_maintenance_disable
	stage_switchover
	stage_watch_members 5
	stage_history
}

stage_destroy() {
	require_tool docker

	log "destroy lab containers and keep state"
	run_command "${script_dir}/destroy-cluster.sh"
}

stage_reset() {
	require_tool docker

	log "destroy lab and wipe state"
	run_command "${script_dir}/reset-state.sh"
}

main() {
	local stage

	if [[ $# -eq 0 ]]; then
		print_usage
		exit 1
	fi

	while [[ $# -gt 0 ]]; do
		case "$1" in
			--dry-run)
				dry_run=true
				shift
				;;
			-h|--help|help)
				print_usage
				return 0
				;;
			*)
				break
				;;
		esac
	done

	if [[ $# -eq 0 ]]; then
		print_usage
		exit 1
	fi

	stage=$1
	shift

	case "${stage}" in
		list)
			print_stage_list
			;;
		prepare)
			stage_prepare "$@"
			;;
		bootstrap)
			stage_bootstrap "$@"
			;;
		probes|health)
			stage_probes "$@"
			;;
		cluster|status)
			stage_cluster "$@"
			;;
		members)
			stage_members "$@"
			;;
		metrics)
			stage_metrics "$@"
			;;
		verify)
			stage_verify "$@"
			;;
		maintenance-enable)
			stage_maintenance_enable "$@"
			;;
		maintenance-disable)
			stage_maintenance_disable "$@"
			;;
		switchover)
			stage_switchover "$@"
			;;
		watch-members)
			stage_watch_members "$@"
			;;
		history)
			stage_history "$@"
			;;
		full-demo)
			stage_full_demo "$@"
			;;
		destroy)
			stage_destroy "$@"
			;;
		reset)
			stage_reset "$@"
			;;
		*)
			printf 'unknown stage: %s\n\n' "${stage}" >&2
			print_usage >&2
			exit 1
			;;
	esac
}

main "$@"

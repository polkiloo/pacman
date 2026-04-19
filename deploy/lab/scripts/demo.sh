#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)
repo_root=$(cd "${lab_dir}/../.." && pwd)
compose_file="${lab_dir}/compose.yml"

dry_run=false

primary_service="${PACMAN_DEMO_PRIMARY_SERVICE:-pacman-primary}"
replica_service="${PACMAN_DEMO_REPLICA_SERVICE:-pacman-replica}"
primary_api_url="${PACMAN_DEMO_PRIMARY_API_URL:-http://127.0.0.1:8080}"
replica_api_url="${PACMAN_DEMO_REPLICA_API_URL:-http://127.0.0.1:8080}"
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
  PACMAN_DEMO_PRIMARY_SERVICE      default: ${primary_service}
  PACMAN_DEMO_REPLICA_SERVICE      default: ${replica_service}
  PACMAN_DEMO_PRIMARY_API_URL      default: ${primary_api_url}
  PACMAN_DEMO_REPLICA_API_URL      default: ${replica_api_url}
  PACMAN_DEMO_API_TOKEN            default: ${api_token}
  PACMAN_DEMO_RPM_DIR              default: ${rpm_dir}
  PACMAN_DEMO_SWITCHOVER_CANDIDATE default: ${default_candidate}
  PACMAN_DEMO_WATCH_ITERATIONS     default: ${watch_iterations}
  PACMAN_DEMO_WATCH_DELAY          default: ${watch_delay}

Notes:
  - Runtime demo stages execute through docker compose, not host curl/jq.
  - Native /api/v1/* access uses the lab bearer token from inside the container.
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

compose_exec() {
	local service=$1
	shift

	run_command docker compose -f "${compose_file}" exec -T "${service}" "$@"
}

compose_shell() {
	local service=$1
	local command=$2

	compose_exec "${service}" /bin/sh -lc "${command}"
}

require_pacmanctl() {
	if "${dry_run}"; then
		return 0
	fi

	compose_exec "${primary_service}" test -x /usr/bin/pacmanctl >/dev/null
}

require_python() {
	local service=$1

	if "${dry_run}"; then
		return 0
	fi

	compose_exec "${service}" command -v python3 >/dev/null
}

python_get_json() {
	local service=$1
	local url=$2

	compose_exec "${service}" python3 -c \
		"import json, urllib.request; req = urllib.request.Request('${url}', headers={'Accept': 'application/json', 'Authorization': 'Bearer ${api_token}'}); print(json.dumps(json.load(urllib.request.urlopen(req, timeout=5)), indent=2, sort_keys=True))"
}

show_probe() {
	local service=$1
	local url=$2
	local label=$3

	require_python "${service}"
	log "${label}"

	python_get_json "${service}" "${url}"
}

run_pacmanctl() {
	local service=$1
	shift

	require_pacmanctl
	compose_exec "${service}" \
		env \
			PACMANCTL_API_URL="${primary_api_url}" \
			PACMANCTL_API_TOKEN="${api_token}" \
			pacmanctl "$@"
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
	show_probe "${primary_service}" "${primary_api_url}/health" "primary /health"
	show_probe "${replica_service}" "${replica_api_url}/health" "replica /health"
}

stage_cluster() {
	log "cluster status via pacmanctl in ${primary_service}"
	run_pacmanctl "${primary_service}" cluster status
}

stage_members() {
	log "members via pacmanctl in ${primary_service}"
	run_pacmanctl "${primary_service}" members list
}

stage_metrics() {
	require_python "${primary_service}"
	log "key prometheus metrics from ${primary_service}"

	compose_shell "${primary_service}" \
		"python3 -c \"import urllib.request; print(urllib.request.urlopen('${primary_api_url}/metrics', timeout=5).read().decode(), end='')\" | grep -E '^pacman_cluster_|^pacman_member_info|^pacman_node_info'"
}

stage_verify() {
	stage_probes
	stage_cluster
	stage_members
}

stage_maintenance_enable() {
	log "enable maintenance mode via pacmanctl in ${primary_service}"
	run_pacmanctl "${primary_service}" cluster maintenance enable -reason demo-maintenance -requested-by demo-script
}

stage_maintenance_disable() {
	log "disable maintenance mode via pacmanctl in ${primary_service}"
	run_pacmanctl "${primary_service}" cluster maintenance disable -reason demo-maintenance-complete -requested-by demo-script
}

stage_switchover() {
	local candidate=${1:-${default_candidate}}

	log "request switchover to ${candidate} via pacmanctl in ${primary_service}"
	run_pacmanctl "${primary_service}" cluster switchover -candidate "${candidate}" -reason demo-switchover -requested-by demo-script
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
	log "history via pacmanctl in ${primary_service}"
	run_pacmanctl "${primary_service}" history list
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

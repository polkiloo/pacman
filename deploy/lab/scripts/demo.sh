#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(cd "${script_dir}/.." && pwd)
repo_root=$(cd "${lab_dir}/../.." && pwd)
compose_file="${lab_dir}/compose.yml"

dry_run=false

primary_service="${PACMAN_DEMO_PRIMARY_SERVICE:-pacman-primary}"
replica_service="${PACMAN_DEMO_REPLICA_SERVICE:-pacman-replica}"
primary_api_url="${PACMAN_DEMO_PRIMARY_API_URL:-http://${primary_service}:8080}"
replica_api_url="${PACMAN_DEMO_REPLICA_API_URL:-http://${replica_service}:8080}"
api_token="${PACMAN_DEMO_API_TOKEN:-lab-admin-token}"
rpm_dir="${PACMAN_DEMO_RPM_DIR:-${repo_root}/bin/ansible-install-rpm}"
default_candidate="${PACMAN_DEMO_SWITCHOVER_CANDIDATE:-alpha-2}"
watch_iterations="${PACMAN_DEMO_WATCH_ITERATIONS:-10}"
watch_delay="${PACMAN_DEMO_WATCH_DELAY:-2}"
postgres_config_parameter="${PACMAN_DEMO_POSTGRES_PARAMETER:-log_min_duration_statement}"
postgres_config_value="${PACMAN_DEMO_POSTGRES_VALUE:-250ms}"
vip_address="${PACMAN_DEMO_VIP_ADDRESS:-172.28.0.100}"
vip_interface="${PACMAN_DEMO_VIP_INTERFACE:-eth0}"
rw_host="${PACMAN_DEMO_RW_HOST:-${vip_address}}"

pgbench_host="${PACMAN_DEMO_PGBENCH_HOST:-${rw_host}}"
pgbench_port="${PACMAN_DEMO_PGBENCH_PORT:-5432}"
pgbench_user="${PACMAN_DEMO_PGBENCH_USER:-postgres}"
pgbench_password="${PACMAN_DEMO_PGBENCH_PASSWORD:-pacman-demo-password}"
pgbench_db="${PACMAN_DEMO_PGBENCH_DB:-postgres}"
pgbench_scale="${PACMAN_DEMO_PGBENCH_SCALE:-10}"
pgbench_clients="${PACMAN_DEMO_PGBENCH_CLIENTS:-4}"
pgbench_threads="${PACMAN_DEMO_PGBENCH_THREADS:-2}"
pgbench_duration="${PACMAN_DEMO_PGBENCH_DURATION:-120}"
pgbench_chunk_duration="${PACMAN_DEMO_PGBENCH_CHUNK_DURATION:-15}"

pgbench_pid_file="/tmp/pacman-demo-pgbench.pid"
pgbench_child_pid_file="/tmp/pacman-demo-pgbench.child.pid"
pgbench_pause_file="/tmp/pacman-demo-pgbench.pause"
pgbench_log_file="/tmp/pacman-demo-pgbench.log"

print_usage() {
	cat <<EOF
Usage:
  $(basename "$0") [--dry-run] <stage> [stage-args]

Stages:
  list                               [catalog]
  prepare                            [build/runtime]
  bootstrap                          [ansible/bootstrap]
  probes                             [Patroni-compatible API]
  cluster                            [PACMAN-native API]
  members                            [PACMAN-native API]
  postgres-config [parameter] [value] [PACMAN-native API + DCS]
  metrics                            [Patroni-compatible API]
  verify                             [mixed: Patroni-compatible + PACMAN-native]
  maintenance-enable                 [PACMAN-native API]
  maintenance-disable                [PACMAN-native API]
  cancel-switchover                  [PACMAN-native API]
  switchover [candidate]             [PACMAN-native API]
  watch-members [iterations]         [PACMAN-native API]
  history                            [PACMAN-native API]
  pgbench-init                       [PostgreSQL client]
  load-on                            [PostgreSQL client]
  load-off                           [PostgreSQL client]
  pgbench-stats                      [pgbench log]
  full-demo                          [mixed]
  destroy                            [docker lifecycle]
  reset                              [docker lifecycle]

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
  PACMAN_DEMO_POSTGRES_PARAMETER   default: ${postgres_config_parameter}
  PACMAN_DEMO_POSTGRES_VALUE       default: ${postgres_config_value}
  PACMAN_DEMO_VIP_ADDRESS          default: ${vip_address}
  PACMAN_DEMO_VIP_INTERFACE        default: ${vip_interface}
  PACMAN_DEMO_RW_HOST              default: ${rw_host}
  PACMAN_DEMO_PGBENCH_HOST         default: ${pgbench_host}
  PACMAN_DEMO_PGBENCH_PORT         default: ${pgbench_port}
  PACMAN_DEMO_PGBENCH_USER         default: ${pgbench_user}
  PACMAN_DEMO_PGBENCH_PASSWORD     default: ${pgbench_password}
  PACMAN_DEMO_PGBENCH_DB           default: ${pgbench_db}
  PACMAN_DEMO_PGBENCH_SCALE        default: ${pgbench_scale}
  PACMAN_DEMO_PGBENCH_CLIENTS      default: ${pgbench_clients}
  PACMAN_DEMO_PGBENCH_THREADS      default: ${pgbench_threads}
  PACMAN_DEMO_PGBENCH_DURATION     default: ${pgbench_duration}s
  PACMAN_DEMO_PGBENCH_CHUNK_DURATION default: ${pgbench_chunk_duration}s

Notes:
  - Runtime demo stages execute through docker compose, not host curl/jq.
  - PACMAN_DEMO_*_API_URL values must be reachable from inside the lab
    containers. Host-published ports stay available on 127.0.0.1:8081/8082.
  - Native /api/v1/* access uses node-local PACMAN URLs plus the lab bearer token.
  - vip-manager manages the writable PostgreSQL VIP at ${vip_address}; PostgreSQL
    client stages default to that address so they follow switchovers automatically.
  - The local lab member names are alpha-1 and alpha-2.
  - postgres-config updates the desired cluster PostgreSQL parameter map in DCS
    and waits for /api/v1/cluster/spec to reflect the new value.
  - postgres-config does not hot-reload the running PostgreSQL instances yet;
    it demonstrates desired-state propagation, not live config application.
  - pgbench runs inside pacman-primary against ${pgbench_host}:${pgbench_port}.
    The demo supervisor restarts interrupted pgbench chunks during switchover
    so a planned primary handoff does not abort the whole load stage.
  - After switchover the former primary automatically rejoins as a standby.
    The watch loop shows it recovering — final state should show both members healthy.
EOF
}

print_stage_list() {
	cat <<'EOF'
prepare              [build/runtime]
bootstrap            [ansible/bootstrap]
probes               [Patroni-compatible API]
cluster              [PACMAN-native API]
members              [PACMAN-native API]
postgres-config      [PACMAN-native API + DCS]
metrics              [Patroni-compatible API]
verify               [mixed: Patroni-compatible + PACMAN-native]
maintenance-enable   [PACMAN-native API]
maintenance-disable  [PACMAN-native API]
cancel-switchover    [PACMAN-native API]
switchover           [PACMAN-native API]
watch-members        [PACMAN-native API]
history              [PACMAN-native API]
pgbench-init         [PostgreSQL client]
load-on              [PostgreSQL client]
load-off             [PostgreSQL client]
pgbench-stats        [pgbench log]
full-demo            [mixed]
destroy              [docker lifecycle]
reset                [docker lifecycle]
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

require_etcdctl() {
	if "${dry_run}"; then
		return 0
	fi

	compose_exec pacman-dcs command -v etcdctl >/dev/null
}

require_python() {
	local service=$1

	if "${dry_run}"; then
		return 0
	fi

	compose_exec "${service}" command -v python3 >/dev/null
}

require_pg_isready() {
	local service=$1

	if "${dry_run}"; then
		return 0
	fi

	compose_exec "${service}" command -v /usr/pgsql-17/bin/pg_isready >/dev/null
}

ensure_container_api_url() {
	local url=$1
	local variable_name=$2
	local service_name=$3

	case "${url}" in
		http://127.0.0.1:*|https://127.0.0.1:*|http://localhost:*|https://localhost:*)
			printf '%s=%s points at container loopback.\n' "${variable_name}" "${url}" >&2
			printf 'demo stages run inside docker compose containers, so use a service URL such as http://%s:8080.\n' "${service_name}" >&2
			printf 'host-published ports remain available on http://127.0.0.1:8081 and http://127.0.0.1:8082 outside the containers.\n' >&2
			exit 1
			;;
	esac
}

python_get_json() {
	local service=$1
	local url=$2

	compose_exec "${service}" python3 -c \
		"import json, sys, urllib.error, urllib.request
req = urllib.request.Request('${url}', headers={'Accept': 'application/json', 'Authorization': 'Bearer ${api_token}'})
try:
    response = urllib.request.urlopen(req, timeout=5)
    status = response.status
except urllib.error.HTTPError as exc:
    response = exc
    status = exc.code
except urllib.error.URLError as exc:
    print(f'failed to fetch ${url} from ${service}: {exc}', file=sys.stderr)
    sys.exit(1)
body = json.load(response)
print(f'HTTP {status}')
print(json.dumps(body, indent=2, sort_keys=True))"
}

show_probe() {
	local service=$1
	local url=$2
	local label=$3

	require_python "${service}"
	log "${label}"

	python_get_json "${service}" "${url}"
}

show_vip_assignment() {
	local service=$1

	log "${service} VIP assignment [vip-manager PostgreSQL VIP]"
	compose_shell "${service}" "ip -brief addr show dev '${vip_interface}'"
}

show_vip_postgres_route() {
	require_pg_isready "${primary_service}"
	log "writable PostgreSQL VIP ${rw_host}:${pgbench_port} [vip-manager PostgreSQL VIP]"
	compose_shell "${primary_service}" \
		"/usr/pgsql-17/bin/pg_isready -h '${rw_host}' -p '${pgbench_port}' -d '${pgbench_db}'"
}

wait_for_vip_postgres_route() {
	require_pg_isready "${primary_service}"
	compose_exec "${primary_service}" /bin/sh -lc "
		deadline=\$(( \$(date +%s) + 90 ))
		until /usr/pgsql-17/bin/pg_isready -h '${rw_host}' -p '${pgbench_port}' -d '${pgbench_db}' >/dev/null 2>&1; do
			if [ \$(date +%s) -ge \${deadline} ]; then
				echo 'timed out waiting for writable PostgreSQL VIP ${rw_host}:${pgbench_port}' >&2
				exit 1
			fi
			sleep 1
		done
	"
}

run_pacmanctl() {
	local service=$1
	shift

	require_pacmanctl
	ensure_container_api_url "${primary_api_url}" "PACMAN_DEMO_PRIMARY_API_URL" "${primary_service}"
	compose_exec "${service}" \
		env \
			PACMANCTL_API_URL="${primary_api_url}" \
			PACMANCTL_API_TOKEN="${api_token}" \
			pacmanctl "$@"
}

show_desired_postgres_parameter() {
	local service=$1
	local parameter=$2

	require_python "${service}"
	ensure_container_api_url "${primary_api_url}" "PACMAN_DEMO_PRIMARY_API_URL" "${primary_service}"
	compose_exec "${service}" python3 - "${parameter}" "${primary_api_url}/api/v1/cluster/spec" "${api_token}" <<'PY'
import json
import sys
import urllib.request

parameter, url, token = sys.argv[1:4]
request = urllib.request.Request(
    url,
    headers={
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    },
)
with urllib.request.urlopen(request, timeout=5) as response:
    spec = json.load(response)

parameters = ((spec.get("postgres") or {}).get("parameters") or {})
summary = {
    "clusterName": spec.get("clusterName"),
    "generation": spec.get("generation"),
    "parameter": parameter,
    "value": parameters.get(parameter, "<unset>"),
}
print(json.dumps(summary, indent=2, sort_keys=True))
PY
}

update_desired_postgres_parameter() {
	local parameter=$1
	local value=$2

	require_etcdctl
	require_python pacman-dcs
	ensure_container_api_url "${primary_api_url}" "PACMAN_DEMO_PRIMARY_API_URL" "${primary_service}"
	compose_exec pacman-dcs python3 - "${parameter}" "${value}" "${primary_api_url}/api/v1/cluster/spec" "${api_token}" <<'PY'
import base64
import json
import os
import subprocess
import sys
import urllib.request

parameter, value, url, token = sys.argv[1:5]
request = urllib.request.Request(
    url,
    headers={
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    },
)
with urllib.request.urlopen(request, timeout=5) as response:
    api_spec = json.load(response)

cluster_name = str(api_spec.get("clusterName") or "").strip()
if not cluster_name:
    raise SystemExit("cluster spec did not include clusterName")

env = dict(os.environ)
env["ETCDCTL_API"] = "3"
key = f"/pacman/{cluster_name}/config"
current = subprocess.run(
    [
        "etcdctl",
        "--endpoints=http://127.0.0.1:2379",
        "get",
        key,
        "-w",
        "json",
    ],
    check=True,
    capture_output=True,
    env=env,
    text=True,
)
response = json.loads(current.stdout)
kvs = response.get("kvs") or []
if not kvs:
    raise SystemExit(f"cluster spec key {key} is missing")

raw_etcd_value = base64.b64decode(kvs[0]["value"])
stored = json.loads(raw_etcd_value)
ttl_nanos = 0

if isinstance(stored, dict) and "value" in stored and "revision" in stored:
    spec_bytes = base64.b64decode(stored["value"])
    spec = json.loads(spec_bytes)
    internal_revision = int(stored.get("revision") or 0)
    ttl_nanos = int(stored.get("ttlNanos") or 0)
else:
    spec = stored
    internal_revision = int((spec or {}).get("generation") or 0)

postgres = spec.setdefault("postgres", {})
parameters = postgres.setdefault("parameters", {})
previous = parameters.get(parameter, "<unset>")
generation_before = int(spec.get("generation") or 0)

if previous == value:
    action = "unchanged"
    generation_after = generation_before
else:
    action = "updated"
    parameters[parameter] = value
    generation_after = generation_before + 1
    spec["generation"] = generation_after
    spec_payload = json.dumps(spec, separators=(",", ":"), sort_keys=True).encode()
    envelope = {
        "value": base64.b64encode(spec_payload).decode("ascii"),
        "revision": internal_revision + 1,
    }
    if ttl_nanos > 0:
        envelope["ttlNanos"] = ttl_nanos
    payload = json.dumps(envelope, separators=(",", ":"), sort_keys=True)
    subprocess.run(
        [
            "etcdctl",
            "--endpoints=http://127.0.0.1:2379",
            "put",
            key,
            payload,
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

summary = {
    "action": action,
    "clusterName": cluster_name,
    "generationAfter": generation_after,
    "generationBefore": generation_before,
    "parameter": parameter,
    "previousValue": previous,
    "value": value,
}
print(json.dumps(summary, indent=2, sort_keys=True))
PY
}

wait_for_desired_postgres_parameter() {
	local parameter=$1
	local value=$2

	require_python "${primary_service}"
	ensure_container_api_url "${primary_api_url}" "PACMAN_DEMO_PRIMARY_API_URL" "${primary_service}"
	compose_exec "${primary_service}" python3 - "${parameter}" "${value}" "${primary_api_url}/api/v1/cluster/spec" "${api_token}" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

parameter, expected, url, token = sys.argv[1:5]
deadline = time.time() + 20
request = urllib.request.Request(
    url,
    headers={
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    },
)

while True:
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            spec = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 503 and time.time() < deadline:
            time.sleep(1)
            continue
        raise
    except urllib.error.URLError:
        if time.time() < deadline:
            time.sleep(1)
            continue
        raise

    parameters = ((spec.get("postgres") or {}).get("parameters") or {})
    current = parameters.get(parameter, "<unset>")
    if current == expected:
        summary = {
            "clusterName": spec.get("clusterName"),
            "generation": spec.get("generation"),
            "parameter": parameter,
            "value": current,
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
        break

    if time.time() >= deadline:
        raise SystemExit(
            f"timed out waiting for {parameter}={expected}; current value is {current!r}"
        )

    time.sleep(1)
PY
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
	ensure_container_api_url "${primary_api_url}" "PACMAN_DEMO_PRIMARY_API_URL" "${primary_service}"
	ensure_container_api_url "${replica_api_url}" "PACMAN_DEMO_REPLICA_API_URL" "${replica_service}"
	show_probe "${primary_service}" "${primary_api_url}/health" "primary /health [Patroni-compatible API]"
	show_probe "${replica_service}" "${replica_api_url}/health" "replica /health [Patroni-compatible API]"
	show_probe "${primary_service}" "${primary_api_url}/primary" "primary /primary [Patroni-compatible API]"
	show_probe "${replica_service}" "${replica_api_url}/primary" "replica /primary [Patroni-compatible API]"
	show_vip_assignment "${primary_service}"
	show_vip_assignment "${replica_service}"
	show_vip_postgres_route
}

stage_cluster() {
	log "cluster status via pacmanctl in ${primary_service} [PACMAN-native API]"
	run_pacmanctl "${primary_service}" cluster status
}

stage_members() {
	log "members via pacmanctl in ${primary_service} [PACMAN-native API]"
	run_pacmanctl "${primary_service}" members list
}

stage_postgres_config() {
	local parameter=${1:-${postgres_config_parameter}}
	local value=${2:-${postgres_config_value}}

	if "${dry_run}"; then
		log "show desired PostgreSQL parameter before change [PACMAN-native API]"
		render_command docker compose -f "${compose_file}" exec -T "${primary_service}" \
			env PACMANCTL_API_URL="${primary_api_url}" PACMANCTL_API_TOKEN="${api_token}" \
			pacmanctl cluster spec show -o json
		log "update desired PostgreSQL parameter ${parameter}=${value} in DCS [PACMAN-native API + DCS]"
		render_command docker compose -f "${compose_file}" exec -T pacman-dcs \
			env ETCDCTL_API=3 etcdctl put "/pacman/<cluster>/config" "<updated-cluster-spec-json>"
		log "wait for desired PostgreSQL parameter to appear in cluster spec [PACMAN-native API]"
		render_command docker compose -f "${compose_file}" exec -T "${primary_service}" \
			env PACMANCTL_API_URL="${primary_api_url}" PACMANCTL_API_TOKEN="${api_token}" \
			pacmanctl cluster spec show -o json
		return 0
	fi

	log "desired PostgreSQL parameter before change [PACMAN-native API]"
	show_desired_postgres_parameter "${primary_service}" "${parameter}"
	log "update desired PostgreSQL parameter ${parameter}=${value} in DCS [PACMAN-native API + DCS]"
	update_desired_postgres_parameter "${parameter}" "${value}"
	log "desired PostgreSQL parameter after change [PACMAN-native API]"
	wait_for_desired_postgres_parameter "${parameter}" "${value}"
}

stage_metrics() {
	ensure_container_api_url "${primary_api_url}" "PACMAN_DEMO_PRIMARY_API_URL" "${primary_service}"
	require_python "${primary_service}"
	log "key prometheus metrics from ${primary_service} [Patroni-compatible API]"

	compose_shell "${primary_service}" \
		"python3 -c \"import urllib.request; print(urllib.request.urlopen('${primary_api_url}/metrics', timeout=5).read().decode(), end='')\" | grep -E '^pacman_cluster_|^pacman_member_info|^pacman_node_info'"
}

stage_verify() {
	stage_probes
	stage_cluster
	stage_members
}

stage_maintenance_enable() {
	log "enable maintenance mode via pacmanctl in ${primary_service} [PACMAN-native API]"
	run_pacmanctl "${primary_service}" cluster maintenance enable -reason demo-maintenance -requested-by demo-script
}

stage_maintenance_disable() {
	log "disable maintenance mode via pacmanctl in ${primary_service} [PACMAN-native API]"
	run_pacmanctl "${primary_service}" cluster maintenance disable -reason demo-maintenance-complete -requested-by demo-script
}

stage_cancel_switchover() {
	log "cancel any pending switchover [PACMAN-native API]"
	require_python "${primary_service}"
	ensure_container_api_url "${primary_api_url}" "PACMAN_DEMO_PRIMARY_API_URL" "${primary_service}"
	compose_exec "${primary_service}" python3 -c \
		"import sys, urllib.request, urllib.error
req = urllib.request.Request('${primary_api_url}/api/v1/operations/switchover', method='DELETE', headers={'Authorization': 'Bearer ${api_token}'})
try:
    urllib.request.urlopen(req, timeout=5)
    print('pending switchover cancelled')
except urllib.error.HTTPError as e:
    if e.code == 404:
        print('no pending switchover to cancel')
    else:
        print(f'cancel returned {e.code}', file=sys.stderr)
except urllib.error.URLError as e:
    print(f'cancel request failed: {e}', file=sys.stderr)" || true
}

stage_switchover() {
	local candidate=${1:-${default_candidate}}
	local pgbench_paused=false

	if pgbench_supervisor_running; then
		log "pause pgbench load at chunk boundary before switchover"
		pause_pgbench_load
		pgbench_paused=true
	fi

	log "request switchover to ${candidate} via pacmanctl in ${primary_service} [PACMAN-native API]"
	if ! run_pacmanctl "${primary_service}" cluster switchover -candidate "${candidate}" -reason demo-switchover -requested-by demo-script -force; then
		if "${pgbench_paused}"; then
			resume_pgbench_load
		fi
		return 1
	fi
	if ! wait_for_member_primary "${candidate}"; then
		if "${pgbench_paused}"; then
			resume_pgbench_load
		fi
		return 1
	fi
	if ! wait_for_vip_postgres_route; then
		if "${pgbench_paused}"; then
			resume_pgbench_load
		fi
		return 1
	fi

	if "${pgbench_paused}"; then
		log "resume pgbench load after switchover"
		resume_pgbench_load
	fi
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
	log "history via pacmanctl in ${primary_service} [PACMAN-native API]"
	run_pacmanctl "${primary_service}" history list
}

resolve_pgbench() {
	# Try PATH first, then the standard PostgreSQL versioned bin directories on
	# EL9 (/usr/pgsql-NN/bin) and Debian (/usr/lib/postgresql/NN/bin).
	compose_exec "${primary_service}" /bin/sh -lc \
		'for candidate in pgbench $(ls /usr/pgsql-*/bin/pgbench /usr/lib/postgresql/*/bin/pgbench 2>/dev/null); do
			command -v "$candidate" >/dev/null 2>&1 && { echo "$candidate"; exit 0; }
			[ -x "$candidate" ] && { echo "$candidate"; exit 0; }
		done; exit 1' 2>/dev/null
}

require_pgbench() {
	if "${dry_run}"; then
		return 0
	fi

	if ! resolve_pgbench >/dev/null; then
		printf 'pgbench not found in %s (checked PATH and /usr/pgsql-*/bin, /usr/lib/postgresql/*/bin)\n' "${primary_service}" >&2
		exit 1
	fi
}

pgbench_bin() {
	if "${dry_run}"; then
		printf 'pgbench'
		return 0
	fi
	resolve_pgbench
}

pgbench_connect_args() {
	printf -- '-h %s -p %s -U %s %s' "${pgbench_host}" "${pgbench_port}" "${pgbench_user}" "${pgbench_db}"
}

pgbench_supervisor_running() {
	compose_exec "${primary_service}" /bin/sh -lc "
		if [ ! -f ${pgbench_pid_file} ]; then
			exit 1
		fi
		pid=\$(cat ${pgbench_pid_file} 2>/dev/null || true)
		[ -n \"\${pid}\" ] && kill -0 \"\${pid}\" 2>/dev/null
	" >/dev/null 2>&1
}

pause_pgbench_load() {
	compose_exec "${primary_service}" /bin/sh -lc "
		touch ${pgbench_pause_file}
		deadline=\$(( \$(date +%s) + ${pgbench_chunk_duration} + 10 ))
		while [ -f ${pgbench_child_pid_file} ]; do
			child=\$(cat ${pgbench_child_pid_file} 2>/dev/null || true)
			if [ -z \"\${child}\" ] || ! kill -0 \"\${child}\" 2>/dev/null; then
				rm -f ${pgbench_child_pid_file}
				break
			fi
			if [ \$(date +%s) -ge \${deadline} ]; then
				echo 'timed out waiting for active pgbench chunk to stop' >&2
				exit 1
			fi
			sleep 1
		done
	"
}

resume_pgbench_load() {
	compose_shell "${primary_service}" "rm -f ${pgbench_pause_file}"
}

wait_for_member_primary() {
	local candidate=$1

	require_python "${primary_service}"
	ensure_container_api_url "${primary_api_url}" "PACMAN_DEMO_PRIMARY_API_URL" "${primary_service}"
	compose_exec "${primary_service}" python3 - "${candidate}" "${primary_api_url}/api/v1/members" "${api_token}" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

candidate, url, token = sys.argv[1:4]
deadline = time.time() + 90
request = urllib.request.Request(
    url,
    headers={
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    },
)

while True:
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = json.load(response)
    except urllib.error.URLError:
        if time.time() < deadline:
            time.sleep(1)
            continue
        raise

    if isinstance(body, dict):
        members = body.get("items") or []
    elif isinstance(body, list):
        members = body
    else:
        members = []

    for member in members:
        if not isinstance(member, dict):
            continue
        if str(member.get("name") or "").strip() != candidate:
            continue
        if (
            str(member.get("role") or "").strip() == "primary"
            and str(member.get("state") or "").strip() == "running"
            and bool(member.get("healthy")) is True
        ):
            print(json.dumps(member, indent=2, sort_keys=True))
            raise SystemExit(0)

    if time.time() >= deadline:
        raise SystemExit(f"timed out waiting for {candidate} to become healthy primary")

    time.sleep(1)
PY
}

stage_pgbench_init() {
	require_pgbench

	log "initialize pgbench schema on ${pgbench_host}:${pgbench_port} (scale=${pgbench_scale}, db=${pgbench_db}) [vip-manager PostgreSQL VIP]"
	# shellcheck disable=SC2046
	compose_exec "${primary_service}" env "PGPASSWORD=${pgbench_password}" /bin/sh -lc \
		"$(pgbench_bin) $(pgbench_connect_args) -i -s ${pgbench_scale} --quiet"
}

stage_load_on() {
	require_pgbench

	log "start pgbench background load against ${pgbench_host}:${pgbench_port}: ${pgbench_clients} clients / ${pgbench_threads} threads / ${pgbench_duration}s [vip-manager PostgreSQL VIP]"
	# --progress-timestamp prints a Unix timestamp on each progress line so
	# the log shows when during the switchover transactions were failing.
	# shellcheck disable=SC2046
	compose_exec "${primary_service}" env "PGPASSWORD=${pgbench_password}" /bin/sh -lc \
		"nohup /bin/sh -lc '
			set -u
			child_pid=
			trap '\''if [ -n \"\${child_pid:-}\" ]; then kill \"\${child_pid}\" 2>/dev/null || true; fi; rm -f ${pgbench_child_pid_file} ${pgbench_pause_file} ${pgbench_pid_file}; exit 0'\'' INT TERM
			end=\$(( \$(date +%s) + ${pgbench_duration} ))
			chunk=${pgbench_chunk_duration}
			: >${pgbench_log_file}
			rm -f ${pgbench_child_pid_file} ${pgbench_pause_file}
			while [ \$(date +%s) -lt \${end} ]; do
				while [ -f ${pgbench_pause_file} ]; do
					sleep 1
				done
				remaining=\$(( end - \$(date +%s) ))
				run_for=\${chunk}
				if [ \${remaining} -lt \${run_for} ]; then
					run_for=\${remaining}
				fi
				if [ \${run_for} -le 0 ]; then
					break
				fi
				status=0
				$(pgbench_bin) $(pgbench_connect_args) \
					-c ${pgbench_clients} \
					-j ${pgbench_threads} \
					-T \${run_for} \
					-P 5 \
					--progress-timestamp \
					>>${pgbench_log_file} 2>&1 &
				child_pid=\$!
				echo \${child_pid} >${pgbench_child_pid_file}
				wait \${child_pid} || status=\$?
				child_pid=
				rm -f ${pgbench_child_pid_file}
				if [ \${status} -ne 0 ]; then
					echo \"pgbench chunk exited with status \${status}; retrying after switchover\" >>${pgbench_log_file}
					sleep 1
				fi
			done
			rm -f ${pgbench_child_pid_file} ${pgbench_pause_file} ${pgbench_pid_file}
		' >/dev/null 2>&1 </dev/null & echo \$! >${pgbench_pid_file}
		echo \"pgbench supervisor started (pid=\$(cat ${pgbench_pid_file}))\""
}

stage_load_off() {
	log "stop pgbench load"
	compose_shell "${primary_service}" \
		"if [ -f ${pgbench_pid_file} ]; then
			pid=\$(cat ${pgbench_pid_file})
			if [ -f ${pgbench_child_pid_file} ]; then
				child=\$(cat ${pgbench_child_pid_file} 2>/dev/null || true)
				[ -n \"\${child}\" ] && kill \"\${child}\" 2>/dev/null || true
			fi
			kill \"\${pid}\" 2>/dev/null && echo \"pgbench pid \${pid} stopped\" || echo \"pgbench already exited\"
			rm -f ${pgbench_pid_file} ${pgbench_child_pid_file} ${pgbench_pause_file}
		else
			echo \"no running pgbench (${pgbench_pid_file} not found)\"
		fi"
}

stage_pgbench_stats() {
	log "pgbench progress log (last 30 lines)"
	compose_shell "${primary_service}" \
		"if [ -f ${pgbench_log_file} ]; then
			tail -n 30 ${pgbench_log_file}
		else
			echo \"no pgbench log at ${pgbench_log_file}\"
		fi"
}

stage_full_demo() {
	stage_prepare
	stage_bootstrap
	stage_verify
	stage_metrics
	stage_maintenance_enable
	stage_maintenance_disable
	stage_switchover
	stage_watch_members 15
	stage_history
	stage_probes
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
		postgres-config)
			stage_postgres_config "$@"
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
		cancel-switchover)
			stage_cancel_switchover "$@"
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
		pgbench-init)
			stage_pgbench_init "$@"
			;;
		load-on)
			stage_load_on "$@"
			;;
		load-off)
			stage_load_off "$@"
			;;
		pgbench-stats)
			stage_pgbench_stats "$@"
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

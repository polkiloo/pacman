iptables_partition() {
  local service=$1
  shift
  local peer peer_ip

  for peer in "$@"; do
    peer_ip=$(service_ip "${peer}")
    compose_exec "${service}" /bin/sh -lc \
      "iptables -I INPUT -s '${peer_ip}' -j DROP; iptables -I OUTPUT -d '${peer_ip}' -j DROP"
  done
}

iptables_heal() {
  local service=$1
  shift
  local peer peer_ip

  for peer in "$@"; do
    peer_ip=$(service_ip "${peer}")
    compose_exec "${service}" /bin/sh -lc \
      "while iptables -D INPUT -s '${peer_ip}' -j DROP 2>/dev/null; do :; done; while iptables -D OUTPUT -d '${peer_ip}' -j DROP 2>/dev/null; do :; done"
  done
}

iptables_replication_partition() {
  local service=$1
  shift
  local peer peer_ip

  for peer in "$@"; do
    peer_ip=$(service_ip "${peer}")
    compose_exec "${service}" /bin/sh -lc \
      "iptables -I INPUT -s '${peer_ip}' -p tcp --dport 5432 -j DROP; iptables -I OUTPUT -d '${peer_ip}' -p tcp --sport 5432 -j DROP"
  done
}

iptables_replication_heal() {
  local service=$1
  shift
  local peer peer_ip

  for peer in "$@"; do
    peer_ip=$(service_ip "${peer}")
    compose_exec "${service}" /bin/sh -lc \
      "while iptables -D INPUT -s '${peer_ip}' -p tcp --dport 5432 -j DROP 2>/dev/null; do :; done; while iptables -D OUTPUT -d '${peer_ip}' -p tcp --sport 5432 -j DROP 2>/dev/null; do :; done"
  done
}

record_client_traffic_probe() {
  local case_dir=$1
  local nemesis=$2
  local probe_id=$3
  local sample_file="${case_dir}/client-traffic-during-nemesis.jsonl"
  local observed_at output status

  observed_at="$(timestamp_utc)"
  status=0
  output=$(psql_vip "
INSERT INTO jepsen.client_availability_probes(probe_id, nemesis)
VALUES ($(sql_literal "${probe_id}"), $(sql_literal "${nemesis}"))
ON CONFLICT (probe_id) DO NOTHING;
" 2>&1) || status=$?

  if [[ "${status}" -eq 0 ]]; then
    append_jsonl "${sample_file}" \
      observedAt "$(json_escape "${observed_at}")" \
      nemesis "$(json_escape "${nemesis}")" \
      probeId "$(json_escape "${probe_id}")" \
      ok true \
      error "$(json_escape "")"
    return 0
  fi

  append_jsonl "${sample_file}" \
    observedAt "$(json_escape "${observed_at}")" \
    nemesis "$(json_escape "${nemesis}")" \
    probeId "$(json_escape "${probe_id}")" \
    ok false \
    error "$(json_escape "${output}")"
  return 1
}

record_replication_health_probe() {
  local service=$1
  local case_dir=$2
  local nemesis=$3
  local sample_file="${case_dir}/replication-traffic-during-nemesis.jsonl"
  local observed_at output status rows_json streaming_replicas

  observed_at="$(timestamp_utc)"
  status=0
  output=$(psql_service "${service}" "
SELECT coalesce(json_agg(json_build_object(
  'applicationName', application_name,
  'clientAddr', client_addr::text,
  'state', state,
  'syncState', sync_state,
  'writeLsn', write_lsn::text,
  'flushLsn', flush_lsn::text,
  'replayLsn', replay_lsn::text
) ORDER BY application_name), '[]'::json)
FROM pg_stat_replication;
" 2>&1) || status=$?

  if [[ "${status}" -eq 0 ]] && rows_json=$(printf '%s\n' "${output}" | jq -c . 2>/dev/null); then
    streaming_replicas=$(printf '%s\n' "${rows_json}" | jq '[.[] | select(.state == "streaming")] | length')
    append_jsonl "${sample_file}" \
      observedAt "$(json_escape "${observed_at}")" \
      nemesis "$(json_escape "${nemesis}")" \
      service "$(json_escape "${service}")" \
      ok true \
      streamingReplicas "${streaming_replicas}" \
      rows "${rows_json}" \
      error "$(json_escape "")"
    return 0
  fi

  append_jsonl "${sample_file}" \
    observedAt "$(json_escape "${observed_at}")" \
    nemesis "$(json_escape "${nemesis}")" \
    service "$(json_escape "${service}")" \
    ok false \
    streamingReplicas 0 \
    rows "[]" \
    error "$(json_escape "${output}")"
  return 1
}

record_dcs_traffic_probe() {
  local service=$1
  local case_dir=$2
  local nemesis=$3
  local sample_file="${case_dir}/dcs-traffic-during-nemesis.jsonl"
  local endpoints observed_at output status

  endpoints=$(dcs_client_endpoints)

  observed_at="$(timestamp_utc)"
  status=0
  output=$(compose_exec "${service}" python3 -c \
    "import json, sys, urllib.request
endpoints = sys.argv[1].split(',')
results = []
ok = True
for endpoint in endpoints:
    try:
        body = urllib.request.urlopen(endpoint + '/health', timeout=3).read().decode()
        results.append({'endpoint': endpoint, 'ok': True, 'body': body})
    except Exception as exc:
        ok = False
        results.append({'endpoint': endpoint, 'ok': False, 'error': str(exc)})
print(json.dumps(results, sort_keys=True))
raise SystemExit(0 if ok else 1)" "${endpoints}" 2>&1) || status=$?

  if [[ "${status}" -eq 0 ]]; then
    append_jsonl "${sample_file}" \
      observedAt "$(json_escape "${observed_at}")" \
      nemesis "$(json_escape "${nemesis}")" \
      service "$(json_escape "${service}")" \
      ok true \
      output "$(json_escape "${output}")" \
      endpoints "$(json_escape "${endpoints}")" \
      error "$(json_escape "")"
    return 0
  fi

  append_jsonl "${sample_file}" \
    observedAt "$(json_escape "${observed_at}")" \
    nemesis "$(json_escape "${nemesis}")" \
    service "$(json_escape "${service}")" \
    ok false \
    output "$(json_escape "")" \
    endpoints "$(json_escape "${endpoints}")" \
    error "$(json_escape "${output}")"
  return 1
}

dcs_quorum_health_json() {
  local observer_service=${1:-pacman-primary}
  local endpoints
  endpoints=$(dcs_client_endpoints)

  compose_exec "${observer_service}" python3 -c \
    "import json, sys, urllib.request
endpoints = sys.argv[1].split(',')
results = []
healthy = 0
for endpoint in endpoints:
    try:
        body = urllib.request.urlopen(endpoint + '/health', timeout=3).read().decode()
        results.append({'endpoint': endpoint, 'ok': True, 'body': body})
        healthy += 1
    except Exception as exc:
        results.append({'endpoint': endpoint, 'ok': False, 'error': str(exc)})
print(json.dumps({
    'totalEndpoints': len(endpoints),
    'healthyEndpoints': healthy,
    'failedEndpoints': len(endpoints) - healthy,
    'endpoints': results,
}, sort_keys=True))" "${endpoints}"
}

dcs_member_running() {
  local service=$1
  local member

  member=$(dcs_member_for_service "${service}") || return 1
  compose_exec "${service}" pgrep -f "/usr/bin/[e]tcd .*--name ${member}" >/dev/null 2>&1
}

record_dcs_quorum_probe() {
  local case_dir=$1
  local nemesis=$2
  local phase=$3
  local target_services=$4
  local observer_service=${5:-pacman-primary}
  local sample_file="${case_dir}/dcs-quorum-during-nemesis.jsonl"
  local target_service target_member target_members observed_at output status target_running total healthy failed target_count running_targets

  target_members=""
  target_count=0
  running_targets=0
  target_running=true
  for target_service in ${target_services}; do
    target_member=$(dcs_member_for_service "${target_service}" 2>/dev/null || printf 'unknown')
    if [[ -n "${target_members}" ]]; then
      target_members="${target_members} "
    fi
    target_members="${target_members}${target_member}"
    target_count=$((target_count + 1))
    if dcs_member_running "${target_service}"; then
      running_targets=$((running_targets + 1))
    else
      target_running=false
    fi
  done
  if [[ "${target_count}" -eq 0 ]]; then
    target_running=false
  fi

  observed_at="$(timestamp_utc)"

  status=0
  output=$(dcs_quorum_health_json "${observer_service}" 2>&1) || status=$?
  if [[ "${status}" -eq 0 ]] && printf '%s\n' "${output}" | jq -e . >/dev/null 2>&1; then
    total=$(printf '%s\n' "${output}" | jq -r '.totalEndpoints // 0')
    healthy=$(printf '%s\n' "${output}" | jq -r '.healthyEndpoints // 0')
    failed=$(printf '%s\n' "${output}" | jq -r '.failedEndpoints // 0')
    append_jsonl "${sample_file}" \
      observedAt "$(json_escape "${observed_at}")" \
      nemesis "$(json_escape "${nemesis}")" \
      phase "$(json_escape "${phase}")" \
      observerService "$(json_escape "${observer_service}")" \
      targetService "$(json_escape "${target_services}")" \
      targetMember "$(json_escape "${target_members}")" \
      targetCount "${target_count}" \
      runningTargets "${running_targets}" \
      targetRunning "${target_running}" \
      ok true \
      totalEndpoints "${total}" \
      healthyEndpoints "${healthy}" \
      failedEndpoints "${failed}" \
      health "${output}" \
      error "$(json_escape "")"
    return 0
  fi

  append_jsonl "${sample_file}" \
    observedAt "$(json_escape "${observed_at}")" \
    nemesis "$(json_escape "${nemesis}")" \
    phase "$(json_escape "${phase}")" \
    observerService "$(json_escape "${observer_service}")" \
    targetService "$(json_escape "${target_services}")" \
    targetMember "$(json_escape "${target_members}")" \
    targetCount "${target_count}" \
    runningTargets "${running_targets}" \
    targetRunning "${target_running}" \
    ok false \
    totalEndpoints 0 \
    healthyEndpoints 0 \
    failedEndpoints 0 \
    health null \
    error "$(json_escape "${output}")"
  return 1
}

stop_dcs_member() {
  local service=$1
  local member

  member=$(dcs_member_for_service "${service}")
  compose_exec "${service}" /bin/sh -lc \
    "pkill -TERM -f '/usr/bin/[e]tcd .*--name ${member}' 2>/dev/null || true"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 20 )); while pgrep -f '/usr/bin/[e]tcd .*--name ${member}' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for ${member} to stop' >&2; exit 1; fi; sleep 1; done"
}

start_dcs_member() {
  local service=$1
  local member initial_cluster

  member=$(dcs_member_for_service "${service}")
  initial_cluster=$(dcs_initial_cluster)
  if dcs_member_running "${service}"; then
    return 0
  fi

  compose_exec "${service}" /bin/bash -lc \
    "nohup /usr/bin/etcd \
      --name ${member} \
      --data-dir /var/lib/etcd/pacman \
      --listen-client-urls http://0.0.0.0:2379 \
      --advertise-client-urls http://${service}:2379 \
      --listen-peer-urls http://0.0.0.0:2380 \
      --initial-advertise-peer-urls http://${service}:2380 \
      --initial-cluster ${initial_cluster} \
      --initial-cluster-state existing \
      --initial-cluster-token pacman-cluster \
      >>/var/log/etcd.log 2>&1 &"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 20 )); while ! pgrep -f '/usr/bin/[e]tcd .*--name ${member}' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for ${member} to start' >&2; cat /var/log/etcd.log 2>/dev/null || true; exit 1; fi; sleep 1; done"
}

wait_for_dcs_healthy_count() {
  local expected=$1
  local timeout=${2:-60}
  local observer_service=${3:-pacman-primary}
  local deadline=$((SECONDS + timeout))
  local output healthy

  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    output=$(dcs_quorum_health_json "${observer_service}" 2>/dev/null || true)
    healthy=$(printf '%s\n' "${output}" | jq -r '.healthyEndpoints // 0' 2>/dev/null || printf '0')
    if [[ "${healthy}" -ge "${expected}" ]]; then
      return 0
    fi
    sleep 2
  done

  printf 'timed out waiting for %s healthy DCS endpoints; last=%s\n' "${expected}" "${output:-}" >&2
  return 1
}

wait_for_dcs_healthy_at_most() {
  local expected=$1
  local timeout=${2:-60}
  local observer_service=${3:-pacman-primary}
  local deadline=$((SECONDS + timeout))
  local output healthy

  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    output=$(dcs_quorum_health_json "${observer_service}" 2>/dev/null || true)
    healthy=$(printf '%s\n' "${output}" | jq -r '.healthyEndpoints // 0' 2>/dev/null || printf '0')
    if [[ "${healthy}" -le "${expected}" ]]; then
      return 0
    fi
    sleep 2
  done

  printf 'timed out waiting for at most %s healthy DCS endpoints; last=%s\n' "${expected}" "${output:-}" >&2
  return 1
}

start_pacman_node_runtime() {
  local service=$1

  compose_exec "${service}" /bin/sh -lc \
    "mkdir -p /var/log/pacman; cd /var/lib/pacman && nohup runuser -u postgres -- /bin/bash -lc '. /etc/sysconfig/pacmand 2>/dev/null || true; export PACMAND_CONFIG PACMAND_EXTRA_ARGS PGPASSWORD; cd /var/lib/pacman && exec /usr/bin/pacmand -config \"\${PACMAND_CONFIG:-/etc/pacman/pacmand.yaml}\" \${PACMAND_EXTRA_ARGS:-}' >>/var/log/pacman/pacmand.log 2>&1 &"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 30 )); while ! pgrep -u postgres -f '/usr/bin/[p]acmand -config /etc/pacman/pacmand.yaml' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for pacmand to start' >&2; cat /var/log/pacman/pacmand.log 2>/dev/null || true; exit 1; fi; sleep 1; done"
  compose_exec "${service}" /bin/sh -lc \
    "nohup /usr/local/bin/vip-manager --config /etc/pacman/vip-manager.yml </dev/null >>/var/log/pacman/vip-manager.log 2>&1 &"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 30 )); while ! pgrep -f '/usr/local/bin/[v]ip-manager --config /etc/pacman/vip-manager.yml' >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for vip-manager to start' >&2; cat /var/log/pacman/vip-manager.log 2>/dev/null || true; exit 1; fi; sleep 1; done"
}

stop_pacman_node_runtime() {
  local service=$1

  compose_exec "${service}" /bin/sh -lc \
    "pids=\$(pgrep -f '/usr/local/bin/[v]ip-manager --config /etc/pacman/vip-manager.yml' 2>/dev/null || true); if [ -n \"\${pids}\" ]; then kill \${pids}; fi"
  compose_exec "${service}" /bin/sh -lc \
    "ip addr del '${jepsen_pg_host}/24' dev '${jepsen_vip_interface}' 2>/dev/null || true"
  compose_exec "${service}" /bin/sh -lc \
    "pkill -u postgres -f '/usr/bin/[p]acmand -config /etc/pacman/pacmand.yaml' 2>/dev/null || true"
  compose_exec "${service}" /bin/sh -lc \
    "runuser -u postgres -- /usr/pgsql-17/bin/pg_ctl -D /var/lib/pgsql/17/data -m immediate stop || true"
  compose_exec "${service}" /bin/sh -lc \
    "deadline=\$(( \$(date +%s) + 20 )); while pgrep -u postgres -f '/usr/bin/[p]acmand -config /etc/pacman/pacmand.yaml' >/dev/null 2>&1 || /usr/pgsql-17/bin/pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; do if [ \$(date +%s) -ge \${deadline} ]; then echo 'timed out waiting for node runtime to stop' >&2; ps -ef | grep -E '[p]acmand|[p]ostgres' || true; exit 1; fi; sleep 1; done"
}

slow_network_on() {
  local service=$1

  compose_exec "${service}" /bin/sh -lc \
    "tc_bin=\$(command -v tc || command -v /usr/sbin/tc || true); if [ -z \"\${tc_bin}\" ]; then echo 'tc command not found' >&2; exit 127; fi; \"\${tc_bin}\" qdisc replace dev eth0 root netem delay 250ms 50ms loss 2%"
}

slow_network_off() {
  local service=$1

  compose_exec "${service}" /bin/sh -lc \
    "tc_bin=\$(command -v tc || command -v /usr/sbin/tc || true); if [ -n \"\${tc_bin}\" ]; then \"\${tc_bin}\" qdisc del dev eth0 root 2>/dev/null || true; fi"
}

stop_postgres() {
  local service=$1

  compose_exec "${service}" /bin/sh -lc \
    "runuser -u postgres -- /usr/pgsql-17/bin/pg_ctl -D /var/lib/pgsql/17/data -m immediate stop || true"
}

start_postgres() {
  local service=$1

  compose_exec "${service}" /bin/sh -lc \
    "runuser -u postgres -- /usr/pgsql-17/bin/pg_ctl -D /var/lib/pgsql/17/data -w start -l /var/lib/pgsql/17/data/log/jepsen-restart.log || true"
}

run_nemesis_profile() {
  local profile=$1
  local run_dir=$2
  local schedule_file=$3
  local __pid_var=$4
  local duration=${5:-${jepsen_default_duration}}

  case "${profile}" in
    none)
      printf '{:time "%s" :nemesis :none :action :start}\n' "$(timestamp_utc)" >>"${schedule_file}"
      capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "" || true
      printf '{:time "%s" :nemesis :none :action :stop}\n' "$(timestamp_utc)" >>"${schedule_file}"
      capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "" || true
      printf -v "${__pid_var}" '%s' ''
      return 0
      ;;
  esac

  (
    sleep $(( duration / 3 > 1 ? duration / 3 : 1 ))
    local member service peer_services
    member=$(current_primary_name 2>/dev/null || true)
    service=$(service_for_member "${member}" 2>/dev/null || printf 'pacman-primary')
    peer_services=$(peer_service_for_member "${member}" 2>/dev/null || printf 'pacman-replica pacman-replica-2')

    case "${profile}" in
      kill)
        local promoted_after_kill
        printf '{:time "%s" :nemesis :kill :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        stop_pacman_node_runtime "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        promoted_after_kill=$(wait_for_current_primary_not "${member:-}" 90 2>>"${run_dir}/nemesis.log" || true)
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        start_pacman_node_runtime "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :kill :action :stop :target "%s" :promoted "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" "${promoted_after_kill:-unknown}" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        ;;
      switchover)
        local candidate candidate_service output switchover_status requested_at
        candidate=$(switchover_candidate_name 2>/dev/null || true)
        candidate_service=$(service_for_member "${candidate}" 2>/dev/null || printf '')
        requested_at="$(timestamp_utc)"
        switchover_status=0

        printf '{:time "%s" :nemesis :switchover :action :start :source "%s" :target "%s"}\n' \
          "${requested_at}" "${member:-unknown}" "${candidate:-unknown}" >>"${schedule_file}"

        if [[ -z "${candidate}" ]]; then
          switchover_status=2
          output="no healthy non-primary switchover candidate found"
        else
          output=$(request_manual_switchover "${candidate}" "${service}" 2>&1) || switchover_status=$?
        fi

        printf '%s\n' "${output}" >>"${run_dir}/nemesis.log"
        jq -n \
          --arg requestedAt "${requested_at}" \
          --arg source "${member:-unknown}" \
          --arg sourceService "${service}" \
          --arg candidate "${candidate}" \
          --arg candidateService "${candidate_service}" \
          --arg controlService "${service}" \
          --arg output "${output}" \
          --argjson exitStatus "${switchover_status}" \
          '{
            requestedAt: $requestedAt,
            source: $source,
            sourceService: $sourceService,
            candidate: $candidate,
            candidateService: $candidateService,
            controlService: $controlService,
            exitStatus: $exitStatus,
            output: $output
          }' >"${run_dir}/manual-switchover.json"

        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${candidate:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        printf '{:time "%s" :nemesis :switchover :action :stop :source "%s" :target "%s" :exit-status %s}\n' \
          "$(timestamp_utc)" "${member:-unknown}" "${candidate:-unknown}" "${switchover_status}" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${candidate:-unknown}" "${candidate_service:-${service}}" || true
        ;;
      packet)
        printf '{:time "%s" :nemesis :packet :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        iptables_partition "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        iptables_heal "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :packet :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        ;;
      packet,kill)
        printf '{:time "%s" :nemesis :packet-kill :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        iptables_partition "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        stop_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        start_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        iptables_heal "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :packet-kill :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        ;;
      primary-dcs-partition)
        local dcs_targets
        dcs_targets=$(dcs_services)
        printf '{:time "%s" :nemesis :primary-dcs-partition :action :start :target "%s" :dcs "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" "${dcs_targets}" >>"${schedule_file}"
        iptables_partition "${service}" ${dcs_targets} >>"${run_dir}/nemesis.log" 2>&1 || true
        record_client_traffic_probe "${run_dir}" "${profile}" "${member:-unknown}-dcs-isolated" >>"${run_dir}/nemesis.log" 2>&1 || true
        record_replication_health_probe "${service}" "${run_dir}" "${profile}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        iptables_heal "${service}" ${dcs_targets} >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :primary-dcs-partition :action :stop :target "%s" :dcs "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" "${dcs_targets}" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        ;;
      primary-replication-partition)
        printf '{:time "%s" :nemesis :primary-replication-partition :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        iptables_replication_partition "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        record_dcs_traffic_probe "${service}" "${run_dir}" "${profile}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        iptables_replication_heal "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :primary-replication-partition :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        ;;
      dcs-kill-one)
        local target_dcs_service target_dcs_member
        target_dcs_service="${jepsen_dcs_kill_service}"
        target_dcs_member=$(dcs_member_for_service "${target_dcs_service}" 2>/dev/null || printf '')
        if [[ -z "${target_dcs_member}" ]]; then
          printf 'unsupported DCS kill target service: %s\n' "${target_dcs_service}" >>"${run_dir}/nemesis.log"
          return 2
        fi

        record_dcs_quorum_probe "${run_dir}" "${profile}" "before-kill" "${target_dcs_service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :dcs-kill-one :action :start :target "%s" :member "%s"}\n' "$(timestamp_utc)" "${target_dcs_service}" "${target_dcs_member}" >>"${schedule_file}"
        stop_dcs_member "${target_dcs_service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        wait_for_dcs_healthy_count 2 30 >>"${run_dir}/nemesis.log" 2>&1 || true
        record_dcs_quorum_probe "${run_dir}" "${profile}" "during-kill" "${target_dcs_service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${target_dcs_member}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        start_dcs_member "${target_dcs_service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        wait_for_dcs_healthy_count 3 60 >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :dcs-kill-one :action :stop :target "%s" :member "%s"}\n' "$(timestamp_utc)" "${target_dcs_service}" "${target_dcs_member}" >>"${schedule_file}"
        record_dcs_quorum_probe "${run_dir}" "${profile}" "after-restart" "${target_dcs_service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${target_dcs_member}" "${service}" || true
        ;;
      dcs-lose-majority)
        local target_dcs_services target_dcs_service target_dcs_members target_dcs_member
        target_dcs_services="${jepsen_dcs_majority_kill_services}"
        target_dcs_members=""

        for target_dcs_service in ${target_dcs_services}; do
          target_dcs_member=$(dcs_member_for_service "${target_dcs_service}" 2>/dev/null || printf '')
          if [[ -z "${target_dcs_member}" ]]; then
            printf 'unsupported DCS majority-loss target service: %s\n' "${target_dcs_service}" >>"${run_dir}/nemesis.log"
            return 2
          fi
          if [[ -n "${target_dcs_members}" ]]; then
            target_dcs_members="${target_dcs_members} "
          fi
          target_dcs_members="${target_dcs_members}${target_dcs_member}"
        done

        record_dcs_quorum_probe "${run_dir}" "${profile}" "before-majority-loss" "${target_dcs_services}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :dcs-lose-majority :action :start :targets "%s" :members "%s"}\n' "$(timestamp_utc)" "${target_dcs_services}" "${target_dcs_members}" >>"${schedule_file}"
        for target_dcs_service in ${target_dcs_services}; do
          stop_dcs_member "${target_dcs_service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        done
        wait_for_dcs_healthy_at_most 1 30 >>"${run_dir}/nemesis.log" 2>&1 || true
        record_dcs_quorum_probe "${run_dir}" "${profile}" "during-majority-loss" "${target_dcs_services}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${target_dcs_members}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        for target_dcs_service in ${target_dcs_services}; do
          start_dcs_member "${target_dcs_service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        done
        wait_for_dcs_healthy_count 3 60 >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :dcs-lose-majority :action :stop :targets "%s" :members "%s"}\n' "$(timestamp_utc)" "${target_dcs_services}" "${target_dcs_members}" >>"${schedule_file}"
        record_dcs_quorum_probe "${run_dir}" "${profile}" "after-restart" "${target_dcs_services}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${target_dcs_members}" "${service}" || true
        ;;
      primary-dcs-majority-partition)
        local target_dcs_services target_dcs_service target_dcs_members target_dcs_member
        target_dcs_services="${jepsen_dcs_majority_partition_services}"
        target_dcs_members=""

        for target_dcs_service in ${target_dcs_services}; do
          target_dcs_member=$(dcs_member_for_service "${target_dcs_service}" 2>/dev/null || printf '')
          if [[ -z "${target_dcs_member}" ]]; then
            printf 'unsupported DCS majority-partition target service: %s\n' "${target_dcs_service}" >>"${run_dir}/nemesis.log"
            return 2
          fi
          if [[ -n "${target_dcs_members}" ]]; then
            target_dcs_members="${target_dcs_members} "
          fi
          target_dcs_members="${target_dcs_members}${target_dcs_member}"
        done

        record_dcs_quorum_probe "${run_dir}" "${profile}" "before-primary-majority-partition" "${target_dcs_services}" "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :primary-dcs-majority-partition :action :start :target "%s" :dcs "%s" :members "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" "${target_dcs_services}" "${target_dcs_members}" >>"${schedule_file}"
        iptables_partition "${service}" ${target_dcs_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        wait_for_dcs_healthy_at_most 1 30 "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        record_dcs_quorum_probe "${run_dir}" "${profile}" "during-primary-majority-partition" "${target_dcs_services}" "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        iptables_heal "${service}" ${target_dcs_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        wait_for_dcs_healthy_count 3 60 "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :primary-dcs-majority-partition :action :stop :target "%s" :dcs "%s" :members "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" "${target_dcs_services}" "${target_dcs_members}" >>"${schedule_file}"
        record_dcs_quorum_probe "${run_dir}" "${profile}" "after-primary-majority-partition" "${target_dcs_services}" "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        ;;
      failover-chain)
        local target target_service source source_service output chain_status requested_at step
        step=0
        : >"${run_dir}/failover-chain.jsonl"
        printf '{:time "%s" :nemesis :failover-chain :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"

        for target in alpha-2 alpha-3 alpha-1; do
          wait_for_cluster_switchover_ready 90 >>"${run_dir}/nemesis.log" 2>&1 || true
          source=$(current_primary_name 2>/dev/null || true)
          if [[ -z "${source}" || "${source}" == "${target}" ]]; then
            continue
          fi

          step=$((step + 1))
          target_service=$(service_for_member "${target}" 2>/dev/null || printf '')
          source_service=$(service_for_member "${source}" 2>/dev/null || printf 'pacman-primary')
          requested_at="$(timestamp_utc)"
          chain_status=0

          if ! wait_for_switchover_candidate "${target}" 75; then
            chain_status=2
            output="target ${target} did not become an eligible switchover candidate"
          else
            output=$(request_manual_switchover "${target}" "${source_service}" 2>&1) || chain_status=$?
            if [[ "${chain_status}" -eq 0 ]]; then
              wait_for_current_primary "${target}" 75 >>"${run_dir}/nemesis.log" 2>&1 || chain_status=$?
              wait_for_cluster_switchover_ready 90 >>"${run_dir}/nemesis.log" 2>&1 || chain_status=$?
            fi
          fi

          printf '%s\n' "${output}" >>"${run_dir}/nemesis.log"
          append_jsonl "${run_dir}/failover-chain.jsonl" \
            step "${step}" \
            requestedAt "$(json_escape "${requested_at}")" \
            source "$(json_escape "${source}")" \
            sourceService "$(json_escape "${source_service}")" \
            target "$(json_escape "${target}")" \
            targetService "$(json_escape "${target_service}")" \
            exitStatus "${chain_status}" \
            output "$(json_escape "${output}")"

          printf '{:time "%s" :nemesis :failover-chain :action :step :source "%s" :target "%s" :exit-status %s}\n' \
            "$(timestamp_utc)" "${source}" "${target}" "${chain_status}" >>"${schedule_file}"
          capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${target}" "${target_service:-${source_service}}" || true

          if [[ "${chain_status}" -ne 0 ]]; then
            break
          fi
          sleep 2
        done

        sleep "${jepsen_nemesis_hold_seconds}"
        printf '{:time "%s" :nemesis :failover-chain :action :stop :target "%s"}\n' "$(timestamp_utc)" "$(current_primary_name 2>/dev/null || printf unknown)" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "$(current_primary_name 2>/dev/null || printf unknown)" "" || true
        ;;
      slow-network)
        printf '{:time "%s" :nemesis :slow-network :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        slow_network_on "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        slow_network_off "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :slow-network :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        ;;
      repeated-failure)
        printf '{:time "%s" :nemesis :repeated-failure :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        slow_network_on "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "slow-network" "${member:-unknown}" "${service}" || true
        sleep 3
        slow_network_off "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        iptables_partition "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "packet" "${member:-unknown}" "${service}" || true
        sleep 3
        iptables_heal "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        stop_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "kill" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        start_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :repeated-failure :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        capture_pacman_cluster_snapshot "${run_dir}" "after-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        ;;
      *)
        printf 'unsupported nemesis profile: %s\n' "${profile}" >>"${run_dir}/nemesis.log"
        return 2
        ;;
    esac
  ) >/dev/null 2>>"${run_dir}/nemesis.log" &
  printf -v "${__pid_var}" '%s' "$!"
}

wait_for_nemesis() {
  local pid=$1

  if [[ -n "${pid}" ]]; then
    wait "${pid}" || true
  fi
}

settle_after_nemesis() {
  local profile=$1
  local case_dir=$2

  if [[ "${profile}" == "none" || "${jepsen_post_nemesis_settle_seconds}" -le 0 ]]; then
    return 0
  fi

  printf 'settling for %s seconds after %s nemesis healed\n' \
    "${jepsen_post_nemesis_settle_seconds}" "${profile}" >>"${case_dir}/nemesis.log"
  sleep "${jepsen_post_nemesis_settle_seconds}"
}

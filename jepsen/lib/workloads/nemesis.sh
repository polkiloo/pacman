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
  local observed_at output status

  observed_at="$(timestamp_utc)"
  status=0
  output=$(compose_exec "${service}" python3 -c \
    "import urllib.request; urllib.request.urlopen('http://pacman-dcs:2379/health', timeout=3).read()" 2>&1) || status=$?

  if [[ "${status}" -eq 0 ]]; then
    append_jsonl "${sample_file}" \
      observedAt "$(json_escape "${observed_at}")" \
      nemesis "$(json_escape "${nemesis}")" \
      service "$(json_escape "${service}")" \
      ok true \
      output "$(json_escape "${output}")" \
      error "$(json_escape "")"
    return 0
  fi

  append_jsonl "${sample_file}" \
    observedAt "$(json_escape "${observed_at}")" \
    nemesis "$(json_escape "${nemesis}")" \
    service "$(json_escape "${service}")" \
    ok false \
    output "$(json_escape "")" \
    error "$(json_escape "${output}")"
  return 1
}

slow_network_on() {
  local service=$1

  compose_exec "${service}" /bin/sh -lc \
    "tc qdisc replace dev eth0 root netem delay 250ms 50ms loss 2%"
}

slow_network_off() {
  local service=$1

  compose_exec "${service}" /bin/sh -lc \
    "tc qdisc del dev eth0 root 2>/dev/null || true"
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
        printf '{:time "%s" :nemesis :kill :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        stop_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        start_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :kill :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
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
        printf '{:time "%s" :nemesis :primary-dcs-partition :action :start :target "%s" :dcs "alpha-dcs"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        iptables_partition "${service}" pacman-dcs >>"${run_dir}/nemesis.log" 2>&1 || true
        record_client_traffic_probe "${run_dir}" "${profile}" "${member:-unknown}-dcs-isolated" >>"${run_dir}/nemesis.log" 2>&1 || true
        record_replication_health_probe "${service}" "${run_dir}" "${profile}" >>"${run_dir}/nemesis.log" 2>&1 || true
        capture_pacman_cluster_snapshot "${run_dir}" "during-nemesis" "${profile}" "${member:-unknown}" "${service}" || true
        sleep "${jepsen_nemesis_hold_seconds}"
        iptables_heal "${service}" pacman-dcs >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :primary-dcs-partition :action :stop :target "%s" :dcs "alpha-dcs"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
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

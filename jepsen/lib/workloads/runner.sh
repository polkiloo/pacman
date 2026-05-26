run_workload_profile() {
  local workload=$1
  local run_id=$2
  local case_dir=$3

  case "${workload}" in
    append-smoke | append-failover | append-dcs-quorum)
      run_append_workload "${run_id}" "${case_dir}" "read committed"
      ;;
    append-switchover)
      run_append_workload "${run_id}" "${case_dir}" "read committed" "${jepsen_default_ops}" "${jepsen_default_keys}" "${jepsen_append_switchover_op_delay}"
      ;;
    open-transaction-failover)
      run_open_transaction_failover_workload "${run_id}" "${case_dir}" "read committed"
      ;;
    vip-routing)
      run_vip_routing_workload "${run_id}" "${case_dir}" "read committed"
      ;;
    single-key-register)
      run_register_workload "${run_id}" "${case_dir}" "read committed"
      ;;
    read-committed-txn)
      run_txn_workload "${run_id}" "${case_dir}" "read committed"
      ;;
    serializable-txn)
      run_txn_workload "${run_id}" "${case_dir}" "serializable"
      ;;
    *)
      printf 'unsupported workload profile: %s\n' "${workload}" >&2
      return 2
      ;;
  esac
}

check_workload_profile() {
  local workload=$1
  local run_id=$2
  local case_dir=$3

  case "${workload}" in
    append-smoke | append-failover | append-switchover | append-dcs-quorum | open-transaction-failover | vip-routing)
      check_append_workload "${run_id}" "${case_dir}"
      ;;
    single-key-register)
      check_register_workload "${run_id}" "${case_dir}"
      ;;
    read-committed-txn)
      check_txn_workload "${run_id}" "${case_dir}" "read-committed-txn"
      ;;
    serializable-txn)
      check_txn_workload "${run_id}" "${case_dir}" "serializable-txn"
      ;;
    *)
      return 2
      ;;
  esac
}

run_jepsen_case() {
  local workload=$1
  local nemesis=$2
  local run_dir=$3
  local campaign_history=$4
  local schedule_file=$5
  local case_results=$6
  local slug
  slug="$(case_slug "${workload}__${nemesis}")"
  local case_dir="${run_dir}/cases/${slug}"
  local run_id="${PACMAN_JEPSEN_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}-${slug}"

  mkdir -p "${case_dir}"
  : >"${case_dir}/history.edn"
  : >"${case_dir}/nemesis.log"
  : >"${case_dir}/primary-observations.jsonl"
  : >"${case_dir}/pacman-cluster-snapshots.jsonl"

  write_edn_event "${campaign_history}" "${workload}/${nemesis}" "invoke" "\"${run_id}\""
  write_case_event "${case_dir}/history.edn" ":case" "invoke" "workload" \
    "{:workload \"${workload}\" :nemesis \"${nemesis}\" :run-id \"${run_id}\"}"

  capture_pacman_cluster_snapshot "${case_dir}" "before-nemesis" "${nemesis}" "" || true
  sample_primary_state 0 "${case_dir}/primary-observations.jsonl"

  local primary_sampler_pid=""
  start_primary_sampler "${case_dir}" primary_sampler_pid

  local nemesis_pid=""
  local schedule_start_line
  schedule_start_line=$(wc -l <"${schedule_file}" | tr -d ' ')
  run_nemesis_profile "${nemesis}" "${case_dir}" "${schedule_file}" nemesis_pid "${jepsen_default_duration}" || true

  local workload_status=0
  run_workload_profile "${workload}" "${run_id}" "${case_dir}" || workload_status=$?
  wait_for_nemesis "${nemesis_pid}"
  tail -n +"$((schedule_start_line + 1))" "${schedule_file}" >"${case_dir}/nemesis-schedule.edn"
  settle_after_nemesis "${nemesis}" "${case_dir}"
  stop_primary_sampler "${primary_sampler_pid}"
  capture_pacman_cluster_snapshot "${case_dir}" "after-settle" "${nemesis}" "" || true
  sample_primary_state 1000000000 "${case_dir}/primary-observations.jsonl"
  capture_pg_stat_replication "${case_dir}" "final" || true
  capture_pg_stat_wal_receiver "${case_dir}" "final" || true

  local workload_checker_status=0
  check_workload_profile "${workload}" "${run_id}" "${case_dir}" || workload_checker_status=$?

  local primary_checker_status=0
  check_single_writable_primary "${case_dir}" || primary_checker_status=$?

  local acknowledged_checker_status=0
  check_acknowledged_write_preservation "${workload}" "${run_id}" "${case_dir}" || acknowledged_checker_status=$?

  local timeline_checker_status=0
  check_timeline_convergence "${case_dir}" || timeline_checker_status=$?

  local old_primary_rejoin_checker_status=0
  check_old_primary_rejoin_after_failover "${case_dir}" "${nemesis}" || old_primary_rejoin_checker_status=$?

  local manual_switchover_checker_status=0
  check_manual_switchover "${nemesis}" "${case_dir}" || manual_switchover_checker_status=$?

  local client_traffic_checker_status=0
  check_client_traffic_during_nemesis "${nemesis}" "${case_dir}" || client_traffic_checker_status=$?

  local replication_traffic_checker_status=0
  check_replication_traffic_during_nemesis "${nemesis}" "${case_dir}" || replication_traffic_checker_status=$?

  local dcs_traffic_checker_status=0
  check_dcs_traffic_during_nemesis "${nemesis}" "${case_dir}" || dcs_traffic_checker_status=$?

  local dcs_quorum_checker_status=0
  check_dcs_quorum_during_nemesis "${nemesis}" "${case_dir}" || dcs_quorum_checker_status=$?

  local failover_chain_checker_status=0
  check_failover_chain "${nemesis}" "${case_dir}" || failover_chain_checker_status=$?

  local open_transaction_checker_status=0
  check_open_transaction_during_failover "${workload}" "${case_dir}" || open_transaction_checker_status=$?

  local vip_routing_checker_status=0
  check_vip_write_routing "${workload}" "${nemesis}" "${case_dir}" || vip_routing_checker_status=$?

  local nemesis_schedule_checker_status=0
  go run "${repo_root}/tools/jepsenctl" nemesis validate-schedule \
    --workload "${workload}" \
    --nemesis "${nemesis}" \
    --schedule-file "${case_dir}/nemesis-schedule.edn" \
    >"${case_dir}/nemesis-schedule-checker.log" 2>&1 || nemesis_schedule_checker_status=$?

  if [[ "${workload_status}" -eq 0 && "${workload_checker_status}" -eq 0 && "${primary_checker_status}" -eq 0 && "${acknowledged_checker_status}" -eq 0 && "${timeline_checker_status}" -eq 0 && "${old_primary_rejoin_checker_status}" -eq 0 && "${manual_switchover_checker_status}" -eq 0 && "${client_traffic_checker_status}" -eq 0 && "${replication_traffic_checker_status}" -eq 0 && "${dcs_traffic_checker_status}" -eq 0 && "${dcs_quorum_checker_status}" -eq 0 && "${failover_chain_checker_status}" -eq 0 && "${open_transaction_checker_status}" -eq 0 && "${vip_routing_checker_status}" -eq 0 && "${nemesis_schedule_checker_status}" -eq 0 ]]; then
    write_case_event "${case_dir}/history.edn" ":case" "ok" "workload" \
      "{:workload \"${workload}\" :nemesis \"${nemesis}\" :run-id \"${run_id}\"}"
    cat "${case_dir}/history.edn" >>"${run_dir}/jepsen-history.edn"
    write_edn_event "${campaign_history}" "${workload}/${nemesis}" "ok" "\"${run_id}\""
    record_case_result "${case_results}" "${workload}" "${nemesis}" "true" "checkers passed"
    return 0
  fi

  local failure_details="workload_status=${workload_status} workload_checker_status=${workload_checker_status} primary_checker_status=${primary_checker_status} acknowledged_checker_status=${acknowledged_checker_status} timeline_checker_status=${timeline_checker_status} old_primary_rejoin_checker_status=${old_primary_rejoin_checker_status} manual_switchover_checker_status=${manual_switchover_checker_status} client_traffic_checker_status=${client_traffic_checker_status} replication_traffic_checker_status=${replication_traffic_checker_status} dcs_traffic_checker_status=${dcs_traffic_checker_status} dcs_quorum_checker_status=${dcs_quorum_checker_status} failover_chain_checker_status=${failover_chain_checker_status} open_transaction_checker_status=${open_transaction_checker_status} vip_routing_checker_status=${vip_routing_checker_status} nemesis_schedule_checker_status=${nemesis_schedule_checker_status}"

  write_case_event "${case_dir}/history.edn" ":case" "fail" "workload" \
    "{:workload \"${workload}\" :nemesis \"${nemesis}\" :run-id \"${run_id}\" :workload-status ${workload_status} :workload-checker-status ${workload_checker_status} :primary-checker-status ${primary_checker_status} :acknowledged-checker-status ${acknowledged_checker_status} :timeline-checker-status ${timeline_checker_status} :old-primary-rejoin-checker-status ${old_primary_rejoin_checker_status} :manual-switchover-checker-status ${manual_switchover_checker_status} :client-traffic-checker-status ${client_traffic_checker_status} :replication-traffic-checker-status ${replication_traffic_checker_status} :dcs-traffic-checker-status ${dcs_traffic_checker_status} :dcs-quorum-checker-status ${dcs_quorum_checker_status} :failover-chain-checker-status ${failover_chain_checker_status} :open-transaction-checker-status ${open_transaction_checker_status} :vip-routing-checker-status ${vip_routing_checker_status} :nemesis-schedule-checker-status ${nemesis_schedule_checker_status}}"
  cat "${case_dir}/history.edn" >>"${run_dir}/jepsen-history.edn"
  write_edn_event "${campaign_history}" "${workload}/${nemesis}" "fail" "\"${run_id}\""
  record_case_result "${case_results}" "${workload}" "${nemesis}" "false" "${failure_details}"
  return 1
}

run_jepsen_cases() {
  local cases=$1
  local run_dir=$2
  local campaign_history=$3
  local schedule_file=$4
  local case_results="${run_dir}/case-results.jsonl"

  mkdir -p "${run_dir}/cases"
  : >"${case_results}"
  ensure_workload_schema

  local failed=0
  local spec workload nemesis
  for spec in ${cases}; do
    workload=${spec%%:*}
    nemesis=${spec#*:}
    if [[ "${workload}" == "${nemesis}" ]]; then
      nemesis="none"
    fi

    if ! run_jepsen_case "${workload}" "${nemesis}" "${run_dir}" "${campaign_history}" "${schedule_file}" "${case_results}"; then
      failed=1
    fi
  done

  return "${failed}"
}

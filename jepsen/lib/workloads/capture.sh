capture_pacman_cluster_snapshot() {
  local case_dir=$1
  local phase=$2
  local nemesis=$3
  local target=${4:-}
  local service=${5:-pacman-primary}
  local snapshot_file="${case_dir}/pacman-cluster-snapshots.jsonl"
  local observed_at output snapshot_status cluster_json

  observed_at="$(timestamp_utc)"
  snapshot_status=0
  output=$(compose_exec "${service}" /bin/sh -lc \
    "PACMANCTL_API_URL=http://${service}:8080 PACMANCTL_API_TOKEN=lab-admin-token pacmanctl cluster status -o json" 2>&1) || snapshot_status=$?

  if [[ "${snapshot_status}" -eq 0 ]] && cluster_json=$(printf '%s\n' "${output}" | jq -sc 'map(select(type == "object" and has("clusterName"))) | last // empty' 2>/dev/null) && [[ -n "${cluster_json}" ]]; then
    append_jsonl "${snapshot_file}" \
      observedAt "$(json_escape "${observed_at}")" \
      phase "$(json_escape "${phase}")" \
      nemesis "$(json_escape "${nemesis}")" \
      target "$(json_escape "${target}")" \
      service "$(json_escape "${service}")" \
      ok true \
      cluster "${cluster_json}" \
      error "$(json_escape "")"
    return 0
  fi

  append_jsonl "${snapshot_file}" \
    observedAt "$(json_escape "${observed_at}")" \
    phase "$(json_escape "${phase}")" \
    nemesis "$(json_escape "${nemesis}")" \
    target "$(json_escape "${target}")" \
    service "$(json_escape "${service}")" \
    ok false \
    cluster null \
    error "$(json_escape "${output}")"
  return 1
}

capture_pg_stat_replication() {
  local case_dir=$1
  local phase=${2:-final}
  local output query_status rows_json primary service
  local snapshot_file="${case_dir}/pg-stat-replication.json"

  primary=$(current_primary_name 2>/dev/null || true)
  [[ -n "${primary}" ]] || primary="unknown"
  service=$(service_for_member "${primary}" 2>/dev/null || printf 'pacman-primary')

  query_status=0
  output=$(psql_service "${service}" "
SELECT coalesce(json_agg(json_build_object(
  'pid', pid,
  'usesysid', usesysid,
  'usename', usename,
  'applicationName', application_name,
  'clientAddr', client_addr::text,
  'clientHostname', client_hostname,
  'clientPort', client_port,
  'backendStart', backend_start,
  'backendXmin', backend_xmin,
  'state', state,
  'sentLsn', sent_lsn::text,
  'writeLsn', write_lsn::text,
  'flushLsn', flush_lsn::text,
  'replayLsn', replay_lsn::text,
  'writeLag', write_lag::text,
  'flushLag', flush_lag::text,
  'replayLag', replay_lag::text,
  'syncPriority', sync_priority,
  'syncState', sync_state,
  'replyTime', reply_time
) ORDER BY application_name), '[]'::json)
FROM pg_stat_replication;
" 2>&1) || query_status=$?

  if [[ "${query_status}" -eq 0 ]] && rows_json=$(printf '%s\n' "${output}" | jq -c . 2>/dev/null); then
    jq -n \
      --arg observedAt "$(timestamp_utc)" \
      --arg phase "${phase}" \
      --arg currentPrimary "${primary}" \
      --arg service "${service}" \
      --argjson rows "${rows_json}" \
      '{
        observedAt: $observedAt,
        phase: $phase,
        currentPrimary: $currentPrimary,
        service: $service,
        ok: true,
        rows: $rows,
        error: ""
      }' >"${snapshot_file}"
    return 0
  fi

  jq -n \
    --arg observedAt "$(timestamp_utc)" \
    --arg phase "${phase}" \
    --arg currentPrimary "${primary}" \
    --arg service "${service}" \
    --arg error "${output}" \
    '{
      observedAt: $observedAt,
      phase: $phase,
      currentPrimary: $currentPrimary,
      service: $service,
      ok: false,
      rows: [],
      error: $error
    }' >"${snapshot_file}"
  return 1
}

capture_pg_stat_wal_receiver() {
  local case_dir=$1
  local phase=${2:-final}
  local snapshot_file="${case_dir}/pg-stat-wal-receiver.jsonl"
  local observed_at member service output query_status rows_json

  : >"${snapshot_file}"
  observed_at="$(timestamp_utc)"
  for member in alpha-1 alpha-2 alpha-3; do
    service=$(service_for_member "${member}")
    query_status=0
    output=$(psql_service "${service}" "
SELECT coalesce(json_agg(json_build_object(
  'pid', pid,
  'status', status,
  'receiveStartLsn', receive_start_lsn::text,
  'receiveStartTli', receive_start_tli,
  'writtenLsn', written_lsn::text,
  'flushedLsn', flushed_lsn::text,
  'receivedTli', received_tli,
  'lastMsgSendTime', last_msg_send_time,
  'lastMsgReceiptTime', last_msg_receipt_time,
  'latestEndLsn', latest_end_lsn::text,
  'latestEndTime', latest_end_time,
  'slotName', slot_name,
  'senderHost', sender_host,
  'senderPort', sender_port,
  'conninfo', conninfo
)), '[]'::json)
FROM pg_stat_wal_receiver;
" 2>&1) || query_status=$?

    if [[ "${query_status}" -eq 0 ]] && rows_json=$(printf '%s\n' "${output}" | jq -c . 2>/dev/null); then
      append_jsonl "${snapshot_file}" \
        observedAt "$(json_escape "${observed_at}")" \
        phase "$(json_escape "${phase}")" \
        member "$(json_escape "${member}")" \
        service "$(json_escape "${service}")" \
        ok true \
        rows "${rows_json}" \
        error "$(json_escape "")"
      continue
    fi

    append_jsonl "${snapshot_file}" \
      observedAt "$(json_escape "${observed_at}")" \
      phase "$(json_escape "${phase}")" \
      member "$(json_escape "${member}")" \
      service "$(json_escape "${service}")" \
      ok false \
      rows "[]" \
      error "$(json_escape "${output}")"
  done
}

pacman_cluster_status_json() {
  local service=${1:-pacman-primary}
  local output

  output=$(compose_exec "${service}" env \
    "PACMANCTL_API_URL=http://${service}:8080" \
    "PACMANCTL_API_TOKEN=lab-admin-token" \
    pacmanctl cluster status -o json 2>&1) || return $?

  printf '%s\n' "${output}" | jq -sc 'map(select(type == "object" and has("clusterName"))) | last // empty'
}

workload_op_table() {
  case "$1" in
    append-smoke | append-failover | append-switchover | open-transaction-failover) printf 'jepsen.append_values\n' ;;
    single-key-register) printf 'jepsen.register_values\n' ;;
    read-committed-txn | serializable-txn) printf 'jepsen.txn_ops\n' ;;
    *) return 1 ;;
  esac
}

json_array_from_file() {
  local path=$1

  jq -Rsc 'split("\n") | map(select(length > 0))' "${path}"
}

sample_primary_state() {
  local sample_id=$1
  local observation_file=$2
  local observed_at
  observed_at="$(timestamp_utc)"

  local member service output status in_recovery timeline lsn writable
  for member in alpha-1 alpha-2 alpha-3; do
    service=$(service_for_member "${member}")
    status=0
    output=$(psql_service "${service}" "
with local as (
  select
    pg_is_in_recovery() as in_recovery,
    case
      when pg_is_in_recovery() then null
      else pg_current_wal_lsn()::text
    end as write_lsn,
    pg_last_wal_replay_lsn()::text as replay_lsn
)
select
  local.in_recovery,
  case
    when local.in_recovery then greatest(
      coalesce(nullif(recovery.min_recovery_end_timeline, 0), 0),
      coalesce((select max(received_tli) from pg_stat_wal_receiver), 0),
      checkpoint.timeline_id
    )
    else ('x' || substr(pg_walfile_name(local.write_lsn::pg_lsn), 1, 8))::bit(32)::bigint
  end,
  coalesce(local.write_lsn, local.replay_lsn, '')
from local
cross join pg_control_checkpoint() as checkpoint
cross join pg_control_recovery() as recovery;
" 2>&1) || status=$?

    if [[ "${status}" -ne 0 ]]; then
      append_jsonl "${observation_file}" \
        sampleId "${sample_id}" \
        observedAt "$(json_escape "${observed_at}")" \
        member "$(json_escape "${member}")" \
        service "$(json_escape "${service}")" \
        reachable false \
        writable false \
        inRecovery null \
        timeline null \
        lsn "$(json_escape "")" \
        error "$(json_escape "${output}")"
      continue
    fi

    IFS=$'\t' read -r in_recovery timeline lsn <<<"${output}"
    writable=false
    if [[ "${in_recovery}" == "f" ]]; then
      writable=true
    fi

    append_jsonl "${observation_file}" \
      sampleId "${sample_id}" \
      observedAt "$(json_escape "${observed_at}")" \
      member "$(json_escape "${member}")" \
      service "$(json_escape "${service}")" \
      reachable true \
      writable "${writable}" \
      inRecovery "$([[ "${in_recovery}" == "t" ]] && printf true || printf false)" \
      timeline "${timeline:-0}" \
      lsn "$(json_escape "${lsn:-}")" \
      error "$(json_escape "")"
  done
}

start_primary_sampler() {
  local case_dir=$1
  local __pid_var=$2
  local observation_file="${case_dir}/primary-observations.jsonl"

  (
    local sample_id=1
    while true; do
      sample_primary_state "${sample_id}" "${observation_file}"
      sample_id=$((sample_id + 1))
      sleep "${jepsen_primary_sample_interval}"
    done
  ) >/dev/null 2>>"${case_dir}/primary-sampler.log" &
  printf -v "${__pid_var}" '%s' "$!"
}

stop_primary_sampler() {
  local pid=$1

  if [[ -n "${pid}" ]]; then
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
}

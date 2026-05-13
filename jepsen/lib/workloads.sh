#!/usr/bin/env bash

jepsen_pg_client_service="${PACMAN_JEPSEN_PG_CLIENT_SERVICE:-pacman-primary}"
jepsen_pg_host="${PACMAN_JEPSEN_PG_HOST:-172.28.0.100}"
jepsen_pg_port="${PACMAN_JEPSEN_PG_PORT:-5432}"
jepsen_pg_user="${PACMAN_JEPSEN_PG_USER:-postgres}"
jepsen_pg_password="${PACMAN_JEPSEN_PG_PASSWORD:-pacman-demo-password}"
jepsen_pg_database="${PACMAN_JEPSEN_PG_DATABASE:-postgres}"
jepsen_default_ops="${PACMAN_JEPSEN_WORKLOAD_OPS:-12}"
jepsen_default_duration="${PACMAN_JEPSEN_WORKLOAD_DURATION_SECONDS:-20}"
jepsen_default_clients="${PACMAN_JEPSEN_WORKLOAD_CLIENTS:-3}"
jepsen_default_keys="${PACMAN_JEPSEN_WORKLOAD_KEYS:-3}"
jepsen_nemesis_hold_seconds="${PACMAN_JEPSEN_NEMESIS_HOLD_SECONDS:-8}"
jepsen_post_nemesis_settle_seconds="${PACMAN_JEPSEN_POST_NEMESIS_SETTLE_SECONDS:-10}"
jepsen_primary_sample_interval="${PACMAN_JEPSEN_PRIMARY_SAMPLE_INTERVAL_SECONDS:-1}"
jepsen_allow_async_loss="${PACMAN_JEPSEN_ALLOW_ASYNC_LOSS:-false}"
jepsen_smoke_cases_default="append-smoke:none"
jepsen_nightly_cases_default="append-smoke:none append-failover:kill single-key-register:packet read-committed-txn:slow-network serializable-txn:packet,kill append-failover:repeated-failure"

jepsen_default_cases() {
  case "$1" in
    smoke) printf '%s\n' "${jepsen_smoke_cases_default}" ;;
    nightly) printf '%s\n' "${jepsen_nightly_cases_default}" ;;
    *)
      printf 'unsupported Jepsen campaign: %s\n' "$1" >&2
      return 2
      ;;
  esac
}

jepsen_cases_for_campaign() {
  local campaign=$1
  if [[ -n "${PACMAN_JEPSEN_CASES:-}" ]]; then
    printf '%s\n' "${PACMAN_JEPSEN_CASES}"
    return 0
  fi
  jepsen_default_cases "${campaign}"
}

list_jepsen_cases() {
  cat <<'EOF'
append-smoke-none append-smoke:none Smoke append workload without nemesis.
append-failover-kill append-failover:kill Append workload while killing current primary PostgreSQL.
single-key-register-packet single-key-register:packet Register workload while partitioning the current primary.
read-committed-txn-slow-network read-committed-txn:slow-network Read committed transaction workload under latency and loss.
serializable-txn-packet-kill serializable-txn:packet,kill Serializable transaction workload under partition plus kill.
append-failover-repeated-failure append-failover:repeated-failure Append workload under slow network, partition, and kill sequence.
EOF
}

resolve_jepsen_case_spec() {
  local name=$1

  case "${name}" in
    append-smoke-none | append-smoke:none) printf 'append-smoke:none\n' ;;
    append-failover-kill | append-failover:kill) printf 'append-failover:kill\n' ;;
    single-key-register-packet | single-key-register:packet) printf 'single-key-register:packet\n' ;;
    read-committed-txn-slow-network | read-committed-txn:slow-network) printf 'read-committed-txn:slow-network\n' ;;
    serializable-txn-packet-kill | serializable-txn:packet,kill) printf 'serializable-txn:packet,kill\n' ;;
    append-failover-repeated-failure | append-failover:repeated-failure) printf 'append-failover:repeated-failure\n' ;;
    *)
      printf 'unsupported Jepsen case: %s\n' "${name}" >&2
      printf 'Supported cases:\n' >&2
      list_jepsen_cases >&2
      return 2
      ;;
  esac
}

json_escape() {
  printf '%s' "${1}" | jq -Rsa .
}

case_slug() {
  local value=$1
  value=${value//,/+}
  value=${value//[^A-Za-z0-9_.+-]/-}
  printf '%s\n' "${value}"
}

sql_literal() {
  local value=$1
  value=${value//\'/\'\'}
  printf "'%s'" "${value}"
}

psql_vip() {
  local sql=$1

  docker compose -f "${compose_file}" exec -T "${jepsen_pg_client_service}" \
    env "PGPASSWORD=${jepsen_pg_password}" \
    /usr/pgsql-17/bin/psql \
      -v ON_ERROR_STOP=1 \
      -h "${jepsen_pg_host}" \
      -p "${jepsen_pg_port}" \
      -U "${jepsen_pg_user}" \
      -d "${jepsen_pg_database}" \
      -Atq <<<"${sql}"
}

psql_vip_optional() {
  local sql=$1

  psql_vip "${sql}" 2>&1 || return 0
}

psql_service() {
  local service=$1
  local sql=$2

  docker compose -f "${compose_file}" exec -T "${service}" \
    env "PGPASSWORD=${jepsen_pg_password}" \
    /usr/pgsql-17/bin/psql \
      -v ON_ERROR_STOP=1 \
      -h 127.0.0.1 \
      -p 5432 \
      -U "${jepsen_pg_user}" \
      -d "${jepsen_pg_database}" \
      -F $'\t' \
      -Atq <<<"${sql}"
}

append_jsonl() {
  local path=$1
  shift

  {
    printf '{'
    local first=true
    while [[ "$#" -gt 0 ]]; do
      local key=$1
      local value=$2
      shift 2
      if [[ "${first}" == "true" ]]; then
        first=false
      else
        printf ','
      fi
      printf '"%s":%s' "${key}" "${value}"
    done
    printf '}\n'
  } >>"${path}"
}

current_primary_name() {
  compose_exec pacman-primary /bin/sh -lc \
    "PACMANCTL_API_URL=http://pacman-primary:8080 PACMANCTL_API_TOKEN=lab-admin-token pacmanctl cluster status -o json" |
    jq -r '.currentPrimary // .current_primary // ""'
}

service_for_member() {
  case "$1" in
    alpha-1) printf 'pacman-primary\n' ;;
    alpha-2) printf 'pacman-replica\n' ;;
    alpha-3) printf 'pacman-replica-2\n' ;;
    *) return 1 ;;
  esac
}

peer_service_for_member() {
  case "$1" in
    alpha-1) printf 'pacman-replica pacman-replica-2\n' ;;
    alpha-2) printf 'pacman-primary\n' ;;
    alpha-3) printf 'pacman-primary\n' ;;
    *) return 1 ;;
  esac
}

service_ip() {
  case "$1" in
    pacman-primary) printf '172.28.0.11\n' ;;
    pacman-replica) printf '172.28.0.12\n' ;;
    pacman-replica-2) printf '172.28.0.13\n' ;;
    pacman-dcs) printf '172.28.0.10\n' ;;
    *) return 1 ;;
  esac
}

workload_op_table() {
  case "$1" in
    append-smoke | append-failover) printf 'jepsen.append_values\n' ;;
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

check_single_writable_primary() {
  local case_dir=$1
  local observation_file="${case_dir}/primary-observations.jsonl"
  local checker_file="${case_dir}/single-primary-checker.json"

  if [[ ! -s "${observation_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"single-writable-primary","valid":false,"observations":0,"samples":0,"writableObservations":0,"violationSamples":[]}
EOF
    return 1
  fi

  jq -s '
    def writable: map(select(.reachable == true and .writable == true));
    def violation_samples:
      writable
      | group_by(.sampleId)
      | map(select(length > 1))
      | map({
          sampleId: .[0].sampleId,
          observedAt: .[0].observedAt,
          writableMembers: map(.member),
          timelines: map(.timeline)
        });
    {
      checker: "single-writable-primary",
      valid: ((violation_samples | length) == 0),
      observations: length,
      samples: ([.[].sampleId] | unique | length),
      writableObservations: (writable | length),
      violationSamples: violation_samples
    }
  ' "${observation_file}" >"${checker_file}"

  jq -e '.valid == true and .samples > 0' "${checker_file}" >/dev/null
}

check_acknowledged_write_preservation() {
  local workload=$1
  local run_id=$2
  local case_dir=$3
  local checker_file="${case_dir}/acknowledged-write-checker.json"
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local counts_file="${case_dir}/final-primary-op-counts.tsv"
  local acknowledged_file="${case_dir}/acknowledged-op-ids.sorted"
  local actual_file="${case_dir}/final-primary-op-ids.sorted"
  local observed_once_file="${case_dir}/final-primary-observed-once-op-ids.sorted"
  local duplicate_file="${case_dir}/final-primary-duplicate-op-ids.sorted"
  local missing_file="${case_dir}/missing-acknowledged-op-ids.txt"
  local duplicate_ack_file="${case_dir}/duplicate-acknowledged-op-ids.txt"
  local unexpected_file="${case_dir}/unacknowledged-observed-op-ids.txt"
  local table final_primary final_primary_service query_status

  table=$(workload_op_table "${workload}") || {
    cat >"${checker_file}" <<EOF
{"checker":"acknowledged-write-preservation","valid":false,"error":"unsupported workload","workload":"${workload}"}
EOF
    return 2
  }

  touch "${ack_file}"
  LC_ALL=C sort -u "${ack_file}" >"${acknowledged_file}"

  final_primary=$(current_primary_name 2>/dev/null || true)
  [[ -n "${final_primary}" ]] || final_primary="alpha-1"
  final_primary_service=$(service_for_member "${final_primary}" 2>/dev/null || printf 'pacman-primary')

  query_status=0
  psql_service "${final_primary_service}" "
SELECT op_id, count(*)::int
FROM ${table}
WHERE run_id = $(sql_literal "${run_id}")
GROUP BY op_id
ORDER BY op_id;
" >"${counts_file}" 2>"${case_dir}/acknowledged-write-checker-query.log" || query_status=$?

  if [[ "${query_status}" -ne 0 ]]; then
    jq -n \
      --arg workload "${workload}" \
      --arg runId "${run_id}" \
      --arg finalPrimary "${final_primary}" \
      --arg finalPrimaryService "${final_primary_service}" \
      --arg table "${table}" \
      --arg error "$(cat "${case_dir}/acknowledged-write-checker-query.log")" \
      '{
        checker: "acknowledged-write-preservation",
        valid: false,
        workload: $workload,
        runId: $runId,
        finalPrimary: $finalPrimary,
        finalPrimaryService: $finalPrimaryService,
        table: $table,
        error: $error
      }' >"${checker_file}"
    return 1
  fi

  awk -F $'\t' 'NF >= 2 {print $1}' "${counts_file}" | LC_ALL=C sort -u >"${actual_file}"
  awk -F $'\t' 'NF >= 2 && $2 == 1 {print $1}' "${counts_file}" | LC_ALL=C sort -u >"${observed_once_file}"
  awk -F $'\t' 'NF >= 2 && $2 != 1 {print $1}' "${counts_file}" | LC_ALL=C sort -u >"${duplicate_file}"
  comm -23 "${acknowledged_file}" "${actual_file}" >"${missing_file}"
  comm -12 "${acknowledged_file}" "${duplicate_file}" >"${duplicate_ack_file}"
  comm -13 "${acknowledged_file}" "${actual_file}" >"${unexpected_file}"

  local expected observed_once missing duplicate_ack unexpected async_loss_allowed valid
  expected=$(wc -l <"${acknowledged_file}" | tr -d ' ')
  observed_once=$(comm -12 "${acknowledged_file}" "${observed_once_file}" | wc -l | tr -d ' ')
  missing=$(wc -l <"${missing_file}" | tr -d ' ')
  duplicate_ack=$(wc -l <"${duplicate_ack_file}" | tr -d ' ')
  unexpected=$(wc -l <"${unexpected_file}" | tr -d ' ')
  async_loss_allowed=false
  if [[ "${jepsen_allow_async_loss}" == "true" ]]; then
    async_loss_allowed=true
  fi

  valid=false
  if [[ "${expected}" -gt 0 && "${duplicate_ack}" -eq 0 ]]; then
    if [[ "${missing}" -eq 0 || "${async_loss_allowed}" == "true" ]]; then
      valid=true
    fi
  fi

  jq -n \
    --arg workload "${workload}" \
    --arg runId "${run_id}" \
    --arg finalPrimary "${final_primary}" \
    --arg finalPrimaryService "${final_primary_service}" \
    --arg table "${table}" \
    --argjson valid "${valid}" \
    --argjson asyncLossAllowed "${async_loss_allowed}" \
    --argjson expectedAcknowledged "${expected}" \
    --argjson observedExactlyOnce "${observed_once}" \
    --argjson missingAcknowledged "${missing}" \
    --argjson duplicateAcknowledged "${duplicate_ack}" \
    --argjson unacknowledgedObserved "${unexpected}" \
    --argjson missingOpIds "$(json_array_from_file "${missing_file}")" \
    --argjson duplicateOpIds "$(json_array_from_file "${duplicate_ack_file}")" \
    --argjson unacknowledgedObservedOpIds "$(json_array_from_file "${unexpected_file}")" \
    '{
      checker: "acknowledged-write-preservation",
      valid: $valid,
      workload: $workload,
      runId: $runId,
      finalPrimary: $finalPrimary,
      finalPrimaryService: $finalPrimaryService,
      table: $table,
      asyncLossAllowed: $asyncLossAllowed,
      expectedAcknowledged: $expectedAcknowledged,
      observedExactlyOnce: $observedExactlyOnce,
      missingAcknowledged: $missingAcknowledged,
      duplicateAcknowledged: $duplicateAcknowledged,
      unacknowledgedObserved: $unacknowledgedObserved,
      missingOpIds: $missingOpIds,
      duplicateOpIds: $duplicateOpIds,
      unacknowledgedObservedOpIds: $unacknowledgedObservedOpIds
    }' >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

check_timeline_convergence() {
  local case_dir=$1
  local observation_file="${case_dir}/primary-observations.jsonl"
  local checker_file="${case_dir}/timeline-checker.json"

  if [[ ! -s "${observation_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"timeline-convergence","valid":false,"observations":0,"samples":0,"error":"missing primary observations"}
EOF
    return 1
  fi

  jq -s '
    def samples:
      sort_by(.sampleId)
      | group_by(.sampleId)
      | map({
          sampleId: .[0].sampleId,
          observedAt: .[0].observedAt,
          observations: .
        });
    def writable_members($sample):
      $sample.observations
      | map(select(.reachable == true and .writable == true));
    def primary_of($sample):
      writable_members($sample) | sort_by(.member) | .[0] // null;
    def summarize_member:
      {
        member,
        service,
        reachable,
        writable,
        inRecovery,
        timeline,
        lsn,
        error
      };

    samples as $samples
    | ($samples[0] // null) as $initialSample
    | ($samples[-1] // null) as $finalSample
    | (if $initialSample == null then [] else writable_members($initialSample) end) as $initialWritable
    | (if $finalSample == null then [] else writable_members($finalSample) end) as $finalWritable
    | (if $initialSample == null then null else primary_of($initialSample) end) as $initialPrimary
    | (if $finalSample == null then null else primary_of($finalSample) end) as $finalPrimary
    | (($initialPrimary != null) and ($finalPrimary != null)) as $hasPrimaries
    | ($hasPrimaries and ($initialPrimary.member != $finalPrimary.member)) as $promotionObserved
    | (
        if ($hasPrimaries | not) then false
        elif ($promotionObserved | not) then true
        else (($finalPrimary.timeline // 0) > ($initialPrimary.timeline // 0))
        end
      ) as $timelineAdvanced
    | (
        if $finalPrimary == null then []
        else
          $finalSample.observations
          | map(select(
              .reachable == true
              and .member != $finalPrimary.member
              and (.timeline // 0) != ($finalPrimary.timeline // 0)
            ))
          | map(summarize_member)
        end
      ) as $replicaTimelineViolations
    | (
        if ($promotionObserved | not) then null
        else
          $finalSample.observations
          | map(select(.member == $initialPrimary.member))
          | .[0] // null
        end
      ) as $oldPrimaryFinalState
    | (
        if ($promotionObserved | not) then true
        elif $oldPrimaryFinalState == null then false
        else
          (($oldPrimaryFinalState.reachable == false)
          or (($oldPrimaryFinalState.writable == false)
              and (($oldPrimaryFinalState.timeline // 0) == ($finalPrimary.timeline // 0))))
        end
      ) as $oldPrimarySafe
    | (($initialWritable | length) == 1) as $singleInitialPrimary
    | (($finalWritable | length) == 1) as $singleFinalPrimary
    | (($replicaTimelineViolations | length) == 0) as $replicasConverged
    | {
        checker: "timeline-convergence",
        valid: (
          ($samples | length) > 0
          and $singleInitialPrimary
          and $singleFinalPrimary
          and $timelineAdvanced
          and $replicasConverged
          and $oldPrimarySafe
        ),
        observations: length,
        samples: ($samples | length),
        initialSample: (
          if $initialSample == null then null else {
            sampleId: $initialSample.sampleId,
            observedAt: $initialSample.observedAt,
            primary: (if $initialPrimary == null then null else ($initialPrimary | summarize_member) end),
            writableMembers: ($initialWritable | map(.member)),
            members: ($initialSample.observations | map(summarize_member))
          } end
        ),
        finalSample: (
          if $finalSample == null then null else {
            sampleId: $finalSample.sampleId,
            observedAt: $finalSample.observedAt,
            primary: (if $finalPrimary == null then null else ($finalPrimary | summarize_member) end),
            writableMembers: ($finalWritable | map(.member)),
            members: ($finalSample.observations | map(summarize_member))
          } end
        ),
        promotionObserved: $promotionObserved,
        timelineAdvanced: $timelineAdvanced,
        replicasConverged: $replicasConverged,
        oldPrimarySafe: $oldPrimarySafe,
        replicaTimelineViolations: $replicaTimelineViolations,
        oldPrimaryFinalState: (if $oldPrimaryFinalState == null then null else ($oldPrimaryFinalState | summarize_member) end)
      }
  ' "${observation_file}" >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

ensure_workload_schema() {
  psql_vip "
CREATE SCHEMA IF NOT EXISTS jepsen;
CREATE TABLE IF NOT EXISTS jepsen.append_values (
  run_id text NOT NULL,
  op_id text PRIMARY KEY,
  key_id integer NOT NULL,
  value text NOT NULL,
  client_id integer NOT NULL,
  observed_primary text NOT NULL,
  isolation text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS jepsen.register_values (
  run_id text NOT NULL,
  op_id text PRIMARY KEY,
  value bigint NOT NULL,
  client_id integer NOT NULL,
  observed_primary text NOT NULL,
  isolation text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS jepsen.txn_accounts (
  run_id text NOT NULL,
  key_id integer NOT NULL,
  balance bigint NOT NULL,
  PRIMARY KEY (run_id, key_id)
);
CREATE TABLE IF NOT EXISTS jepsen.txn_ops (
  run_id text NOT NULL,
  op_id text PRIMARY KEY,
  key_id integer NOT NULL,
  amount bigint NOT NULL,
  observed_total bigint NOT NULL,
  client_id integer NOT NULL,
  observed_primary text NOT NULL,
  isolation text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
" >/dev/null
}

write_case_event() {
  local case_history=$1
  local process=$2
  local status=$3
  local function_name=$4
  local value=$5

  printf '{:time "%s" :process %s :type :%s :f :%s :value %s}\n' \
    "$(timestamp_utc)" "${process}" "${status}" "${function_name}" "${value}" >>"${case_history}"
}

record_case_result() {
  local case_result_file=$1
  local workload=$2
  local nemesis=$3
  local valid=$4
  local details=$5

  append_jsonl "${case_result_file}" \
    workload "$(json_escape "${workload}")" \
    nemesis "$(json_escape "${nemesis}")" \
    valid "${valid}" \
    details "$(json_escape "${details}")"
}

run_append_workload() {
  local run_id=$1
  local case_dir=$2
  local isolation=${3:-read committed}
  local ops=${4:-${jepsen_default_ops}}
  local keys=${5:-${jepsen_default_keys}}
  local history="${case_dir}/history.edn"
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local failures="${case_dir}/failures.log"

  : >"${ack_file}"
  : >"${failures}"

  for ((op=1; op<=ops; op++)); do
    local client=$(( (op - 1) % jepsen_default_clients ))
    local key=$(( (op - 1) % keys ))
    local op_id="${run_id}-append-${op}"
    local value="v-${op}"
    local observed_primary
    observed_primary=$(current_primary_name 2>/dev/null || true)
    [[ -n "${observed_primary}" ]] || observed_primary="unknown"

    write_case_event "${history}" "${client}" "invoke" "append" \
      "{:op-id \"${op_id}\" :key ${key} :value \"${value}\" :primary \"${observed_primary}\"}"

    local sql="
BEGIN ISOLATION LEVEL ${isolation};
INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
VALUES ($(sql_literal "${run_id}"), $(sql_literal "${op_id}"), ${key}, $(sql_literal "${value}"), ${client}, $(sql_literal "${observed_primary}"), $(sql_literal "${isolation}"));
COMMIT;
"
    if psql_vip "${sql}" >/dev/null 2>>"${failures}"; then
      printf '%s\n' "${op_id}" >>"${ack_file}"
      write_case_event "${history}" "${client}" "ok" "append" \
        "{:op-id \"${op_id}\" :key ${key} :value \"${value}\" :primary \"${observed_primary}\"}"
    else
      write_case_event "${history}" "${client}" "fail" "append" \
        "{:op-id \"${op_id}\" :key ${key} :value \"${value}\" :primary \"${observed_primary}\"}"
    fi
  done
}

check_append_workload() {
  local run_id=$1
  local case_dir=$2
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local expected
  local actual
  local duplicates

  expected=$(wc -l <"${ack_file}" | tr -d ' ')
  actual=$(psql_vip "SELECT count(*) FROM jepsen.append_values WHERE run_id = $(sql_literal "${run_id}");" | tail -n 1)
  duplicates=$(psql_vip "SELECT count(*) FROM (SELECT op_id FROM jepsen.append_values WHERE run_id = $(sql_literal "${run_id}") GROUP BY op_id HAVING count(*) > 1) dup;" | tail -n 1)

  cat >"${case_dir}/checker.json" <<EOF
{"checker":"append","expectedAcknowledged":${expected},"actualRows":${actual},"duplicateOpIds":${duplicates}}
EOF

  [[ "${expected}" -gt 0 && "${actual}" -eq "${expected}" && "${duplicates}" -eq 0 ]]
}

run_register_workload() {
  local run_id=$1
  local case_dir=$2
  local isolation=${3:-read committed}
  local ops=${4:-${jepsen_default_ops}}
  local history="${case_dir}/history.edn"
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local failures="${case_dir}/failures.log"

  : >"${ack_file}"
  : >"${failures}"

  for ((op=1; op<=ops; op++)); do
    local client=$(( (op - 1) % jepsen_default_clients ))
    local value=${op}
    local op_id="${run_id}-register-${op}"
    local observed_primary
    observed_primary=$(current_primary_name 2>/dev/null || true)
    [[ -n "${observed_primary}" ]] || observed_primary="unknown"

    write_case_event "${history}" "${client}" "invoke" "write" \
      "{:op-id \"${op_id}\" :value ${value} :primary \"${observed_primary}\"}"

    local observed
    if observed=$(psql_vip "
WITH inserted AS (
  INSERT INTO jepsen.register_values(run_id, op_id, value, client_id, observed_primary, isolation)
  VALUES ($(sql_literal "${run_id}"), $(sql_literal "${op_id}"), ${value}, ${client}, $(sql_literal "${observed_primary}"), $(sql_literal "${isolation}"))
  RETURNING value
)
SELECT max(value) FROM jepsen.register_values WHERE run_id = $(sql_literal "${run_id}");
" 2>>"${failures}"); then
      printf '%s\n' "${op_id}" >>"${ack_file}"
      write_case_event "${history}" "${client}" "ok" "write" \
        "{:op-id \"${op_id}\" :value ${value} :read ${observed:-0} :primary \"${observed_primary}\"}"
      if [[ "${observed:-0}" -lt "${value}" ]]; then
        printf 'stale read after %s: saw %s\n' "${op_id}" "${observed}" >>"${failures}"
      fi
    else
      write_case_event "${history}" "${client}" "fail" "write" \
        "{:op-id \"${op_id}\" :value ${value} :primary \"${observed_primary}\"}"
    fi
  done
}

check_register_workload() {
  local run_id=$1
  local case_dir=$2
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local expected
  local actual
  local max_value

  expected=$(wc -l <"${ack_file}" | tr -d ' ')
  actual=$(psql_vip "SELECT count(*) FROM jepsen.register_values WHERE run_id = $(sql_literal "${run_id}");" | tail -n 1)
  max_value=$(psql_vip "SELECT COALESCE(max(value), 0) FROM jepsen.register_values WHERE run_id = $(sql_literal "${run_id}");" | tail -n 1)

  cat >"${case_dir}/checker.json" <<EOF
{"checker":"single-key-register","expectedAcknowledged":${expected},"actualRows":${actual},"maxValue":${max_value}}
EOF

  [[ "${expected}" -gt 0 && "${actual}" -eq "${expected}" && "${max_value}" -ge "${expected}" ]]
}

run_txn_workload() {
  local run_id=$1
  local case_dir=$2
  local isolation=$3
  local ops=${4:-${jepsen_default_ops}}
  local keys=${5:-${jepsen_default_keys}}
  local history="${case_dir}/history.edn"
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local failures="${case_dir}/failures.log"

  : >"${ack_file}"
  : >"${failures}"

  for ((key=0; key<keys; key++)); do
    psql_vip "INSERT INTO jepsen.txn_accounts(run_id, key_id, balance) VALUES ($(sql_literal "${run_id}"), ${key}, 0) ON CONFLICT (run_id, key_id) DO NOTHING;" >/dev/null
  done

  for ((op=1; op<=ops; op++)); do
    local client=$(( (op - 1) % jepsen_default_clients ))
    local key=$(( (op - 1) % keys ))
    local op_id="${run_id}-txn-${op}"
    local observed_primary
    observed_primary=$(current_primary_name 2>/dev/null || true)
    [[ -n "${observed_primary}" ]] || observed_primary="unknown"

    write_case_event "${history}" "${client}" "invoke" "txn" \
      "{:op-id \"${op_id}\" :key ${key} :amount 1 :isolation \"${isolation}\" :primary \"${observed_primary}\"}"

    local total
    if total=$(psql_vip "
BEGIN ISOLATION LEVEL ${isolation};
UPDATE jepsen.txn_accounts SET balance = balance + 1 WHERE run_id = $(sql_literal "${run_id}") AND key_id = ${key};
WITH total AS (
  SELECT sum(balance) AS value FROM jepsen.txn_accounts WHERE run_id = $(sql_literal "${run_id}")
), inserted AS (
  INSERT INTO jepsen.txn_ops(run_id, op_id, key_id, amount, observed_total, client_id, observed_primary, isolation)
  SELECT $(sql_literal "${run_id}"), $(sql_literal "${op_id}"), ${key}, 1, value, ${client}, $(sql_literal "${observed_primary}"), $(sql_literal "${isolation}") FROM total
)
SELECT value FROM total;
COMMIT;
" 2>>"${failures}"); then
      total=$(printf '%s\n' "${total}" | sed '/^$/d' | tail -n 1)
      printf '%s\n' "${op_id}" >>"${ack_file}"
      write_case_event "${history}" "${client}" "ok" "txn" \
        "{:op-id \"${op_id}\" :key ${key} :total ${total:-0} :isolation \"${isolation}\" :primary \"${observed_primary}\"}"
    else
      write_case_event "${history}" "${client}" "fail" "txn" \
        "{:op-id \"${op_id}\" :key ${key} :isolation \"${isolation}\" :primary \"${observed_primary}\"}"
    fi
  done
}

check_txn_workload() {
  local run_id=$1
  local case_dir=$2
  local checker_name=$3
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local expected
  local op_count
  local total

  expected=$(wc -l <"${ack_file}" | tr -d ' ')
  op_count=$(psql_vip "SELECT count(*) FROM jepsen.txn_ops WHERE run_id = $(sql_literal "${run_id}");" | tail -n 1)
  total=$(psql_vip "SELECT COALESCE(sum(balance), 0) FROM jepsen.txn_accounts WHERE run_id = $(sql_literal "${run_id}");" | tail -n 1)

  cat >"${case_dir}/checker.json" <<EOF
{"checker":"${checker_name}","expectedAcknowledged":${expected},"actualOps":${op_count},"accountTotal":${total}}
EOF

  [[ "${expected}" -gt 0 && "${op_count}" -eq "${expected}" && "${total}" -eq "${expected}" ]]
}

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
      printf '{:time "%s" :nemesis :none :action :stop}\n' "$(timestamp_utc)" >>"${schedule_file}"
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
        sleep "${jepsen_nemesis_hold_seconds}"
        start_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :kill :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        ;;
      packet)
        printf '{:time "%s" :nemesis :packet :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        iptables_partition "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        sleep "${jepsen_nemesis_hold_seconds}"
        iptables_heal "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :packet :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        ;;
      packet,kill)
        printf '{:time "%s" :nemesis :packet-kill :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        iptables_partition "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        stop_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        sleep "${jepsen_nemesis_hold_seconds}"
        start_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        iptables_heal "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :packet-kill :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        ;;
      slow-network)
        printf '{:time "%s" :nemesis :slow-network :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        slow_network_on "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        sleep "${jepsen_nemesis_hold_seconds}"
        slow_network_off "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :slow-network :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        ;;
      repeated-failure)
        printf '{:time "%s" :nemesis :repeated-failure :action :start :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
        slow_network_on "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        sleep 3
        slow_network_off "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        iptables_partition "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        sleep 3
        iptables_heal "${service}" ${peer_services} >>"${run_dir}/nemesis.log" 2>&1 || true
        stop_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        sleep "${jepsen_nemesis_hold_seconds}"
        start_postgres "${service}" >>"${run_dir}/nemesis.log" 2>&1 || true
        printf '{:time "%s" :nemesis :repeated-failure :action :stop :target "%s"}\n' "$(timestamp_utc)" "${member:-unknown}" >>"${schedule_file}"
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

run_workload_profile() {
  local workload=$1
  local run_id=$2
  local case_dir=$3

  case "${workload}" in
    append-smoke | append-failover)
      run_append_workload "${run_id}" "${case_dir}" "read committed"
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
    append-smoke | append-failover)
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

  write_edn_event "${campaign_history}" "${workload}/${nemesis}" "invoke" "\"${run_id}\""
  write_case_event "${case_dir}/history.edn" ":case" "invoke" "workload" \
    "{:workload \"${workload}\" :nemesis \"${nemesis}\" :run-id \"${run_id}\"}"

  sample_primary_state 0 "${case_dir}/primary-observations.jsonl"

  local primary_sampler_pid=""
  start_primary_sampler "${case_dir}" primary_sampler_pid

  local nemesis_pid=""
  run_nemesis_profile "${nemesis}" "${case_dir}" "${schedule_file}" nemesis_pid "${jepsen_default_duration}" || true

  local workload_status=0
  run_workload_profile "${workload}" "${run_id}" "${case_dir}" || workload_status=$?
  wait_for_nemesis "${nemesis_pid}"
  settle_after_nemesis "${nemesis}" "${case_dir}"
  stop_primary_sampler "${primary_sampler_pid}"
  sample_primary_state 1000000000 "${case_dir}/primary-observations.jsonl"

  local workload_checker_status=0
  check_workload_profile "${workload}" "${run_id}" "${case_dir}" || workload_checker_status=$?

  local primary_checker_status=0
  check_single_writable_primary "${case_dir}" || primary_checker_status=$?

  local acknowledged_checker_status=0
  check_acknowledged_write_preservation "${workload}" "${run_id}" "${case_dir}" || acknowledged_checker_status=$?

  local timeline_checker_status=0
  check_timeline_convergence "${case_dir}" || timeline_checker_status=$?

  if [[ "${workload_status}" -eq 0 && "${workload_checker_status}" -eq 0 && "${primary_checker_status}" -eq 0 && "${acknowledged_checker_status}" -eq 0 && "${timeline_checker_status}" -eq 0 ]]; then
    write_case_event "${case_dir}/history.edn" ":case" "ok" "workload" \
      "{:workload \"${workload}\" :nemesis \"${nemesis}\" :run-id \"${run_id}\"}"
    cat "${case_dir}/history.edn" >>"${run_dir}/jepsen-history.edn"
    write_edn_event "${campaign_history}" "${workload}/${nemesis}" "ok" "\"${run_id}\""
    record_case_result "${case_results}" "${workload}" "${nemesis}" "true" "checkers passed"
    return 0
  fi

  write_case_event "${case_dir}/history.edn" ":case" "fail" "workload" \
    "{:workload \"${workload}\" :nemesis \"${nemesis}\" :run-id \"${run_id}\" :workload-status ${workload_status} :workload-checker-status ${workload_checker_status} :primary-checker-status ${primary_checker_status} :acknowledged-checker-status ${acknowledged_checker_status} :timeline-checker-status ${timeline_checker_status}}"
  cat "${case_dir}/history.edn" >>"${run_dir}/jepsen-history.edn"
  write_edn_event "${campaign_history}" "${workload}/${nemesis}" "fail" "\"${run_id}\""
  record_case_result "${case_results}" "${workload}" "${nemesis}" "false" "workload_status=${workload_status} workload_checker_status=${workload_checker_status} primary_checker_status=${primary_checker_status} acknowledged_checker_status=${acknowledged_checker_status} timeline_checker_status=${timeline_checker_status}"
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

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
CREATE TABLE IF NOT EXISTS jepsen.client_availability_probes (
  probe_id text PRIMARY KEY,
  nemesis text NOT NULL,
  observed_at timestamptz NOT NULL DEFAULT now()
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
  local op_delay=${6:-0}
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

    if [[ "${op}" -lt "${ops}" && "${op_delay}" != "0" ]]; then
      sleep "${op_delay}"
    fi
  done
}

run_open_transaction_failover_workload() {
  local run_id=$1
  local case_dir=$2
  local isolation=${3:-read committed}
  local history="${case_dir}/history.edn"
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local failures="${case_dir}/failures.log"
  local metadata_file="${case_dir}/open-transaction.json"
  local open_sleep=$(( jepsen_nemesis_hold_seconds + (jepsen_default_duration / 3) + 4 ))
  local pre_op_id="${run_id}-open-txn-pre"
  local open_op_id="${run_id}-open-txn-held"
  local post_op_id="${run_id}-open-txn-post"
  local observed_primary open_started_at open_finished_at post_primary output status post_status

  : >"${ack_file}"
  : >"${failures}"

  observed_primary=$(current_primary_name 2>/dev/null || true)
  [[ -n "${observed_primary}" ]] || observed_primary="unknown"

  write_case_event "${history}" 0 "invoke" "append" \
    "{:op-id \"${pre_op_id}\" :key 0 :value \"pre\" :primary \"${observed_primary}\"}"
  if psql_vip "
BEGIN ISOLATION LEVEL ${isolation};
INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
VALUES ($(sql_literal "${run_id}"), $(sql_literal "${pre_op_id}"), 0, 'pre', 0, $(sql_literal "${observed_primary}"), $(sql_literal "${isolation}"));
COMMIT;
" >/dev/null 2>>"${failures}"; then
    printf '%s\n' "${pre_op_id}" >>"${ack_file}"
    write_case_event "${history}" 0 "ok" "append" \
      "{:op-id \"${pre_op_id}\" :key 0 :value \"pre\" :primary \"${observed_primary}\"}"
  else
    write_case_event "${history}" 0 "fail" "append" \
      "{:op-id \"${pre_op_id}\" :key 0 :value \"pre\" :primary \"${observed_primary}\"}"
  fi

  open_started_at="$(timestamp_utc)"
  write_case_event "${history}" 1 "invoke" "open-txn" \
    "{:op-id \"${open_op_id}\" :key 1 :value \"held\" :primary \"${observed_primary}\" :sleep-seconds ${open_sleep}}"

  status=0
  output=$(psql_vip "
BEGIN ISOLATION LEVEL ${isolation};
INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
VALUES ($(sql_literal "${run_id}"), $(sql_literal "${open_op_id}"), 1, 'held', 1, $(sql_literal "${observed_primary}"), $(sql_literal "${isolation}"));
SELECT pg_sleep(${open_sleep});
COMMIT;
" 2>&1) || status=$?
  open_finished_at="$(timestamp_utc)"

  if [[ "${status}" -eq 0 ]]; then
    printf '%s\n' "${open_op_id}" >>"${ack_file}"
    write_case_event "${history}" 1 "ok" "open-txn" \
      "{:op-id \"${open_op_id}\" :key 1 :value \"held\" :primary \"${observed_primary}\" :sleep-seconds ${open_sleep}}"
  else
    printf '%s\n' "${output}" >>"${failures}"
    write_case_event "${history}" 1 "fail" "open-txn" \
      "{:op-id \"${open_op_id}\" :key 1 :value \"held\" :primary \"${observed_primary}\" :sleep-seconds ${open_sleep}}"
  fi

  wait_for_vip_writable 90 >>"${failures}" 2>&1 || true
  post_primary=$(current_primary_name 2>/dev/null || true)
  [[ -n "${post_primary}" ]] || post_primary="unknown"

  write_case_event "${history}" 2 "invoke" "append" \
    "{:op-id \"${post_op_id}\" :key 2 :value \"post\" :primary \"${post_primary}\"}"
  post_status=0
  if psql_vip "
BEGIN ISOLATION LEVEL ${isolation};
INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
VALUES ($(sql_literal "${run_id}"), $(sql_literal "${post_op_id}"), 2, 'post', 2, $(sql_literal "${post_primary}"), $(sql_literal "${isolation}"));
COMMIT;
" >/dev/null 2>>"${failures}"; then
    printf '%s\n' "${post_op_id}" >>"${ack_file}"
    write_case_event "${history}" 2 "ok" "append" \
      "{:op-id \"${post_op_id}\" :key 2 :value \"post\" :primary \"${post_primary}\"}"
  else
    post_status=$?
    write_case_event "${history}" 2 "fail" "append" \
      "{:op-id \"${post_op_id}\" :key 2 :value \"post\" :primary \"${post_primary}\"}"
  fi

  jq -n \
    --arg runId "${run_id}" \
    --arg isolation "${isolation}" \
    --arg initialPrimary "${observed_primary}" \
    --arg finalPrimary "${post_primary}" \
    --arg openOpId "${open_op_id}" \
    --arg preOpId "${pre_op_id}" \
    --arg postOpId "${post_op_id}" \
    --arg startedAt "${open_started_at}" \
    --arg finishedAt "${open_finished_at}" \
    --arg output "${output}" \
    --argjson sleepSeconds "${open_sleep}" \
    --argjson openExitStatus "${status}" \
    --argjson postExitStatus "${post_status}" \
    '{
      workload: "open-transaction-failover",
      runId: $runId,
      isolation: $isolation,
      initialPrimary: $initialPrimary,
      finalPrimary: $finalPrimary,
      preOpId: $preOpId,
      openOpId: $openOpId,
      postOpId: $postOpId,
      openStartedAt: $startedAt,
      openFinishedAt: $finishedAt,
      openSleepSeconds: $sleepSeconds,
      openExitStatus: $openExitStatus,
      postExitStatus: $postExitStatus,
      output: $output
    }' >"${metadata_file}"

  [[ "${post_status}" -eq 0 ]]
}

run_vip_routing_workload() {
  local run_id=$1
  local case_dir=$2
  local isolation=${3:-read committed}
  local history="${case_dir}/history.edn"
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local failures="${case_dir}/failures.log"
  local route_file="${case_dir}/vip-routing.jsonl"
  local duration=$((jepsen_default_duration + jepsen_nemesis_hold_seconds + 4))
  local deadline=$((SECONDS + duration))
  local op=0
  local ok_count=0

  : >"${ack_file}"
  : >"${failures}"
  : >"${route_file}"

  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    op=$((op + 1))
    local client=$(( (op - 1) % jepsen_default_clients ))
    local key=$(( (op - 1) % jepsen_default_keys ))
    local op_id="${run_id}-vip-routing-${op}"
    local value="route-${op}"
    local observed_at primary_before primary_after vip_before vip_after output status in_recovery server_addr returned_op

    observed_at="$(timestamp_utc)"
    primary_before=$(current_primary_name 2>/dev/null || true)
    [[ -n "${primary_before}" ]] || primary_before="unknown"
    vip_before=$(vip_holder_member 2>/dev/null || true)
    [[ -n "${vip_before}" ]] || vip_before="unknown"

    write_case_event "${history}" "${client}" "invoke" "vip-routing" \
      "{:op-id \"${op_id}\" :key ${key} :value \"${value}\" :pacman-primary \"${primary_before}\" :vip-holder \"${vip_before}\"}"

    status=0
    output=$(psql_vip "
BEGIN ISOLATION LEVEL ${isolation};
WITH inserted AS (
  INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
  VALUES ($(sql_literal "${run_id}"), $(sql_literal "${op_id}"), ${key}, $(sql_literal "${value}"), ${client}, $(sql_literal "${primary_before}"), $(sql_literal "${isolation}"))
  RETURNING op_id
)
SELECT pg_is_in_recovery(), coalesce(inet_server_addr()::text, ''), op_id FROM inserted;
COMMIT;
" 2>&1) || status=$?

    primary_after=$(current_primary_name 2>/dev/null || true)
    [[ -n "${primary_after}" ]] || primary_after="unknown"
    vip_after=$(vip_holder_member 2>/dev/null || true)
    [[ -n "${vip_after}" ]] || vip_after="unknown"

    in_recovery=""
    server_addr=""
    returned_op=""
    if [[ "${status}" -eq 0 ]]; then
      IFS='|' read -r in_recovery server_addr returned_op <<<"$(printf '%s\n' "${output}" | sed '/^$/d' | tail -n 1)"
      printf '%s\n' "${op_id}" >>"${ack_file}"
      ok_count=$((ok_count + 1))
      write_case_event "${history}" "${client}" "ok" "vip-routing" \
        "{:op-id \"${op_id}\" :key ${key} :value \"${value}\" :pacman-primary-before \"${primary_before}\" :pacman-primary-after \"${primary_after}\" :vip-holder-before \"${vip_before}\" :vip-holder-after \"${vip_after}\" :in-recovery \"${in_recovery}\"}"
    else
      printf '%s\n' "${output}" >>"${failures}"
      write_case_event "${history}" "${client}" "fail" "vip-routing" \
        "{:op-id \"${op_id}\" :key ${key} :value \"${value}\" :pacman-primary-before \"${primary_before}\" :pacman-primary-after \"${primary_after}\" :vip-holder-before \"${vip_before}\" :vip-holder-after \"${vip_after}\"}"
    fi

    append_jsonl "${route_file}" \
      observedAt "$(json_escape "${observed_at}")" \
      opId "$(json_escape "${op_id}")" \
      ok "$([[ "${status}" -eq 0 ]] && printf true || printf false)" \
      status "${status}" \
      pacmanPrimaryBefore "$(json_escape "${primary_before}")" \
      pacmanPrimaryAfter "$(json_escape "${primary_after}")" \
      vipHolderBefore "$(json_escape "${vip_before}")" \
      vipHolderAfter "$(json_escape "${vip_after}")" \
      inRecovery "$([[ "${in_recovery}" == "t" ]] && printf true || printf false)" \
      serverAddr "$(json_escape "${server_addr}")" \
      returnedOp "$(json_escape "${returned_op}")" \
      error "$(json_escape "$([[ "${status}" -eq 0 ]] && printf '' || printf '%s' "${output}")")"

    sleep 1
  done

  [[ "${ok_count}" -gt 0 ]]
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

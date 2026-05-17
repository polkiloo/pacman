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

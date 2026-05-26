check_single_writable_primary() {
  local case_dir=$1

  go run "${repo_root}/tools/jepsenctl" checkers single-primary \
    --case-dir "${case_dir}"
}

check_acknowledged_write_preservation() {
  local workload=$1
  local run_id=$2
  local case_dir=$3
  local checker_file="${case_dir}/acknowledged-write-checker.json"
  local ack_file="${case_dir}/acknowledged-op-ids.txt"
  local counts_file="${case_dir}/final-primary-op-counts.tsv"
  local table final_primary final_primary_service query_status

  table=$(workload_op_table "${workload}") || {
    cat >"${checker_file}" <<EOF
{"checker":"acknowledged-write-preservation","valid":false,"error":"unsupported workload","workload":"${workload}"}
EOF
    return 2
  }

  touch "${ack_file}"

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

  go run "${repo_root}/tools/jepsenctl" checkers acknowledged-write \
    --workload "${workload}" \
    --run-id "${run_id}" \
    --case-dir "${case_dir}" \
    --table "${table}" \
    --final-primary "${final_primary}" \
    --final-primary-service "${final_primary_service}" \
    --async-loss-allowed="${jepsen_allow_async_loss}"
}

check_timeline_convergence() {
  local case_dir=$1

  go run "${repo_root}/tools/jepsenctl" checkers timeline \
    --case-dir "${case_dir}"
}

check_old_primary_rejoin_after_failover() {
  local case_dir=$1
  local nemesis=${2:-}

  go run "${repo_root}/tools/jepsenctl" checkers old-primary-rejoin \
    --case-dir "${case_dir}" \
    --nemesis "${nemesis}"
}

check_manual_switchover() {
  local nemesis=$1
  local case_dir=$2

  go run "${repo_root}/tools/jepsenctl" checkers manual-switchover \
    --case-dir "${case_dir}" \
    --nemesis "${nemesis}"
}

check_client_traffic_during_nemesis() {
  local nemesis=$1
  local case_dir=$2
  local checker_file="${case_dir}/client-traffic-during-nemesis-checker.json"
  local sample_file="${case_dir}/client-traffic-during-nemesis.jsonl"

  if [[ "${nemesis}" != "primary-dcs-partition" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"client-traffic-during-nemesis","valid":true,"applicable":false}
EOF
    return 0
  fi

  if [[ ! -s "${sample_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"client-traffic-during-nemesis","valid":false,"applicable":true,"error":"missing client traffic probe samples"}
EOF
    return 1
  fi

  jq -s '
    {
      checker: "client-traffic-during-nemesis",
      valid: (map(select(.ok == true)) | length > 0),
      applicable: true,
      samples: length,
      successfulSamples: (map(select(.ok == true)) | length),
      failedSamples: (map(select(.ok != true)) | length),
      observations: .
    }
  ' "${sample_file}" >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

check_replication_traffic_during_nemesis() {
  local nemesis=$1
  local case_dir=$2
  local checker_file="${case_dir}/replication-traffic-during-nemesis-checker.json"
  local sample_file="${case_dir}/replication-traffic-during-nemesis.jsonl"

  if [[ "${nemesis}" != "primary-dcs-partition" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"replication-traffic-during-nemesis","valid":true,"applicable":false}
EOF
    return 0
  fi

  if [[ ! -s "${sample_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"replication-traffic-during-nemesis","valid":false,"applicable":true,"error":"missing replication health probe samples"}
EOF
    return 1
  fi

  jq -s '
    {
      checker: "replication-traffic-during-nemesis",
      valid: (map(select(.ok == true and (.streamingReplicas // 0) >= 2)) | length > 0),
      applicable: true,
      samples: length,
      healthySamples: (map(select(.ok == true and (.streamingReplicas // 0) >= 2)) | length),
      observations: .
    }
  ' "${sample_file}" >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

check_dcs_traffic_during_nemesis() {
  local nemesis=$1
  local case_dir=$2
  local checker_file="${case_dir}/dcs-traffic-during-nemesis-checker.json"
  local sample_file="${case_dir}/dcs-traffic-during-nemesis.jsonl"

  if [[ "${nemesis}" != "primary-replication-partition" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"dcs-traffic-during-nemesis","valid":true,"applicable":false}
EOF
    return 0
  fi

  if [[ ! -s "${sample_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"dcs-traffic-during-nemesis","valid":false,"applicable":true,"error":"missing DCS health probe samples"}
EOF
    return 1
  fi

  jq -s '
    {
      checker: "dcs-traffic-during-nemesis",
      valid: (map(select(.ok == true)) | length > 0),
      applicable: true,
      samples: length,
      healthySamples: (map(select(.ok == true)) | length),
      observations: .
    }
  ' "${sample_file}" >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

check_dcs_quorum_during_nemesis() {
  local nemesis=$1
  local case_dir=$2

  go run "${repo_root}/tools/jepsenctl" checkers dcs-quorum \
    --nemesis "${nemesis}" \
    --case-dir "${case_dir}" \
    --min-slow-latency-ms "${jepsen_dcs_slow_min_latency_ms}"
}

check_failover_chain() {
  local nemesis=$1
  local case_dir=$2
  local checker_file="${case_dir}/failover-chain-checker.json"
  local chain_file="${case_dir}/failover-chain.jsonl"
  local observation_file="${case_dir}/primary-observations.jsonl"

  if [[ "${nemesis}" != "failover-chain" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"failover-chain","valid":true,"applicable":false}
EOF
    return 0
  fi

  if [[ ! -s "${chain_file}" || ! -s "${observation_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"failover-chain","valid":false,"applicable":true,"error":"missing failover chain metadata or primary observations"}
EOF
    return 1
  fi

  jq -n \
    --slurpfile steps "${chain_file}" \
    --slurpfile observations "${observation_file}" '
      def writable_members:
        map(select(.reachable == true and .writable == true) | .member)
        | unique
        | sort;
      ($steps | map(select((.exitStatus // 1) == 0))) as $successfulSteps
      | ($observations | writable_members) as $writableMembers
      | {
          checker: "failover-chain",
          valid: (
            ($steps | length) >= 2
            and ($successfulSteps | length) == ($steps | length)
            and (["alpha-1", "alpha-2", "alpha-3"] - $writableMembers | length) == 0
          ),
          applicable: true,
          steps: ($steps | length),
          successfulSteps: ($successfulSteps | length),
          writablePrimaryMembers: $writableMembers,
          chain: $steps
        }
    ' >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

check_open_transaction_during_failover() {
  local workload=$1
  local case_dir=$2
  local checker_file="${case_dir}/open-transaction-checker.json"
  local metadata_file="${case_dir}/open-transaction.json"
  local ack_file="${case_dir}/acknowledged-op-ids.txt"

  if [[ "${workload}" != "open-transaction-failover" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"open-transaction-during-failover","valid":true,"applicable":false}
EOF
    return 0
  fi

  if [[ ! -s "${metadata_file}" || ! -s "${ack_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"open-transaction-during-failover","valid":false,"applicable":true,"error":"missing open transaction metadata or acknowledged writes"}
EOF
    return 1
  fi

  jq -n \
    --slurpfile metadata "${metadata_file}" \
    --argjson acknowledged "$(json_array_from_file "${ack_file}")" '
      ($metadata[0] // {}) as $meta
      | ($acknowledged | index($meta.preOpId // "")) as $preAcked
      | ($acknowledged | index($meta.openOpId // "")) as $openAcked
      | ($acknowledged | index($meta.postOpId // "")) as $postAcked
      | (($meta.openExitStatus // 1) == 0) as $openCommitted
      | {
          checker: "open-transaction-during-failover",
          valid: (
            ($preAcked != null)
            and ($postAcked != null)
            and (
              ($openCommitted and ($openAcked != null))
              or (($openCommitted | not) and ($openAcked == null))
            )
          ),
          applicable: true,
          preAcked: ($preAcked != null),
          openCommitted: $openCommitted,
          openAcked: ($openAcked != null),
          postAcked: ($postAcked != null),
          metadata: $meta
        }
    ' >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

check_vip_write_routing() {
  local workload=$1
  local nemesis=$2
  local case_dir=$3

  go run "${repo_root}/tools/jepsenctl" checkers vip-routing \
    --workload "${workload}" \
    --case-dir "${case_dir}" \
    --nemesis "${nemesis}"
}

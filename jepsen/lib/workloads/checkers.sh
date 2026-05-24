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

check_old_primary_rejoin_after_failover() {
  local case_dir=$1
  local nemesis=${2:-}
  local observation_file="${case_dir}/primary-observations.jsonl"
  local checker_file="${case_dir}/old-primary-rejoin-checker.json"

  if [[ "${nemesis}" == "switchover" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"old-primary-rejoin-after-failover","valid":true,"applicable":false,"observations":0,"samples":0,"reason":"manual switchover is covered by the manual switchover checker"}
EOF
    return 0
  fi

  if [[ ! -s "${observation_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"old-primary-rejoin-after-failover","valid":false,"applicable":false,"observations":0,"samples":0,"error":"missing primary observations"}
EOF
    return 1
  fi

  jq -s --arg nemesis "${nemesis}" '
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
    | (if $initialSample == null then null else primary_of($initialSample) end) as $initialPrimary
    | (if $finalSample == null then null else primary_of($finalSample) end) as $finalPrimary
    | (($initialPrimary != null) and ($finalPrimary != null) and ($initialPrimary.member != $finalPrimary.member)) as $promotionObserved
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
          ($oldPrimaryFinalState.reachable == true)
          and ($oldPrimaryFinalState.writable == false)
          and ($oldPrimaryFinalState.inRecovery == true)
          and (($oldPrimaryFinalState.timeline // 0) == ($finalPrimary.timeline // 0))
        end
      ) as $oldPrimaryRejoined
    | (
        if ($promotionObserved | not) then true
        elif (($nemesis == "kill") or ($nemesis == "packet,kill") or ($nemesis == "repeated-failure")) then
          ($oldPrimaryFinalState != null)
          and (
            ($oldPrimaryFinalState.reachable == false)
            or (
              ($oldPrimaryFinalState.writable == false)
              and (
                ($oldPrimaryFinalState.inRecovery == true)
                or (($oldPrimaryFinalState.timeline // 0) == ($finalPrimary.timeline // 0))
              )
            )
          )
        else $oldPrimaryRejoined
        end
      ) as $oldPrimarySafeOrRejoined
    | {
        checker: "old-primary-rejoin-after-failover",
        valid: (
          ($samples | length) > 0
          and (
            ($promotionObserved | not)
            or $oldPrimarySafeOrRejoined
          )
        ),
        applicable: $promotionObserved,
        nemesis: $nemesis,
        observations: length,
        samples: ($samples | length),
        promotionObserved: $promotionObserved,
        initialPrimary: (if $initialPrimary == null then null else ($initialPrimary | summarize_member) end),
        finalPrimary: (if $finalPrimary == null then null else ($finalPrimary | summarize_member) end),
        oldPrimaryRejoined: $oldPrimaryRejoined,
        oldPrimarySafeOrRejoined: $oldPrimarySafeOrRejoined,
        oldPrimaryFinalState: (if $oldPrimaryFinalState == null then null else ($oldPrimaryFinalState | summarize_member) end)
      }
  ' "${observation_file}" >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

check_manual_switchover() {
  local nemesis=$1
  local case_dir=$2
  local checker_file="${case_dir}/manual-switchover-checker.json"
  local operation_file="${case_dir}/manual-switchover.json"
  local observation_file="${case_dir}/primary-observations.jsonl"

  if [[ "${nemesis}" != "switchover" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"manual-switchover","valid":true,"applicable":false}
EOF
    return 0
  fi

  if [[ ! -s "${operation_file}" || ! -s "${observation_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"manual-switchover","valid":false,"applicable":true,"error":"missing switchover operation metadata or primary observations"}
EOF
    return 1
  fi

  jq -n \
    --slurpfile operation "${operation_file}" \
    --slurpfile observations "${observation_file}" '
      def samples:
        $observations
        | sort_by(.sampleId)
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

      ($operation[0] // {}) as $op
      | samples as $samples
      | ($samples[-1] // null) as $finalSample
      | (if $finalSample == null then null else primary_of($finalSample) end) as $finalPrimary
      | (($op.candidate // "") != "") as $hasCandidate
      | (($op.exitStatus // 1) == 0) as $requestAccepted
      | ($hasCandidate and $requestAccepted and $finalPrimary != null and ($finalPrimary.member == $op.candidate)) as $valid
      | {
          checker: "manual-switchover",
          valid: $valid,
          applicable: true,
          requestedAt: ($op.requestedAt // null),
          candidate: ($op.candidate // ""),
          controlService: ($op.controlService // ""),
          exitStatus: ($op.exitStatus // null),
          requestAccepted: $requestAccepted,
          finalPrimary: (if $finalPrimary == null then null else ($finalPrimary | summarize_member) end),
          output: ($op.output // "")
        }
    ' >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
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
  local checker_file="${case_dir}/dcs-quorum-checker.json"
  local sample_file="${case_dir}/dcs-quorum-during-nemesis.jsonl"

  if [[ "${nemesis}" != "dcs-kill-one" && "${nemesis}" != "dcs-lose-majority" && "${nemesis}" != "primary-dcs-majority-partition" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"dcs-quorum-during-nemesis","valid":true,"applicable":false}
EOF
    return 0
  fi

  if [[ ! -s "${sample_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"dcs-quorum-during-nemesis","valid":false,"applicable":true,"error":"missing DCS quorum probe samples"}
EOF
    return 1
  fi

  jq -s --arg nemesis "${nemesis}" '
    def phase($name): map(select(.phase == $name));
    (
      if $nemesis == "dcs-lose-majority" then phase("before-majority-loss")
      elif $nemesis == "primary-dcs-majority-partition" then phase("before-primary-majority-partition")
      else phase("before-kill")
      end
    ) as $before
    | (
      if $nemesis == "dcs-lose-majority" then phase("during-majority-loss")
      elif $nemesis == "primary-dcs-majority-partition" then phase("during-primary-majority-partition")
      else phase("during-kill")
      end
    ) as $during
    | (
      if $nemesis == "primary-dcs-majority-partition" then phase("after-primary-majority-partition")
      else phase("after-restart")
      end
    ) as $after
    | (
        if $nemesis == "dcs-lose-majority" then
          $during | map(select(
            .ok == true
            and (.healthyEndpoints // 0) <= 1
            and (.failedEndpoints // 0) >= 2
            and (.targetCount // 0) >= 2
            and (.runningTargets // 0) == 0
            and .targetRunning == false
          ))
        elif $nemesis == "primary-dcs-majority-partition" then
          $during | map(select(
            .ok == true
            and (.healthyEndpoints // 0) <= 1
            and (.failedEndpoints // 0) >= 2
            and (.targetCount // 0) >= 2
            and (.runningTargets // 0) == (.targetCount // 0)
            and .targetRunning == true
          ))
        else
          $during | map(select(
            .ok == true
            and (.healthyEndpoints // 0) >= 2
            and (.failedEndpoints // 0) >= 1
            and .targetRunning == false
          ))
        end
      ) as $duringExpected
    | ($after | map(select(
        .ok == true
        and (.healthyEndpoints // 0) == (.totalEndpoints // 0)
        and (.totalEndpoints // 0) >= 3
        and .targetRunning == true
      ))) as $afterRecovered
    | {
        checker: "dcs-quorum-during-nemesis",
        valid: (($duringExpected | length) > 0 and ($afterRecovered | length) > 0),
        applicable: true,
        nemesis: $nemesis,
        samples: length,
        beforeSamples: ($before | length),
        duringExpectedSamples: ($duringExpected | length),
        afterRecoveredSamples: ($afterRecovered | length),
        observations: .
      }
  ' "${sample_file}" >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
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
  local checker_file="${case_dir}/vip-routing-checker.json"
  local route_file="${case_dir}/vip-routing.jsonl"

  if [[ "${workload}" != "vip-routing" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"vip-write-routing","valid":true,"applicable":false}
EOF
    return 0
  fi

  if [[ ! -s "${route_file}" ]]; then
    cat >"${checker_file}" <<'EOF'
{"checker":"vip-write-routing","valid":false,"applicable":true,"error":"missing VIP routing samples"}
EOF
    return 1
  fi

  jq -s --arg nemesis "${nemesis}" '
    def known($value): (($value // "") != "" and ($value // "") != "unknown");
    def stable:
      known(.pacmanPrimaryBefore)
      and known(.pacmanPrimaryAfter)
      and known(.vipHolderBefore)
      and known(.vipHolderAfter)
      and .pacmanPrimaryBefore == .pacmanPrimaryAfter
      and .vipHolderBefore == .vipHolderAfter;
    def successful_stable_matches:
      map(select(
        .ok == true
        and (.inRecovery == false)
        and stable
        and .pacmanPrimaryBefore == .vipHolderBefore
      ));
    def routed_to_replica_violations:
      map(select(.ok == true and .inRecovery == true));
    def stable_primary_mismatch_violations:
      map(select(
        .ok == true
        and stable
        and .pacmanPrimaryBefore != .vipHolderBefore
      ));

    successful_stable_matches as $matches
    | routed_to_replica_violations as $replicaViolations
    | stable_primary_mismatch_violations as $mismatchViolations
    | ($matches | map(.pacmanPrimaryBefore) | unique | sort) as $matchedPrimaries
    | {
        checker: "vip-write-routing",
        valid: (
          (map(select(.ok == true)) | length) > 0
          and ($replicaViolations | length) == 0
          and ($mismatchViolations | length) == 0
          and (
            if $nemesis == "switchover"
            then ($matchedPrimaries | length) >= 2
            else ($matchedPrimaries | length) >= 1
            end
          )
        ),
        applicable: true,
        samples: length,
        successfulWrites: (map(select(.ok == true)) | length),
        failedWrites: (map(select(.ok != true)) | length),
        matchedPrimaryMembers: $matchedPrimaries,
        routedToReplicaViolations: $replicaViolations,
        stablePrimaryMismatchViolations: $mismatchViolations,
        observations: .
      }
  ' "${route_file}" >"${checker_file}"

  jq -e '.valid == true' "${checker_file}" >/dev/null
}

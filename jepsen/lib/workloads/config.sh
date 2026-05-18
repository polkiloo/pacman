jepsen_pg_client_service="${PACMAN_JEPSEN_PG_CLIENT_SERVICE:-pacman-primary}"
jepsen_pg_host="${PACMAN_JEPSEN_PG_HOST:-172.28.0.100}"
jepsen_pg_port="${PACMAN_JEPSEN_PG_PORT:-5432}"
jepsen_pg_user="${PACMAN_JEPSEN_PG_USER:-postgres}"
jepsen_pg_password="${PACMAN_JEPSEN_PG_PASSWORD:-pacman-demo-password}"
jepsen_pg_database="${PACMAN_JEPSEN_PG_DATABASE:-postgres}"
jepsen_vip_interface="${PACMAN_JEPSEN_VIP_INTERFACE:-eth0}"
jepsen_default_ops="${PACMAN_JEPSEN_WORKLOAD_OPS:-12}"
jepsen_default_duration="${PACMAN_JEPSEN_WORKLOAD_DURATION_SECONDS:-20}"
jepsen_default_clients="${PACMAN_JEPSEN_WORKLOAD_CLIENTS:-3}"
jepsen_default_keys="${PACMAN_JEPSEN_WORKLOAD_KEYS:-3}"
jepsen_nemesis_hold_seconds="${PACMAN_JEPSEN_NEMESIS_HOLD_SECONDS:-8}"
jepsen_post_nemesis_settle_seconds="${PACMAN_JEPSEN_POST_NEMESIS_SETTLE_SECONDS:-10}"
jepsen_primary_sample_interval="${PACMAN_JEPSEN_PRIMARY_SAMPLE_INTERVAL_SECONDS:-1}"
jepsen_allow_async_loss="${PACMAN_JEPSEN_ALLOW_ASYNC_LOSS:-false}"
jepsen_append_switchover_op_delay="${PACMAN_JEPSEN_APPEND_SWITCHOVER_OP_DELAY_SECONDS:-1}"
jepsen_dcs_kill_service="${PACMAN_JEPSEN_DCS_KILL_SERVICE:-pacman-dcs-2}"
jepsen_smoke_cases_default="append-smoke:none"
jepsen_nightly_cases_default="append-smoke:none append-switchover:switchover append-failover:kill append-failover:packet append-failover:packet,kill append-failover:primary-dcs-partition append-failover:primary-replication-partition append-failover:failover-chain open-transaction-failover:kill vip-routing:switchover append-dcs-quorum:dcs-kill-one single-key-register:packet read-committed-txn:slow-network serializable-txn:packet,kill append-failover:repeated-failure"

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
append-switchover-switchover append-switchover:switchover Append workload while requesting a manual PACMAN switchover.
append-failover-kill append-failover:kill Append workload while killing current primary PostgreSQL.
append-failover-packet append-failover:packet Append workload while partitioning the current primary.
append-failover-packet-kill append-failover:packet,kill Append workload while partitioning and killing the current primary.
append-failover-primary-dcs-partition append-failover:primary-dcs-partition Append workload while isolating the current primary from DCS only.
append-failover-primary-replication-partition append-failover:primary-replication-partition Append workload while blocking primary replication traffic only.
append-failover-failover-chain append-failover:failover-chain Append workload while chaining manual failovers across all three data nodes.
open-transaction-failover-kill open-transaction-failover:kill Hold a transaction open while killing the current primary.
vip-routing-switchover vip-routing:switchover Verify vip-manager routes writes only to the current PACMAN primary during switchover.
append-dcs-quorum-dcs-kill-one append-dcs-quorum:dcs-kill-one Append workload while killing one etcd DCS member.
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
    append-switchover-switchover | append-switchover:switchover) printf 'append-switchover:switchover\n' ;;
    append-failover-kill | append-failover:kill) printf 'append-failover:kill\n' ;;
    append-failover-packet | append-failover:packet) printf 'append-failover:packet\n' ;;
    append-failover-packet-kill | append-failover:packet,kill) printf 'append-failover:packet,kill\n' ;;
    append-failover-primary-dcs-partition | append-failover:primary-dcs-partition) printf 'append-failover:primary-dcs-partition\n' ;;
    append-failover-primary-replication-partition | append-failover:primary-replication-partition) printf 'append-failover:primary-replication-partition\n' ;;
    append-failover-failover-chain | append-failover:failover-chain) printf 'append-failover:failover-chain\n' ;;
    open-transaction-failover-kill | open-transaction-failover:kill) printf 'open-transaction-failover:kill\n' ;;
    vip-routing-switchover | vip-routing:switchover) printf 'vip-routing:switchover\n' ;;
    append-dcs-quorum-dcs-kill-one | append-dcs-quorum:dcs-kill-one) printf 'append-dcs-quorum:dcs-kill-one\n' ;;
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

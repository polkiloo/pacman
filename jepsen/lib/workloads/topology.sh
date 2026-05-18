current_primary_name() {
  pacman_cluster_status_json pacman-primary |
    jq -r '.currentPrimary // .current_primary // ""'
}

switchover_candidate_name() {
  pacman_cluster_status_json pacman-primary |
    jq -r '
      (.currentPrimary // .current_primary // "") as $primary
      | [
          .members[]
          | select((.name // "") != $primary)
          | select(.healthy == true)
          | select(((.needsRejoin // false) | not))
          | select(((.role // "") == "replica") or ((.role // "") == "standby"))
          | select(((.state // "") == "streaming") or ((.state // "") == "running"))
          | .name
        ][0] // ""
    '
}

member_switchover_candidate_ready() {
  local member=$1

  pacman_cluster_status_json pacman-primary |
    jq -e --arg member "${member}" '
      (.currentPrimary // .current_primary // "") as $primary
      | any(
          .members[];
          (.name // "") == $member
          and (.name // "") != $primary
          and .healthy == true
          and (((.needsRejoin // false) | not))
          and (((.role // "") == "replica") or ((.role // "") == "standby"))
          and (((.state // "") == "streaming") or ((.state // "") == "running"))
        )
    ' >/dev/null
}

wait_for_switchover_candidate() {
  local member=$1
  local timeout=${2:-60}
  local deadline=$((SECONDS + timeout))

  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    if member_switchover_candidate_ready "${member}"; then
      return 0
    fi
    sleep 1
  done

  return 1
}

wait_for_current_primary() {
  local member=$1
  local timeout=${2:-60}
  local deadline=$((SECONDS + timeout))
  local current

  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    current=$(current_primary_name 2>/dev/null || true)
    if [[ "${current}" == "${member}" ]]; then
      return 0
    fi
    sleep 1
  done

  return 1
}

request_manual_switchover() {
  local candidate=$1
  local service=${2:-pacman-primary}

  compose_exec "${service}" env \
    "PACMANCTL_API_URL=http://${service}:8080" \
    "PACMANCTL_API_TOKEN=lab-admin-token" \
    pacmanctl cluster switchover \
      -candidate "${candidate}" \
      -reason "jepsen-manual-switchover" \
      -requested-by "jepsen" \
      -force
}

service_for_member() {
  case "$1" in
    alpha-1) printf 'pacman-primary\n' ;;
    alpha-2) printf 'pacman-replica\n' ;;
    alpha-3) printf 'pacman-replica-2\n' ;;
    *) return 1 ;;
  esac
}

member_for_service() {
  case "$1" in
    pacman-primary) printf 'alpha-1\n' ;;
    pacman-replica) printf 'alpha-2\n' ;;
    pacman-replica-2) printf 'alpha-3\n' ;;
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

dcs_services() {
  printf 'pacman-dcs pacman-dcs-2 pacman-dcs-3\n'
}

dcs_member_for_service() {
  case "$1" in
    pacman-dcs) printf 'alpha-dcs\n' ;;
    pacman-dcs-2) printf 'alpha-dcs-2\n' ;;
    pacman-dcs-3) printf 'alpha-dcs-3\n' ;;
    *) return 1 ;;
  esac
}

dcs_client_endpoints() {
  printf 'http://pacman-dcs:2379,http://pacman-dcs-2:2379,http://pacman-dcs-3:2379\n'
}

dcs_initial_cluster() {
  printf 'alpha-dcs=http://pacman-dcs:2380,alpha-dcs-2=http://pacman-dcs-2:2380,alpha-dcs-3=http://pacman-dcs-3:2380\n'
}

vip_holder_member() {
  local service member output

  for service in pacman-primary pacman-replica pacman-replica-2; do
    if output=$(compose_exec "${service}" /bin/sh -lc "ip -o -4 addr show dev '${jepsen_vip_interface}' | grep -q ' ${jepsen_pg_host}/'" 2>&1); then
      member=$(member_for_service "${service}") || return 1
      printf '%s\n' "${member}"
      return 0
    fi
  done

  return 1
}

service_ip() {
  case "$1" in
    pacman-primary) printf '172.28.0.11\n' ;;
    pacman-replica) printf '172.28.0.12\n' ;;
    pacman-replica-2) printf '172.28.0.13\n' ;;
    pacman-dcs) printf '172.28.0.10\n' ;;
    pacman-dcs-2) printf '172.28.0.14\n' ;;
    pacman-dcs-3) printf '172.28.0.15\n' ;;
    *) return 1 ;;
  esac
}

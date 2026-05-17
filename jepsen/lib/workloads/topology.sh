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

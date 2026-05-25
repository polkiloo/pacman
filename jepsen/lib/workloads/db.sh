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

wait_for_vip_writable() {
  local timeout=${1:-60}
  local deadline=$((SECONDS + timeout))

  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    if psql_vip "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  return 1
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

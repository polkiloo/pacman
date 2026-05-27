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

jepsenctl_cmd() {
  if command -v jepsenctl >/dev/null 2>&1; then
    jepsenctl "$@"
    return $?
  fi

  go run "${repo_root}/tools/jepsenctl" "$@"
}

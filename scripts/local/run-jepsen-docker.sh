#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: scripts/local/run-jepsen-docker.sh smoke|nightly|case [case-name|workload:nemesis]

Runs the PACMAN Jepsen campaign from a Dockerized control-node container.

Environment:
  PACMAN_JEPSEN_DOCKER_IMAGE       runner image tag (default: pacman-jepsen-runner:local)
  PACMAN_JEPSEN_DOCKER_BUILD       set false to skip image build
  PACMAN_JEPSEN_DOCKER_DRY_RUN     set true to print commands without running them
  PACMAN_JEPSEN_DIR                harness path inside the repo (default: <repo>/jepsen)
  PACMAN_JEPSEN_CASES              space-separated workload:nemesis cases
  PACMAN_JEPSEN_CASE               single case for the case campaign
EOF
}

campaign="${1:-}"
case "${campaign}" in
  smoke | nightly | case)
    ;;
  *)
    usage
    exit 2
    ;;
esac

if [[ "${campaign}" == "case" ]]; then
  export PACMAN_JEPSEN_CASE="${2:-${PACMAN_JEPSEN_CASE:-}}"
  if [[ -z "${PACMAN_JEPSEN_CASE}" ]]; then
    usage
    if [[ -x "./jepsen/bin/list-cases" ]]; then
      echo >&2
      echo "Supported cases:" >&2
      ./jepsen/bin/list-cases >&2
    fi
    exit 2
  fi
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
image="${PACMAN_JEPSEN_DOCKER_IMAGE:-pacman-jepsen-runner:local}"
dockerfile="${PACMAN_JEPSEN_DOCKERFILE:-${repo_root}/deploy/jepsen/Dockerfile}"
build_image="${PACMAN_JEPSEN_DOCKER_BUILD:-true}"
dry_run="${PACMAN_JEPSEN_DOCKER_DRY_RUN:-false}"
docker_sock="${PACMAN_JEPSEN_DOCKER_SOCKET:-/var/run/docker.sock}"

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to run local Jepsen campaigns in containers" >&2
    exit 1
  fi
}

run_cmd() {
  if [[ "${dry_run}" == "true" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

require_docker

if [[ "${build_image}" != "false" ]]; then
  run_cmd docker build \
    -f "${dockerfile}" \
    -t "${image}" \
    "${repo_root}"
fi

docker_args=(
  run
  --rm
  -t
  -v "${repo_root}:${repo_root}"
  -w "${repo_root}"
  -e "PACMAN_JEPSEN_DIR=${PACMAN_JEPSEN_DIR:-${repo_root}/jepsen}"
  -e "PACMAN_JEPSEN_ARTIFACT_DIR=${PACMAN_JEPSEN_ARTIFACT_DIR:-${repo_root}/jepsen/store}"
  -e "PACMAN_JEPSEN_CI_ARTIFACT_DIR=${PACMAN_JEPSEN_CI_ARTIFACT_DIR:-${repo_root}/bin/jepsen-ci/${campaign}}"
  -e "PACMAN_JEPSEN_CASES=${PACMAN_JEPSEN_CASES:-}"
  -e "PACMAN_JEPSEN_CASE=${PACMAN_JEPSEN_CASE:-}"
  -e "PACMAN_JEPSEN_WORKLOAD_OPS=${PACMAN_JEPSEN_WORKLOAD_OPS:-}"
  -e "PACMAN_JEPSEN_WORKLOAD_DURATION_SECONDS=${PACMAN_JEPSEN_WORKLOAD_DURATION_SECONDS:-}"
  -e "PACMAN_JEPSEN_NEMESIS_HOLD_SECONDS=${PACMAN_JEPSEN_NEMESIS_HOLD_SECONDS:-}"
)

if [[ -S "${docker_sock}" ]]; then
  docker_args+=(
    -v "${docker_sock}:/var/run/docker.sock"
    -e DOCKER_HOST=unix:///var/run/docker.sock
  )
else
  echo "warning: Docker socket ${docker_sock} was not found; nested lab control may not work" >&2
fi

if [[ -n "${SSH_AUTH_SOCK:-}" && -S "${SSH_AUTH_SOCK}" ]]; then
  docker_args+=(
    -v "${SSH_AUTH_SOCK}:/ssh-agent"
    -e SSH_AUTH_SOCK=/ssh-agent
  )
fi

docker_args+=(
  "${image}"
  "${repo_root}/scripts/ci/run-jepsen.sh"
  "${campaign}"
)

if [[ "${campaign}" == "case" ]]; then
  docker_args+=("${PACMAN_JEPSEN_CASE}")
fi

run_cmd docker "${docker_args[@]}"

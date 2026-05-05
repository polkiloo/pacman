#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 smoke|nightly" >&2
}

campaign="${1:-}"
case "${campaign}" in
  smoke | nightly)
    ;;
  *)
    usage
    exit 2
    ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
jepsen_dir="${PACMAN_JEPSEN_DIR:-${repo_root}/jepsen}"
artifact_dir="${PACMAN_JEPSEN_ARTIFACT_DIR:-${jepsen_dir}/store}"
runner="${jepsen_dir}/bin/ci-${campaign}"

append_summary() {
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '%s\n' "$*" >>"${GITHUB_STEP_SUMMARY}"
  fi
}

notice() {
  echo "$*"
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "::notice::$*"
  fi
}

if [[ ! -d "${jepsen_dir}" ]]; then
  notice "Jepsen harness directory is not present at ${jepsen_dir}; skipping ${campaign} campaign."
  append_summary "### Jepsen ${campaign}"
  append_summary ""
  append_summary "Skipped: Jepsen harness directory is not present yet."
  exit 0
fi

if [[ ! -x "${runner}" ]]; then
  echo "Jepsen harness exists at ${jepsen_dir}, but ${runner} is missing or not executable." >&2
  echo "Add the campaign runner before enabling this CI path for real execution." >&2
  exit 1
fi

if ! command -v lein >/dev/null 2>&1; then
  echo "Leiningen is required for Jepsen ${campaign} campaigns." >&2
  exit 1
fi

mkdir -p "${artifact_dir}"
export PACMAN_JEPSEN_ARTIFACT_DIR="${artifact_dir}"

append_summary "### Jepsen ${campaign}"
append_summary ""
append_summary "Running ${runner}"

cd "${jepsen_dir}"
"${runner}"

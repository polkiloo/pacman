#!/usr/bin/env bash
set -euo pipefail

script_source="${PACMAN_JEPSEN_SCRIPT_SOURCE:-$0}"
if [[ "${PACMAN_JEPSEN_SCRIPT_SNAPSHOT:-false}" != "true" ]]; then
  snapshot_path="${TMPDIR:-/tmp}/pacman-run-jepsen-$PPID-$$.sh"
  cp "${script_source}" "${snapshot_path}"
  chmod +x "${snapshot_path}"
  export PACMAN_JEPSEN_SCRIPT_SNAPSHOT=true
  export PACMAN_JEPSEN_SCRIPT_SOURCE="${script_source}"
  exec bash "${snapshot_path}" "$@"
fi

usage() {
  echo "usage: $0 smoke|nightly|case [case-name|workload:nemesis]" >&2
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

repo_root="$(cd "$(dirname "${PACMAN_JEPSEN_SCRIPT_SOURCE}")/../.." && pwd)"
jepsen_dir="${PACMAN_JEPSEN_DIR:-${repo_root}/jepsen}"
artifact_dir="${PACMAN_JEPSEN_ARTIFACT_DIR:-${jepsen_dir}/store}"
ci_artifact_dir="${PACMAN_JEPSEN_CI_ARTIFACT_DIR:-${repo_root}/bin/jepsen-ci/${campaign}}"
summary_path="${PACMAN_JEPSEN_SUMMARY_PATH:-${ci_artifact_dir}/summary.md}"
artifact_index_path="${ci_artifact_dir}/artifact-index.txt"
runner="${jepsen_dir}/bin/ci-${campaign}"
summary_note=""
summary_status_label=""

notice() {
  echo "$*"
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "::notice::$*"
  fi
}

write_summary() {
  local status="$1"
  local status_label="passed"
  local commit_ref

  if [[ -n "${summary_status_label}" ]]; then
    status_label="${summary_status_label}"
  elif [[ "${status}" -eq 0 ]]; then
    status_label="passed"
  else
    status_label="failed"
  fi

  commit_ref="${GITHUB_SHA:-$(git -C "${repo_root}" rev-parse --short HEAD 2>/dev/null || echo unknown)}"

  mkdir -p "${ci_artifact_dir}"
  go run "${repo_root}/tools/jepsenctl" artifacts summarize \
    --campaign "${campaign}" \
    --case "${PACMAN_JEPSEN_CASE:-}" \
    --status "${status}" \
    --status-label "${status_label}" \
    --harness "${jepsen_dir}" \
    --store "${artifact_dir}" \
    --runner "${runner}" \
    --commit "${commit_ref}" \
    --github-run-id "${GITHUB_RUN_ID:-}" \
    --repo-root "${repo_root}" \
    --summary-path "${summary_path}" \
    --artifact-index-path "${artifact_index_path}" \
    --summary-note "${summary_note}"

  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    cat "${summary_path}" >>"${GITHUB_STEP_SUMMARY}"
  fi
}

if [[ ! -d "${jepsen_dir}" ]]; then
  notice "Jepsen harness directory is not present at ${jepsen_dir}; skipping ${campaign} campaign."
  summary_status_label="skipped"
  summary_note="Skipped because the Jepsen harness directory is not present yet."
  write_summary 0
  exit 0
fi

if [[ ! -x "${runner}" ]]; then
  echo "Jepsen harness exists at ${jepsen_dir}, but ${runner} is missing or not executable." >&2
  echo "Add the campaign runner before enabling this CI path for real execution." >&2
  summary_note="Failed before campaign start because ${runner} is missing or not executable."
  write_summary 1
  exit 1
fi

if ! command -v lein >/dev/null 2>&1; then
  echo "Leiningen is required for Jepsen ${campaign} campaigns." >&2
  summary_note="Failed before campaign start because Leiningen is not installed."
  write_summary 1
  exit 1
fi

mkdir -p "${artifact_dir}"
export PACMAN_JEPSEN_ARTIFACT_DIR="${artifact_dir}"
export PACMAN_JEPSEN_CI_ARTIFACT_DIR="${ci_artifact_dir}"
export PACMAN_JEPSEN_SUMMARY_PATH="${summary_path}"

cd "${jepsen_dir}"
status=0
if [[ "${campaign}" == "case" ]]; then
  "${runner}" "${PACMAN_JEPSEN_CASE}" || status=$?
else
  "${runner}" || status=$?
fi
write_summary "${status}"
exit "${status}"

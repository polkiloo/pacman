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

  if [[ -n "${summary_status_label}" ]]; then
    status_label="${summary_status_label}"
  elif [[ "${status}" -eq 0 ]]; then
    status_label="passed"
  else
    status_label="failed"
  fi

  mkdir -p "${ci_artifact_dir}"
  {
    echo "# Jepsen ${campaign} ${status_label}"
    echo
    echo "- Campaign: \`${campaign}\`"
    if [[ "${campaign}" == "case" ]]; then
      echo "- Case: \`${PACMAN_JEPSEN_CASE}\`"
    fi
    echo "- Status: \`${status_label}\`"
    echo "- Harness: \`${jepsen_dir}\`"
    echo "- Store: \`${artifact_dir}\`"
    echo "- Runner: \`${runner}\`"
    echo "- Commit: \`${GITHUB_SHA:-$(git -C "${repo_root}" rev-parse --short HEAD 2>/dev/null || echo unknown)}\`"
    if [[ -n "${GITHUB_RUN_ID:-}" ]]; then
      echo "- GitHub run: \`${GITHUB_RUN_ID}\`"
    fi
    echo
    if [[ -n "${summary_note}" ]]; then
      echo "## Summary"
      echo
      echo "${summary_note}"
      echo
    elif [[ "${status}" -ne 0 ]]; then
      echo "## Summary"
      echo
      echo "The Jepsen campaign exited with status ${status}. Inspect the HTML report, history, checker output, and node logs in the uploaded artifacts."
      echo
    fi
    echo "## Review Checklist"
    echo
    echo "1. Open this summary first."
    echo "2. Inspect Jepsen HTML reports and checker output."
    echo "3. Inspect \`jepsen-history.edn\`, \`results.edn\`, and \`nemesis-schedule.edn\` around failure windows."
    echo "4. Compare PACMAN cluster/history snapshots with PostgreSQL and DCS logs."
    echo "5. Preserve the seed and full artifact path in any regression issue."
    echo
    echo "## Artifact Index"
    echo
  } >"${summary_path}"

  if [[ -d "${artifact_dir}" ]]; then
    {
      find "${artifact_dir}" -type f \( \
        -name '*.html' -o \
        -name '*history*.edn' -o \
        -name 'history.edn' -o \
        -name 'jepsen-history.edn' -o \
        -name 'results.edn' -o \
        -name 'nemesis-schedule.edn' -o \
        -name 'case-results.jsonl' -o \
        -name 'checker.json' -o \
        -name 'single-primary-checker.json' -o \
        -name 'primary-observations.jsonl' -o \
        -name '*.log' -o \
        -name '*.json' \
      \) | sort
    } >"${artifact_index_path}"

    if [[ -s "${artifact_index_path}" ]]; then
      sed "s#${repo_root}/##" "${artifact_index_path}" | sed 's/^/- `/' | sed 's/$/`/' >>"${summary_path}"
    else
      echo "- No Jepsen report, history, log, or JSON artifacts found under the store path." >>"${summary_path}"
    fi
  else
    echo "- Jepsen store path was not created." >>"${summary_path}"
    : >"${artifact_index_path}"
  fi

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

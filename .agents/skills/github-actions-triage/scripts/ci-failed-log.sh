#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?usage: ci-failed-log.sh <run-id> [repo]}"
REPO="${2:-}"
OUT_DIR=".codex/ci"
SUMMARY_FILE="${OUT_DIR}/run-${RUN_ID}-summary.json"
LOG_FILE="${OUT_DIR}/run-${RUN_ID}-failed.log"
MARKERS_FILE="${OUT_DIR}/run-${RUN_ID}-markers.txt"
WINDOW_FILE="${OUT_DIR}/run-${RUN_ID}-error-window.log"
TAIL_FILE="${OUT_DIR}/run-${RUN_ID}-tail.log"
MARKER_RE='FAIL:|panic:|fatal:|error:|undefined:|no such file|permission denied|race detected|context deadline exceeded|valid\? false|Analysis invalid|Invalid analysis|indeterminate|not linearizable|not valid|history invalid|java\.lang|Exception|AssertionError'

repo_arg=()
if [[ -n "$REPO" ]]; then
  repo_arg=(-R "$REPO")
fi

mkdir -p "$OUT_DIR"

echo "== Run summary =="
gh run view "$RUN_ID" "${repo_arg[@]}" \
  --json name,status,conclusion,workflowName,url,jobs \
  --jq '{
    workflowName,
    status,
    conclusion,
    url,
    failedJobs: [.jobs[] | select(.conclusion=="failure") | {
      name,
      databaseId,
      conclusion,
      steps: [.steps[] | select(.conclusion=="failure") | {
        name,
        conclusion,
        number
      }]
    }]
  }' | tee "$SUMMARY_FILE"

echo
echo "== Capturing failed logs =="
gh run view "$RUN_ID" "${repo_arg[@]}" --log-failed > "$LOG_FILE"
line_count="$(wc -l < "$LOG_FILE" | tr -d ' ')"
byte_count="$(wc -c < "$LOG_FILE" | tr -d ' ')"
echo "saved ${line_count} lines (${byte_count} bytes) to ${LOG_FILE}"

echo
echo "== Error markers =="
grep -nEi "$MARKER_RE" "$LOG_FILE" \
  | head -40 \
  | tee "$MARKERS_FILE" || true

first_marker="$(
  grep -nEi -m 1 "$MARKER_RE" "$LOG_FILE" | cut -d: -f1 || true
)"

echo
echo "== Error window =="
if [[ -n "$first_marker" ]]; then
  start=$(( first_marker > 100 ? first_marker - 100 : 1 ))
  end=$(( first_marker + 100 ))
  sed -n "${start},${end}p" "$LOG_FILE" | tee "$WINDOW_FILE"
else
  echo "no strong marker found; showing first 120 failed-log lines"
  sed -n '1,120p' "$LOG_FILE" | tee "$WINDOW_FILE"
fi

echo
echo "== Final lines =="
tail -80 "$LOG_FILE" | tee "$TAIL_FILE"

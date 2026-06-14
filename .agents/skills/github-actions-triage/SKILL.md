---
name: github-actions-triage
description: Use when inspecting GitHub Actions, CI failures, failed Go tests, workflow logs, or build/lint/test errors. Avoid reading full logs; summarize failed jobs and inspect only relevant snippets.
---

# GitHub Actions Triage

Goal: diagnose CI failures with minimal context usage.

Rules:
1. Never read the full GitHub Actions log first.
2. First identify:
   - workflow name
   - run id
   - failed job
   - failed step
   - exit code
   - relevant command
3. For a known run id, start with `bash .agents/skills/github-actions-triage/scripts/ci-failed-log.sh <run-id> [owner/repo]`.
4. Prefer `gh run view --json jobs` before any log read.
5. If using `gh run view --log-failed`, redirect it to a local file; do not print it directly.
6. Extract only:
   - the failed step
   - 100 lines before the first error
   - 100 lines after the first error
   - final summary lines
7. Search for strong error markers:
   - `FAIL:`
   - `panic:`
   - `fatal:`
   - `error:`
   - `undefined:`
   - `no such file`
   - `permission denied`
   - `race detected`
   - `context deadline exceeded`
   - `valid? false`
   - `Analysis invalid`
   - `indeterminate`
   - `not linearizable`
   - `java.lang`
   - `Exception`
   - `AssertionError`
8. After identifying the failure, inspect only related files.
9. Do not open unrelated packages or docs.
10. Produce a short diagnosis:
   - root cause
   - affected files
   - minimal fix
   - minimal test to rerun

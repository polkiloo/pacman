---
name: change-approval-gate
description: Require a concrete change explanation and explicit user approval before modifying a repository, then implement and verify the approved scope. Use whenever Codex is asked to create, edit, delete, generate, format, refactor, fix, upgrade, or otherwise mutate repository files, dependencies, configuration, tests, documentation, build artifacts, or version-control state.
---

# Change Approval Gate

Apply this gate before every repository mutation.

## Workflow

1. Inspect the repository and relevant external evidence with read-only actions.
2. Explain the proposed change before editing. Include:
   - the observed problem or requested outcome;
   - the root cause or current behavior, when known;
   - the exact behavior and files expected to change;
   - the implementation approach;
   - the verification plan;
   - material risks, assumptions, or alternatives.
3. Ask for explicit user approval of that scope and stop the turn.
4. Do not edit files, run formatters that write, install or update dependencies, generate artifacts, stage changes, commit, push, deploy, or perform any other mutation before approval.
5. After approval, implement the approved scope and run proportionate verification without asking again.
6. Ask for new approval only when evidence requires a materially different solution, additional files or systems outside the explained scope, destructive action, or another externally visible mutation.
7. Report the completed change, verification results, and remaining risk.

## Approval Rules

- Treat requests such as “fix,” “implement,” or “update” as authorization to investigate and propose a change, not approval of an unexplained implementation.
- Require approval after the concrete plan is known. Approval given before the explanation does not satisfy the gate.
- Keep read-only diagnosis moving before approval; do not ask permission merely to inspect.
- Keep the proposal focused. Do not bundle unrelated cleanup.
- Treat approval as limited to the described scope and current task.
- Do not treat implementation approval as permission to commit, push, open a pull request, merge, deploy, or message external parties unless those actions were explicitly included.
- Once approval is explicit, develop the change to completion. Pause only for a material scope change, a required new authority, or an actual blocker.

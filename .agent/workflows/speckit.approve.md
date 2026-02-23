---
description: Approve spec work by verifying acceptance criteria, definition of done, and producing an approval summary with status.
---

## User Input

```text
$ARGUMENTS
```

**Input**: Spec name (optional). Examples: `001-postgres-query-builder-refactor`, `001`, or empty to use current branch.

You **MUST** consider the user input before proceeding. If the user provided a spec name, use it to resolve the feature directory; otherwise use the current branch.

## Outline

1. **Resolve feature directory**:
   - From repo root: run `.specify/scripts/bash/check-prerequisites.sh --json --paths-only` (or `--include-tasks` if tasks are needed). If the user provided a spec name in `$ARGUMENTS`, set env `SPECIFY_FEATURE` to that value (e.g. `001`, or `001-postgres-query-builder-refactor`) so the script resolves FEATURE_DIR from `specs/` (branch fallback uses `SPECIFY_FEATURE` when set). Parse JSON for `FEATURE_DIR` and `REPO_ROOT`. All paths must be absolute.
   - If `$ARGUMENTS` is empty: do not set `SPECIFY_FEATURE`; current branch is used automatically.
   - Ensure `FEATURE_DIR` exists and contains `spec.md`; if not, instruct the user to run `/speckit.specify` or provide a valid spec name and stop.

2. **Ensure all acceptance criteria are met**:
   - Load `spec.md` from FEATURE_DIR and extract every **Acceptance Scenario** / **Acceptance criteria** (e.g. "Given/When/Then" or explicit acceptance lists).
   - For each criterion: Determine whether it is satisfied by the current codebase and tests. Use code search, test names, and implementation under `src/` (and any referenced contracts). Mark each as **Met**, **Not met**, or **Postponed** (with brief reason).
   - If any criterion is **Not met** without being explicitly **Postponed**, the approval status cannot be **APPROVED**.

3. **Check "Definition of Done"**:
   - **Tests**: Run the project test suite (e.g. `pytest`, `uv run pytest`, or project-specific command). Report pass/fail and any failures. DoD requires tests green.
   - **Lints**: Run project linters (e.g. `ruff`, `mypy`, or project config). Report pass/fail. DoD requires lints green.
   - **Public API**: Check for breaking changes to public APIs (e.g. `src/` exports, documented interfaces). If there are breaks, they must be explicitly marked or commented (e.g. deprecation, migration note); otherwise DoD is not met.
   - **Changelog**: Per project changelog skill—changelog must include only user-relevant product changes. For this repo: changes in `src/`, public APIs, domain primitives, behavioral changes. Exclude test-only, CI, docs-only, internal tooling. Report if `CHANGELOG.md` entries match changes under `src/` and are correctly categorized; flag missing or out-of-scope entries.

4. **Generate "Approve summary"** (output in a single structured block):
   - **What's done**: Short list of completed scope (features, stories, tasks) and acceptance criteria met.
   - **What is not done or postponed**: Any deferred items, skipped criteria, or known gaps with one-line justification each.
   - **Risks and limitations**: Known risks, assumptions, or limitations of the current implementation or rollout.

5. **Give a status** (exactly one):
   - **APPROVED**: All acceptance criteria met, DoD fully satisfied (tests green, lints green, no unmarked API breaks, changelog correct).
   - **APPROVED WITH NOTES**: All acceptance criteria met and DoD satisfied, but summary contains non-blocking notes (e.g. minor risks or limitations).
   - **CHANGES REQUESTED**: One or more acceptance criteria not met, or DoD not satisfied (failing tests/lints, unmarked API breaks, or changelog not aligned). List what must be fixed.

## Report format

Output:

1. **Feature**: Spec name and FEATURE_DIR path.
2. **Acceptance criteria**: Table or list with status (Met / Not met / Postponed).
3. **Definition of done**: Tests (pass/fail), Lints (pass/fail), Public API (ok / breaks with marks / breaks without marks), Changelog (ok / needs updates).
4. **Approve summary**: What's done; What's not done or postponed; Risks and limitations.
5. **Status**: APPROVED | APPROVED WITH NOTES | CHANGES REQUESTED.

No repository state changes (commit, push, merge) are performed by this command.

---
description: Propose a publish strategy and exact commands for merging and (optionally) releasing; never push or merge directly—require explicit user confirmation.
---

## Repository State Rule (MANDATORY)

This workflow **MUST NOT** execute `git push`, `git merge`, `git tag` (with push), or any command that modifies remote repository state. It only **proposes** a strategy and **lists exact commands**. The agent must:

1. Present the exact commands that would be executed.
2. Clearly explain the consequences.
3. State that the user must confirm explicitly (e.g. "Proceed", "Confirm", "Execute") before any such command is run.

Without explicit confirmation, stop at preparation only. If the user asks to "run" or "execute" publish, output the plan and commands and ask for confirmation; do not run them yourself.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (e.g. preferred option A/B/C, target version for release, or "recommend").

## Publish options

1. **Option A (recommended)**  
   **Branch → PR → merge (squash)**  
   Best default: open a PR from current branch to main, review, then merge with squash. No direct push to main.

2. **Option B (fast)**  
   **Merge directly to main**  
   Only for small, low-risk edits (e.g. docs, workflows, config). No PR. Use with caution.

3. **Option C (release)**  
   **After merge: bump changelog → tag vX.Y.Z → push tag**  
   Use after work is merged to main. Bump `CHANGELOG.md` for new version, create tag `vX.Y.Z`, push tag. **Pushing the tag triggers the release workflow** (e.g. GHA publishes OCI artifact and creates GitHub Release from changelog).

## Outline

1. **Context**:
   - Determine current branch and whether there are uncommitted changes (e.g. `git status`).
   - If user gave a preferred option or version in `$ARGUMENTS`, use it; otherwise recommend one option with a short reason.

2. **Recommend strategy**:
   - Choose **A**, **B**, or **C** (or a sequence, e.g. "A then C").
   - Explain in one or two sentences why (e.g. "Option A recommended for feature work; Option B only for docs; Option C after merge when ready to release.").

3. **Output exact commands (no execution)**:
   - For **Option A**: Commands to create/update branch, push branch, open PR (e.g. `gh pr create` or link), and merge with squash (e.g. `gh pr merge --squash` or equivalent). Do not run them.
   - For **Option B**: Commands to merge current branch into main (e.g. checkout main, pull, merge, push main). Do not run them.
   - For **Option C**: Commands to update `CHANGELOG.md` (per changelog-release-assistant skill), create tag `vX.Y.Z`, and push the tag. Do not run them.
   - Every command that modifies repo or remote state must be listed explicitly and marked as "do not run until user confirms".

4. **Notice about tag and release**:
   - If Option C (or a sequence including C) is in the plan, state clearly: **"Pushing the tag will trigger the release workflow (e.g. GitHub Actions will publish the OCI artifact and create the GitHub Release from CHANGELOG)."**

5. **Ask for confirmation**:
   - End with a clear prompt: e.g. "If you want to proceed, confirm with 'Proceed' or 'Execute' and I will run only the commands you approve." Or: "Run these commands yourself, or reply with confirmation to have me execute them step by step."

## Report format

Output:

1. **Strategy**: Option (A / B / C or A then C) and short rationale.
2. **Exact commands** (copy-paste ready, with comments):
   - Group by phase (e.g. "Push branch & open PR", "Merge PR", "Tag & push").
   - No execution—explicit note that these are proposed only.
3. **Tag / release notice**: If tagging is in the plan, state that pushing the tag triggers the release workflow.
4. **Confirmation**: Request explicit user confirmation before any push/merge/tag execution.

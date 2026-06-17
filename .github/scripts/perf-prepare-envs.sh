#!/usr/bin/env bash
# Add a worktree at the PR's merge-base and sync both it and HEAD, so the perf
# gate can A/B both refs on the same VM. Reads BASE_REF.
set -euo pipefail

BASE="$(git merge-base HEAD "origin/${BASE_REF}")"
git worktree add /tmp/base-ref "$BASE"
(cd /tmp/base-ref && uv sync --all-groups --all-extras)
uv sync --all-groups --all-extras

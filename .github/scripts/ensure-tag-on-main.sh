#!/usr/bin/env bash
# Refuse to build a release whose tag commit is not contained in main.
# Reads GITHUB_REF_NAME.
set -euo pipefail

git fetch --no-tags origin main:refs/remotes/origin/main

TAG_COMMIT="$(git rev-list -n 1 "${GITHUB_REF_NAME}")"

if git merge-base --is-ancestor "$TAG_COMMIT" "origin/main"; then
	echo "OK: tag commit is on main"
else
	echo "ERROR: tag '${GITHUB_REF_NAME}' points to commit not contained in main"
	echo "Tag commit: $TAG_COMMIT"
	echo "Refuse release build."
	exit 1
fi

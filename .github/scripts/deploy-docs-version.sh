#!/usr/bin/env bash
# Deploy one docs version to the gh-pages branch via mike.
#
# Usage: deploy-docs-version.sh <minor> <move_latest: true|false>
#
# When move_latest is true, also point the `latest` alias and the root redirect
# at this version. Run from anywhere in the repo (it cd's into pages/).
set -euo pipefail

minor="${1:?usage: deploy-docs-version.sh <minor> <move_latest>}"
move_latest="${2:?usage: deploy-docs-version.sh <minor> <move_latest>}"

case "$move_latest" in
true | false) ;;
*)
	echo "::error::move_latest must be 'true' or 'false' (got '$move_latest')"
	exit 1
	;;
esac

cd "$(git rev-parse --show-toplevel)/pages"

if [ "$move_latest" = "true" ]; then
	uv run mike deploy --push --update-aliases --branch gh-pages "$minor" latest
	uv run mike set-default --push --branch gh-pages latest
else
	uv run mike deploy --push --branch gh-pages "$minor"
fi

#!/usr/bin/env bash
# Resolve which docs version to publish and whether to advance the `latest` alias.
#
# Reads : GITHUB_REF_NAME, GITHUB_REF_TYPE
# Writes: `minor`, `move_latest` to $GITHUB_OUTPUT
#
#   - tag `vX.Y.Z`               -> version X.Y (ship / backport)
#   - dispatch on release/vX.Y.* -> version X.Y (post-release polish)
#
# `latest` + the root redirect move only when X.Y is the newest released minor,
# so polishing an older minor refreshes it in place without disturbing `latest`.
set -euo pipefail

ref="${GITHUB_REF_NAME}"

# Newest released minor across all tags (vX.Y.Z, highest wins).
newest_tag="$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
newest_minor="$(printf '%s' "${newest_tag#v}" | cut -d. -f1,2)"

if [ "${GITHUB_REF_TYPE}" = "tag" ]; then
	# Ship (or backport): take MAJOR.MINOR from the tag.
	target_minor="$(printf '%s' "${ref#v}" | cut -d. -f1,2)"
else
	# Polish: the version comes from the release branch name. Polishing an older
	# minor is allowed; the move_latest gate below keeps `latest`/root untouched
	# unless it is the newest minor.
	case "$ref" in
	release/v*) target_minor="$(printf '%s' "${ref#release/v}" | cut -d. -f1,2)" ;;
	*)
		echo "::error::workflow_dispatch must run on a 'release/vX.Y.*' branch (got '$ref')"
		exit 1
		;;
	esac
fi

if [ "$target_minor" = "$newest_minor" ]; then
	move_latest=true
else
	move_latest=false
fi

echo "minor=$target_minor" >>"$GITHUB_OUTPUT"
echo "move_latest=$move_latest" >>"$GITHUB_OUTPUT"
echo "Resolved: minor=$target_minor move_latest=$move_latest (newest=$newest_minor)"

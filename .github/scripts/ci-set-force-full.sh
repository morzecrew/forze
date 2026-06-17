#!/usr/bin/env bash
# Normalize the force_full input into a definite boolean step output.
# Reads FORCE_FULL; writes force_full to $GITHUB_OUTPUT.
set -euo pipefail

if [[ "${FORCE_FULL:-false}" == "true" ]]; then
	echo "force_full=true" >>"$GITHUB_OUTPUT"
else
	echo "force_full=false" >>"$GITHUB_OUTPUT"
fi

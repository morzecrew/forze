#!/usr/bin/env bash
# Write the PEP 440 version (the tag without its leading `v`) to $GITHUB_OUTPUT.
# Reads GITHUB_REF_NAME.
set -euo pipefail

echo "version=${GITHUB_REF_NAME#v}" >>"$GITHUB_OUTPUT"

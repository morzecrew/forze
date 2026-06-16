#!/usr/bin/env bash
# Combine the per-shard coverage data into one report. Never gates here — the
# threshold is enforced by a later step, after the upload.
set -euo pipefail

mv coverage-data/.coverage.* . 2>/dev/null || true
uv run coverage combine
uv run coverage xml || true

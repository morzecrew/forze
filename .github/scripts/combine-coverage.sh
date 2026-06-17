#!/usr/bin/env bash
# Combine the per-shard coverage data into one report. Never gates here — the
# threshold is enforced by a later step, after the upload.
set -euo pipefail

mv coverage-data/.coverage.* . 2>/dev/null || true
# Non-fatal: with no shard files downloaded, combine exits 1 ("no data"). This
# step never gates (the threshold gate runs later), so don't fail the job here.
uv run coverage combine || true
uv run coverage xml || true

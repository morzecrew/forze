#!/usr/bin/env bash
# Interleaved A/B perf gate: base vs head, both on this one VM, INTERLEAVED across
# PERF_GATE_ROUNDS rounds so the runner lottery (between-VM) and within-job drift
# (thermal throttle, a co-tenant arriving) both cancel. The gate compares the
# MEDIAN of each side's per-round `min` — `min` is the cleanest per-run estimate
# of a CPU path (interference only ever slows an iteration), and the median across
# interleaved rounds removes the unlucky-round flakiness a single `min` suffers on
# sub-millisecond benchmarks. Fails on a >15% regression.
set -euo pipefail

ROUNDS="${PERF_GATE_ROUNDS:-3}"
STORAGE="file://${GITHUB_WORKSPACE}/.benchmarks"

for r in $(seq 1 "$ROUNDS"); do
	echo "::group::perf round $r/$ROUNDS"
	# `base` first, then `head`, so the two sides are temporally adjacent. base
	# tolerates a merge-base predating the perf_gate marker (no data); a head
	# failure (the PR's own benches erroring) fails the gate explicitly rather than
	# letting gate_compare.py pass on empty head data.
	(cd /tmp/base-ref && ./.venv/bin/pytest \
		--benchmark-only --benchmark-warmup=on --benchmark-disable-gc \
		--benchmark-storage="$STORAGE" --benchmark-save=base \
		-m perf_gate tests/perf) ||
		echo "::warning::base perf round $r produced no data (ref may predate perf_gate)"
	(cd "${GITHUB_WORKSPACE}" && ./.venv/bin/pytest \
		--benchmark-only --benchmark-warmup=on --benchmark-disable-gc \
		--benchmark-storage="$STORAGE" --benchmark-save=head \
		-m perf_gate tests/perf)
	echo "::endgroup::"
done

./.venv/bin/python tests/perf/gate_compare.py \
	--storage "${GITHUB_WORKSPACE}/.benchmarks" --threshold 15

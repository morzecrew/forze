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
	# may legitimately have NO perf_gate tests (merge-base predates the marker):
	# pytest exits 5 ("no tests collected") there, which we tolerate. Any OTHER
	# non-zero means base actually broke — fail rather than gate against a missing
	# baseline (gate_compare.py would otherwise pass it as "no base"). A head
	# failure fails the gate via set -e for the same reason.
	base_rc=0
	(cd /tmp/base-ref && ./.venv/bin/pytest \
		--benchmark-only --benchmark-warmup=on --benchmark-disable-gc \
		--benchmark-storage="$STORAGE" --benchmark-save=base \
		-m perf_gate tests/perf) || base_rc=$?
	if [ "$base_rc" -eq 5 ]; then
		echo "::warning::base perf round $r collected no perf_gate tests (ref predates the marker)"
	elif [ "$base_rc" -ne 0 ]; then
		echo "::error::base perf round $r failed (pytest exit $base_rc); refusing to gate without a valid baseline"
		exit 1
	fi
	(cd "${GITHUB_WORKSPACE}" && ./.venv/bin/pytest \
		--benchmark-only --benchmark-warmup=on --benchmark-disable-gc \
		--benchmark-storage="$STORAGE" --benchmark-save=head \
		-m perf_gate tests/perf)
	echo "::endgroup::"
done

./.venv/bin/python tests/perf/gate_compare.py \
	--storage "${GITHUB_WORKSPACE}/.benchmarks" --threshold 15

#!/usr/bin/env bash
# Interleaved A/B perf gate: base vs head, both on this one VM, INTERLEAVED across
# PERF_GATE_ROUNDS rounds so the runner lottery (between-VM) and within-job drift
# (thermal throttle, a co-tenant arriving) both cancel. The gate compares the
# MEDIAN of each side's per-round `min` — `min` is the cleanest per-run estimate
# of a CPU path (interference only ever slows an iteration), and the median across
# interleaved rounds removes the unlucky-round flakiness a single `min` suffers on
# sub-millisecond benchmarks. Fails on a >15% regression that REPRODUCES: whatever
# crosses the threshold is re-run alone for another PERF_GATE_ROUNDS interleaved
# rounds, and only a regression there too fails the gate. A slowdown spanning a
# whole run (a co-tenant burst, an unlucky memory layout) lifts even its `min`, and
# two such runs out of three can carry a median.
set -euo pipefail

ROUNDS="${PERF_GATE_ROUNDS:-3}"
FLAGGED="${GITHUB_WORKSPACE}/.benchmarks-flagged.txt"

# One interleaved round: base, then head, saved to storage dir $2; the remaining
# args select the benchmarks (a directory or node ids).
bench_round() {
	local label="$1" storage="file://$2"
	shift 2
	echo "::group::perf round $label"
	# `base` first, then `head`, so the two sides are temporally adjacent. base
	# may legitimately have NO perf_gate tests (merge-base predates the marker):
	# pytest exits 5 ("no tests collected") there, which we tolerate. Any OTHER
	# non-zero means base actually broke — fail rather than gate against a missing
	# baseline (gate_compare.py would otherwise pass it as "no base"). A head
	# failure fails the gate via set -e for the same reason.
	local base_rc=0
	(cd /tmp/base-ref && ./.venv/bin/pytest \
		--benchmark-only --benchmark-warmup=on --benchmark-disable-gc \
		--benchmark-storage="$storage" --benchmark-save=base \
		-m perf_gate "$@") || base_rc=$?
	if [ "$base_rc" -eq 5 ]; then
		echo "::warning::base perf round $label collected no perf_gate tests (ref predates the marker)"
	elif [ "$base_rc" -ne 0 ]; then
		echo "::error::base perf round $label failed (pytest exit $base_rc); refusing to gate without a valid baseline"
		exit 1
	fi
	(cd "${GITHUB_WORKSPACE}" && ./.venv/bin/pytest \
		--benchmark-only --benchmark-warmup=on --benchmark-disable-gc \
		--benchmark-storage="$storage" --benchmark-save=head \
		-m perf_gate "$@")
	echo "::endgroup::"
}

for r in $(seq 1 "$ROUNDS"); do
	bench_round "$r/$ROUNDS" "${GITHUB_WORKSPACE}/.benchmarks" tests/perf
done

if ./.venv/bin/python tests/perf/gate_compare.py \
	--storage "${GITHUB_WORKSPACE}/.benchmarks" --threshold 15 --flagged-out "$FLAGGED"; then
	exit 0
fi
# Nothing flagged means the comparator itself failed — no re-run can fix that.
[ -s "$FLAGGED" ] || exit 1

mapfile -t flagged <"$FLAGGED"
echo "perf-gate: re-running ${#flagged[@]} flagged benchmark(s) for $ROUNDS more interleaved rounds; the gate fails only if they regress again"
for r in $(seq 1 "$ROUNDS"); do
	bench_round "confirm $r/$ROUNDS" "${GITHUB_WORKSPACE}/.benchmarks-confirm" "${flagged[@]}"
done

./.venv/bin/python tests/perf/gate_compare.py \
	--storage "${GITHUB_WORKSPACE}/.benchmarks-confirm" --threshold 15

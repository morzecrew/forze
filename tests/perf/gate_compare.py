"""Median-of-mins A/B comparator for the CI performance gate.

The gate benchmarks the PR's merge-base and its head on the **same** runner,
**interleaved** across several rounds (base, head, base, head, …). Same-runner
pairing cancels the between-runner lottery; interleaving + taking the *median of
each side's per-round ``min``* cancels the within-job temporal drift (turbo /
thermal throttle, a co-tenant arriving) that a single sequential A/B still lets
through. ``min`` stays the per-round metric — for a CPU micro-benchmark noise is
one-directional (interference only slows an iteration), so the per-run ``min`` is
the cleanest estimate of the code path; the median across rounds then removes the
unlucky-round effect that makes a single ``min`` sample flaky for sub-millisecond
benchmarks.

Reads the ``*_base.json`` / ``*_head.json`` runs pytest-benchmark saved into the
shared storage dir, matches benchmarks by ``fullname``, and fails (exit 1) when a
benchmark's median-of-mins regressed by more than the threshold. New benchmarks
(head-only) and dropped ones (base-only) are reported but never fail the gate.

Usage::

    python tests/perf/gate_compare.py --storage .benchmarks --threshold 15
"""

from __future__ import annotations

import argparse
import glob
import json
import statistics
import sys
from collections import defaultdict

# ----------------------- #

_MS = 1000.0  # pytest-benchmark stores seconds; report milliseconds.


def _load_mins(storage: str, suffix: str) -> dict[str, list[float]]:
    """Map ``fullname`` → list of per-run ``min`` values for one side (base/head)."""

    mins: dict[str, list[float]] = defaultdict(list)
    for path in sorted(glob.glob(f"{storage}/**/*_{suffix}.json", recursive=True)):
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        for bench in data.get("benchmarks", []):
            mins[bench["fullname"]].append(bench["stats"]["min"])
    return mins


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--storage",
        required=True,
        help="pytest-benchmark storage dir holding the *_base.json / *_head.json runs.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=15.0,
        help="Max tolerated regression of the median-of-mins, in percent (default: 15).",
    )
    args = parser.parse_args()

    base = _load_mins(args.storage, "base")
    head = _load_mins(args.storage, "head")

    if not head:
        print("perf-gate: no head benchmarks were produced — nothing to compare.")
        return 0
    if not base:
        # A merge-base that predates the perf_gate marker produces no baseline;
        # report head numbers and pass (there is nothing to regress against).
        print("perf-gate: no baseline produced on merge-base — head numbers only:")
        for name in sorted(head):
            print(f"  {statistics.median(head[name]) * _MS:9.4f} ms  {name}")
        return 0

    shared = sorted(set(base) & set(head))
    new = sorted(set(head) - set(base))
    gone = sorted(set(base) - set(head))

    rounds_base = max((len(v) for v in base.values()), default=0)
    rounds_head = max((len(v) for v in head.values()), default=0)

    rows: list[tuple[float, str, float, float, int, int]] = []
    for name in shared:
        base_med = statistics.median(base[name])
        head_med = statistics.median(head[name])
        delta = ((head_med - base_med) / base_med * 100.0) if base_med > 0 else 0.0
        rows.append((delta, name, base_med, head_med, len(base[name]), len(head[name])))

    rows.sort(reverse=True)  # worst regression first
    regressed = [r for r in rows if r[0] > args.threshold]

    print(
        f"perf-gate: median of per-run min over {rounds_base}×base / {rounds_head}×head "
        f"interleaved rounds; fail at +{args.threshold:g}%\n"
    )
    print(f"{'Δ%':>8}  {'base(ms)':>10}  {'head(ms)':>10}  {'n(b/h)':>7}  benchmark")
    print(f"{'-' * 8}  {'-' * 10}  {'-' * 10}  {'-' * 7}  {'-' * 40}")
    for delta, name, base_med, head_med, nb, nh in rows:
        flag = "  <-- REGRESSED" if delta > args.threshold else ""
        print(
            f"{delta:+8.2f}  {base_med * _MS:10.4f}  {head_med * _MS:10.4f}  "
            f"{nb:>3}/{nh:<3}  {name}{flag}"
        )

    for name in new:
        print(
            f"{'NEW':>8}  {'-':>10}  {statistics.median(head[name]) * _MS:10.4f}  "
            f"{'-':>7}  {name}"
        )
    for name in gone:
        print(
            f"{'GONE':>8}  {statistics.median(base[name]) * _MS:10.4f}  {'-':>10}  "
            f"{'-':>7}  {name}"
        )

    if regressed:
        print(
            f"\nperf-gate: FAIL — {len(regressed)} benchmark(s) regressed > "
            f"{args.threshold:g}% (median of mins)."
        )
        return 1

    print(
        f"\nperf-gate: PASS — no benchmark regressed > {args.threshold:g}% (median of mins)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

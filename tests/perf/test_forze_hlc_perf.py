"""Micro-benchmarks for the Hybrid Logical Clock primitive.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.

What is measured: ``now`` (stamp a local event) and ``update`` (merge a received
timestamp) are the two hot paths an outbox enrichment step would call once per
event; ``pack`` is the per-row encode into the sortable storage key. These set a
regression floor for the primitive before any outbox wiring is built on it.

Run only these benchmarks::

    just perf tests/perf/test_forze_hlc_perf.py
"""

from __future__ import annotations

import pytest

from forze.base.primitives import HlcTimestamp, HybridLogicalClock

# ----------------------- #


@pytest.mark.perf
def test_hlc_now_benchmark(benchmark) -> None:
    """Cost of issuing a local timestamp."""

    clock = HybridLogicalClock()

    benchmark(clock.now)


@pytest.mark.perf
def test_hlc_update_benchmark(benchmark) -> None:
    """Cost of merging a received timestamp."""

    clock = HybridLogicalClock()
    remote = HlcTimestamp(physical_ms=1_700_000_000_000, logical=3)

    def run() -> None:
        clock.update(remote)

    benchmark(run)


@pytest.mark.perf
def test_hlc_pack_benchmark(benchmark) -> None:
    """Cost of packing a timestamp into its sortable integer storage key."""

    ts = HlcTimestamp(physical_ms=1_700_000_000_000, logical=42)

    benchmark(ts.pack)


# ....................... #

pytestmark = pytest.mark.perf_gate

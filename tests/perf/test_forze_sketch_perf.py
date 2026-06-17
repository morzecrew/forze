"""Micro-benchmarks for the DDSketch quantile sketch.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.

What is measured: ``observe`` is the hot path (one ``log`` + one dict bump per
sample), and ``quantile`` is the read path (a ``sorted`` walk of the bucket
map). The ``WindowedDDSketch`` observe pays for feeding two sketches plus
rotation bookkeeping. These set a regression floor for the primitive before any
fleet-aggregation wiring is built on top.

Run only these benchmarks::

    just perf tests/perf/test_forze_sketch_perf.py
"""

from __future__ import annotations

import random

import pytest

from forze.base.primitives import DDSketch, WindowedDDSketch

# ----------------------- #

_ALPHA = 0.01
_RNG = random.Random(2024)
_SAMPLES = [_RNG.lognormvariate(3.0, 1.0) for _ in range(2_000)]


def _warm_sketch() -> DDSketch:
    sketch = DDSketch(relative_accuracy=_ALPHA)

    for v in _SAMPLES:
        sketch.observe(v)

    return sketch


# ----------------------- #


@pytest.mark.perf
def test_ddsketch_observe_benchmark(benchmark) -> None:
    """Cost of ingesting a batch of observations into a fresh sketch."""

    def run() -> None:
        sketch = DDSketch(relative_accuracy=_ALPHA)

        for v in _SAMPLES:
            sketch.observe(v)

    benchmark(run)


@pytest.mark.perf
def test_ddsketch_quantile_benchmark(benchmark) -> None:
    """Cost of answering the common p50/p95/p99 trio from a warm sketch."""

    sketch = _warm_sketch()

    def run() -> None:
        sketch.quantile(0.5)
        sketch.quantile(0.95)
        sketch.quantile(0.99)

    benchmark(run)


@pytest.mark.perf
def test_ddsketch_merge_benchmark(benchmark) -> None:
    """Cost of merging two warm sketches (the fleet-aggregation primitive)."""

    left = _warm_sketch()
    right = _warm_sketch()

    def run() -> None:
        DDSketch.merged(left, right)

    benchmark(run)


@pytest.mark.perf
def test_windowed_ddsketch_observe_benchmark(benchmark) -> None:
    """Cost of ingesting observations through the rotating windowed sketch."""

    def run() -> None:
        sketch = WindowedDDSketch(relative_accuracy=_ALPHA, window=512)

        for v in _SAMPLES:
            sketch.observe(v)

    benchmark(run)


# ....................... #

pytestmark = pytest.mark.perf_gate

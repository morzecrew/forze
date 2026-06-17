"""Micro-benchmark for the Gradient2 delay-based limiter.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.

What is measured: ``observe`` is called once per completed call on the bulkhead
hot path — a baseline EWMA update plus the gradient/limit arithmetic. This sets
a regression floor for the controller before any bulkhead wiring is built on it.

Run only this benchmark::

    just perf tests/perf/test_forze_gradient_limiter_perf.py
"""

from __future__ import annotations

import pytest

from forze.application.execution.resilience.limiter import Gradient2Limiter

# ----------------------- #


@pytest.mark.perf
def test_gradient_observe_benchmark(benchmark) -> None:
    """Cost of folding one completed-call latency sample into the limit."""

    limiter = Gradient2Limiter(initial_limit=50, max_limit=200)

    # Warm the baseline so the benchmarked path exercises the full update.
    for _ in range(100):
        limiter.observe(rtt=0.01, inflight=50)

    def run() -> None:
        limiter.observe(rtt=0.012, inflight=50)

    benchmark(run)


# ....................... #

pytestmark = pytest.mark.perf_gate

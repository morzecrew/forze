"""Micro-benchmarks for the adaptive-concurrency admission machine.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.

Two per-request hot paths of :class:`AdaptiveBulkheadState` (the unified admission machine behind the
AIMD, Gradient2, and CoDel bulkheads):

* the **AIMD controller update** (``on_complete``) — one EWMA/threshold decision folded per completed
  call, the AIMD sibling of the already-benched ``Gradient2Limiter.observe``;
* the **uncontended admission cycle** (``acquire`` → ``release``) — the counter + wait-queue overhead
  every admitted call pays even when there is spare capacity.

Both are deterministic and in-process (injected clock, no real time), so they set a regression floor
for adaptive load shedding without Docker.

Run only these::

    just perf tests/perf/test_forze_adaptive_bulkhead_perf.py
"""

from __future__ import annotations

from typing import Any, Callable

import pytest

from forze.application.execution.resilience.state import AdaptiveBulkheadState

# ----------------------- #


def _state(clock: Callable[[], float]) -> AdaptiveBulkheadState:
    return AdaptiveBulkheadState(
        latency_threshold=0.1,
        min_concurrency=1,
        max_concurrency=200,
        max_queue=64,
        backoff_ratio=0.9,
        increase_step=1.0,
        cooldown=1.0,
        clock=clock,
    )


# ....................... #


@pytest.mark.perf_gate
def test_aimd_on_complete_benchmark(benchmark: Any) -> None:
    """Cost of folding one healthy (below-threshold) completion into the AIMD limit."""

    state = _state(clock=lambda: 0.0)

    # Warm the limit to its ceiling so the benchmarked path is the steady-state ramp arithmetic.
    for _ in range(300):
        state.on_complete(latency=0.001, now=0.0)

    def run() -> None:
        state.on_complete(latency=0.001, now=0.0)

    benchmark(run)


# ....................... #


async def test_admission_cycle_benchmark(async_benchmark: Any) -> None:
    """Cost of an uncontended admit → release (the counter + wait-queue fast path)."""

    state = _state(clock=lambda: 0.0)

    async def cycle() -> None:
        await state.acquire()
        state.release()

    await async_benchmark(cycle)


# ....................... #

pytestmark = pytest.mark.perf

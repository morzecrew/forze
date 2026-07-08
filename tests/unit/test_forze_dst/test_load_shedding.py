"""Prioritized load shedding under overload, proven under DST.

The prioritized bulkhead sheds the *right* requests under overload: when saturated, a higher-criticality
arrival displaces the lowest-criticality waiter, so critical work makes progress while best-effort work
is shed. The forced-interleaving unit tests (``test_prioritized_bulkhead``) prove specific orderings;
this drives a mixed-criticality overload through the **deterministic simulation loop** across a range
of seeds, so the guarantee is shown to hold under every interleaving the scheduler explores, not one.

Invariant (checked every seed): under genuine overload, **no CRITICAL request is shed** (nothing can
displace it and CoDel is off) and **every CRITICAL completes**, while best-effort requests *are* shed —
the load shedding is criticality-ordered, provably, not incidentally.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    ResiliencePolicy,
)
from forze.application.execution.context.criticality import (
    Criticality,
    bind_criticality,
)
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.base.exceptions import CoreException
from forze_dst.runtime import run_simulation

# ----------------------- #

_N_CRITICAL = 2
_N_BEST_EFFORT = 6  # 8 arrivals against a capacity-4 bulkhead: a 2x overload


def _policy() -> ResiliencePolicy:
    # max_concurrency=2, max_queue=2 -> capacity 4; prioritized displacement, CoDel off
    # (queue_target_s default None) so shedding is by criticality alone, never by sojourn time.
    return ResiliencePolicy(
        name="svc",
        strategies=(
            AdaptiveBulkheadStrategy(
                latency_threshold=timedelta(seconds=10),
                max_concurrency=2,
                max_queue=2,
                prioritized=True,
            ),
        ),
    )


async def _overload_round() -> dict[str, int]:
    """Fire a saturating burst of mixed-criticality calls; tally completions vs sheds per tier."""

    executor = InProcessResilienceExecutor(policies={"svc": _policy()})
    tally = {"critical_ok": 0, "critical_shed": 0, "best_ok": 0, "best_shed": 0}

    async def work() -> str:
        # Hold the slot across a yield so callers genuinely contend (the queue fills and sheds).
        await asyncio.sleep(0.001)
        return "ok"

    async def call(tier: Criticality) -> None:
        ok_key, shed_key = (
            ("critical_ok", "critical_shed")
            if tier is Criticality.CRITICAL
            else ("best_ok", "best_shed")
        )
        with bind_criticality(tier):
            try:
                await executor.run(work, policy="svc", route="r")
                tally[ok_key] += 1

            except CoreException:
                tally[shed_key] += 1

    arrivals = [Criticality.BEST_EFFORT] * _N_BEST_EFFORT + [
        Criticality.CRITICAL
    ] * _N_CRITICAL
    await asyncio.gather(*(asyncio.ensure_future(call(t)) for t in arrivals))

    return tally


# ....................... #


class TestPrioritizedSheddingUnderDst:
    def test_critical_never_shed_under_overload_across_seeds(self) -> None:
        for seed in range(40):
            tally = run_simulation(_overload_round, seed=seed)

            # The right requests are shed: critical work is fully protected...
            assert tally["critical_shed"] == 0, f"seed {seed}: a CRITICAL request was shed"
            assert tally["critical_ok"] == _N_CRITICAL, f"seed {seed}: a CRITICAL was lost"

            # ...while the overload is real — best-effort work is shed, not vacuously admitted.
            assert tally["best_shed"] > 0, f"seed {seed}: not actually overloaded"

            # Nothing is invented or double-counted.
            assert tally["best_ok"] + tally["best_shed"] == _N_BEST_EFFORT

    def test_runs_are_reproducible(self) -> None:
        # Same seed -> identical outcome (the whole point of proving it under DST).
        assert run_simulation(_overload_round, seed=7) == run_simulation(
            _overload_round, seed=7
        )

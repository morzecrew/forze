"""Cooperative scheduling point — yields at port boundaries only under simulation.

Real adapters suspend on I/O; the in-memory mocks don't, so without a yield two concurrent
operations would run as if atomic and interleaving bugs would hide. ``cooperative_point``
yields when a simulation has enabled cooperative scheduling, and is a no-op otherwise (so
production / non-simulated code is untouched and never needs an artificial ``sleep(0)``).
"""

from __future__ import annotations

import asyncio

from forze.application.execution.tracing.cooperative import (
    cooperative_point,
    cooperative_scheduling,
)

# ----------------------- #


async def _interleaving_order(active: bool) -> list[str]:
    order: list[str] = []

    async def worker(tag: str) -> None:
        order.append(f"{tag}1")
        await cooperative_point()
        order.append(f"{tag}2")

    async def run() -> None:
        await asyncio.gather(worker("a"), worker("b"))

    if active:
        with cooperative_scheduling():
            await run()
    else:
        await run()

    return order


def test_inactive_is_a_no_op_so_operations_run_atomically() -> None:
    # Default (no simulation): cooperative_point does not yield → each worker runs to
    # completion before the next starts.
    assert asyncio.run(_interleaving_order(active=False)) == ["a1", "a2", "b1", "b2"]


def test_active_yields_so_operations_interleave() -> None:
    # Under cooperative scheduling the yield lets the workers interleave at the point.
    assert asyncio.run(_interleaving_order(active=True)) == ["a1", "b1", "a2", "b2"]

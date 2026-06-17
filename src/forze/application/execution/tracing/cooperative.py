"""Cooperative scheduling point for deterministic simulation.

Under simulation every I/O is a scheduling point: a real adapter suspends on network/disk,
so concurrent operations interleave *at port boundaries*. The in-memory mocks don't suspend,
so without help two concurrent operations would run as if atomic and any interleaving bug
would hide — forcing test authors to sprinkle ``await asyncio.sleep(0)`` into production
handlers (a crutch). Instead, the port-tracing proxy awaits :func:`cooperative_point` before
each wrapped call; when a simulation has enabled cooperative scheduling it yields to the
loop, so the scheduler (PCT / DPOR / perturbation) explores the interleavings naturally.

Off by default — a no-op in production and outside simulation (one ``ContextVar`` read).
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

# ----------------------- #

_COOPERATIVE: ContextVar[bool] = ContextVar(
    "forze_cooperative_scheduling", default=False
)


async def cooperative_point() -> None:
    """Yield to the event loop at a port boundary when cooperative scheduling is active."""

    if _COOPERATIVE.get():
        await asyncio.sleep(0)


@contextmanager
def cooperative_scheduling() -> Iterator[None]:
    """Enable cooperative scheduling (port boundaries yield) for the duration of the block."""

    token = _COOPERATIVE.set(True)

    try:
        yield

    finally:
        _COOPERATIVE.reset(token)

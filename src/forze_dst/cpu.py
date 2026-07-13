"""A :class:`~forze.base.primitives.CpuExecutor` that runs inline under simulation.

Production ``run_cpu`` offloads to a thread pool, which the simulation loop forbids
(``RealIOForbidden``). Under DST the work runs **inline** instead — deterministic,
single-threaded — and optionally advances the virtual clock by a per-call-site cost,
so an offloaded parse is a first-class simulation event: a yield point the scheduler
can interleave around, and a latency site a deadline can fire "during".
"""

import asyncio
from collections.abc import Callable
from typing import final

import attrs

from forze.base.primitives.cpu import CpuExecutor

# ----------------------- #

CpuCostModel = Callable[[str | None], float]
"""``(label) -> seconds`` of virtual time for an offloaded call site (its qualname)."""

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class SimulationCpuExecutor(CpuExecutor):
    """Run offloaded work inline on the simulation loop, optionally costing virtual time.

    With no :attr:`cost`, a call is a bare yield (``await asyncio.sleep(0)``) — an
    interleaving point, instant in virtual time. A :attr:`cost` model advances the
    virtual clock for the named call site before running the thunk, so time-dependent
    behavior around slow CPU work (deadlines, timeouts) is exercised deterministically.
    """

    cost: CpuCostModel | None = None

    async def run[T](self, fn: Callable[[], T], *, label: str | None = None) -> T:
        delay = self.cost(label) if self.cost is not None else 0.0
        await asyncio.sleep(delay)  # virtual-time advance + yield (delay 0.0 = bare yield)

        return fn()

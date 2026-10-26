"""A worker held by ``serve_runtime``, run as a subprocess by ``test_serve_runtime``.

Usage: ``python _serve_runtime_app.py RECORD_FILE MODE``. Every milestone is appended to
RECORD_FILE, one per line, so the test reads what happened after the process is gone.

Modes: ``quick`` — the drain phase returns at once; ``timed`` — it takes a second;
``slow`` — it sleeps for a minute, recording its cancellation; ``raise`` —
``runtime.drain()`` itself raises.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import timedelta
from pathlib import Path

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext, build_runtime, serve_runtime
from forze.application.execution.runtime import ExecutionRuntime

# ----------------------- #

RECORD = Path(sys.argv[1])
MODE = sys.argv[2]

ANNOUNCE_DELAY = timedelta(seconds=0.5)


def record(event: str) -> None:
    with RECORD.open("a") as out:
        out.write(event + "\n")


# ....................... #


async def phase(_allowed: timedelta) -> None:
    record("phase-started")

    if MODE == "timed":
        await asyncio.sleep(1)

    if MODE == "slow":
        try:
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            record("phase-cancelled")
            raise

    record("phase-done")


async def startup(ctx: ExecutionContext) -> None:
    ctx.drain_phases.register("record", phase)
    record("started")


async def teardown(_ctx: ExecutionContext) -> None:
    record("teardown")


if MODE == "raise":

    async def _failing_drain(
        _self: ExecutionRuntime,
        _budget: timedelta,
        *,
        announce_delay: timedelta = timedelta(0),
    ) -> None:
        record("drain-raised")
        raise RuntimeError("drain failed")

    ExecutionRuntime.drain = _failing_drain  # type: ignore[method-assign]


runtime = build_runtime(
    lifecycle_steps=LifecycleStep(id="recorder", startup=startup, shutdown=teardown),
    drain_timeout=timedelta(0),
)


async def main() -> None:
    await serve_runtime(runtime, drain_budget=timedelta(seconds=30), announce_delay=ANNOUNCE_DELAY)

    record("served")


if __name__ == "__main__":
    asyncio.run(main())

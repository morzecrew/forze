"""An app served by ``serve_uvicorn``, run as a subprocess by ``test_serve_uvicorn``.

Usage: ``python _serve_uvicorn_app.py PORT RECORD_FILE MODE``. Every milestone is appended
to RECORD_FILE, one per line, so the test reads what happened after the process is gone.

Modes: ``quick`` — the drain phase returns at once; ``slow`` — it sleeps for a minute,
recording its cancellation; ``raise`` — ``runtime.drain()`` itself raises.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.responses import StreamingResponse

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext, build_runtime
from forze.application.execution.runtime import ExecutionRuntime
from forze_fastapi import runtime_lifespan, serve_uvicorn
from forze_fastapi.routes import attach_readiness_route

# ----------------------- #

PORT = int(sys.argv[1])
RECORD = Path(sys.argv[2])
MODE = sys.argv[3]

ANNOUNCE_DELAY = timedelta(seconds=1.5)
GRACEFUL_TIMEOUT = timedelta(seconds=1)


def record(event: str) -> None:
    with RECORD.open("a") as out:
        out.write(event + "\n")


# ....................... #


async def phase(_allowed: timedelta) -> None:
    record("phase-started")

    if MODE == "slow":
        try:
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            record("phase-cancelled")
            raise

    record("phase-done")


async def startup(ctx: ExecutionContext) -> None:
    ctx.drain_phases.register("record", phase)


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

router = APIRouter()
attach_readiness_route(router, runtime)


@router.get("/stream")
async def stream() -> StreamingResponse:
    async def ticks() -> AsyncIterator[bytes]:
        for _ in range(6_000):  # ten minutes at most, whatever happens to the server
            yield b"data: tick\n\n"
            await asyncio.sleep(0.1)

    return StreamingResponse(ticks(), media_type="text/event-stream")


app = FastAPI(lifespan=runtime_lifespan(runtime))
app.include_router(router)


async def main() -> None:
    config = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="warning")

    await serve_uvicorn(
        config,
        runtime,
        drain_budget=timedelta(seconds=30),
        announce_delay=ANNOUNCE_DELAY,
        graceful_timeout=GRACEFUL_TIMEOUT,
    )

    record("served")


if __name__ == "__main__":
    asyncio.run(main())

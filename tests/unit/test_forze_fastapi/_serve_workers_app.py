"""An app factory served by ``uvicorn --workers 2``, run as a subprocess by
``test_serve_workers``.

The runtime is built in the lifespan, so each worker builds its own. Every milestone is
appended to the file ``FORZE_TEST_RECORD`` names, one ``<pid> <event>`` per line, so the
test reads what each worker did after the processes are gone.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from pathlib import Path

from fastapi import APIRouter, FastAPI

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext, ExecutionRuntime, build_runtime
from forze_fastapi import runtime_lifespan
from forze_fastapi.routes import attach_readiness_route

# ----------------------- #

ANNOUNCE_DELAY = timedelta(seconds=1.5)
OUTLIVE_DRAIN = 1.0


def record(event: str) -> None:
    with Path(os.environ["FORZE_TEST_RECORD"]).open("a") as out:
        out.write(f"{os.getpid()} {event}\n")


# ....................... #


async def phase(_allowed: timedelta) -> None:
    record("phase-started")
    record("phase-done")


async def startup(ctx: ExecutionContext) -> None:
    ctx.drain_phases.register("record", phase)
    record("started")


async def teardown(_ctx: ExecutionContext) -> None:
    record("teardown")


def build() -> ExecutionRuntime:
    record("built")

    return build_runtime(
        lifecycle_steps=LifecycleStep(id="recorder", startup=startup, shutdown=teardown),
        drain_timeout=timedelta(0),
    )


# ....................... #


def create_app() -> FastAPI:
    lifespan = runtime_lifespan(
        build=build,
        drain_budget=timedelta(seconds=5),
        announce_delay=ANNOUNCE_DELAY,
    )

    router = APIRouter()
    attach_readiness_route(router, lifespan.runtime)

    @router.get("/slow")
    async def slow() -> dict[str, int]:  # pyright: ignore[reportUnusedFunction]
        lifespan.get_context()  # the ctx_dep the generated routes would be given
        runtime = lifespan.runtime()
        assert runtime is not None
        record("slow-started")

        # In flight until well past this worker's drain, however late the signal reaches it.
        for _ in range(600):  # a minute at most, whatever happens to the server
            if runtime.draining:
                record("draining")
                break

            await asyncio.sleep(0.1)

        await asyncio.sleep(ANNOUNCE_DELAY.total_seconds() + OUTLIVE_DRAIN)
        record("slow-done")

        return {"pid": os.getpid()}

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)

    return app

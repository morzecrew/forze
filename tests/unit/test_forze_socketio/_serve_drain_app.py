"""A Socket.IO app served by ``serve_uvicorn``, run as a subprocess by ``test_connection_drain_serve``.

Usage: ``python _serve_drain_app.py PORT RECORD_FILE``. Milestones are appended to
RECORD_FILE as ``<event> <wall-clock seconds>``, one per line, so the test can order them
against what its clients saw.
"""

from __future__ import annotations

import asyncio
import sys
import time
from datetime import timedelta
from pathlib import Path

import uvicorn
from fastapi import APIRouter, FastAPI

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext, build_runtime
from forze_fastapi import runtime_lifespan, serve_uvicorn
from forze_fastapi.routes import attach_readiness_route
from forze_socketio import (
    attach_realtime_connection,
    build_socketio_asgi_app,
    build_socketio_server,
    connection_drain_lifecycle_step,
)

# ----------------------- #

PORT = int(sys.argv[1])
RECORD = Path(sys.argv[2])

WINDOW = timedelta(seconds=1)
TAIL = timedelta(seconds=0.5)
ANNOUNCE_DELAY = timedelta(seconds=0.5)


def record(event: str) -> None:
    with RECORD.open("a") as out:
        out.write(f"{event} {time.time()}\n")


async def teardown(_ctx: ExecutionContext) -> None:
    record("teardown")


# ....................... #

sio = build_socketio_server(async_mode="asgi")
runtime = build_runtime(
    lifecycle_steps=(
        LifecycleStep(id="recorder", shutdown=teardown),
        connection_drain_lifecycle_step(sio, window=WINDOW, tail=TAIL),
    ),
    drain_timeout=timedelta(0),
)
attach_realtime_connection(sio, resolve=lambda _connect: None, draining=lambda: runtime.draining)

router = APIRouter()
attach_readiness_route(router, runtime)
api = FastAPI(lifespan=runtime_lifespan(runtime))
api.include_router(router)


async def main() -> None:
    config = uvicorn.Config(
        build_socketio_asgi_app(sio, other_asgi_app=api),
        host="127.0.0.1",
        port=PORT,
        log_level="warning",
    )

    await serve_uvicorn(
        config,
        runtime,
        drain_budget=timedelta(seconds=10),
        announce_delay=ANNOUNCE_DELAY,
        graceful_timeout=timedelta(seconds=1),
    )

    record("served")


if __name__ == "__main__":
    asyncio.run(main())

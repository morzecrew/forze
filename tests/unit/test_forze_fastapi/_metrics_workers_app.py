"""An app factory served by ``uvicorn --workers N``, run as a subprocess by
``test_metrics_snapshot_workers``.

Every worker installs its own telemetry, so each mints its own ``service.instance.id``,
unless ``FORZE_TEST_INSTANCE_ID`` forces one id on all of them: what gunicorn's
``--preload`` does to an app that calls ``bootstrap_telemetry`` at import. Each worker
reports its pid as a gauge, so the test can tell which instance is which process.
``FORZE_TEST_PLAIN`` serves the single-process route instead of the union.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import timedelta
from pathlib import Path

from fastapi import APIRouter, FastAPI
from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

from forze.application.execution import build_runtime
from forze.base.telemetry import bootstrap_telemetry
from forze_fastapi import runtime_lifespan
from forze_fastapi.routes import MetricsSnapshots, attach_metrics_route, worker_metrics_reader

# ----------------------- #

INTERVAL = timedelta(milliseconds=200)
TTL = timedelta(milliseconds=600)


def _pid(_options: CallbackOptions) -> Iterable[Observation]:
    yield Observation(os.getpid())


def create_app() -> FastAPI:
    reader = worker_metrics_reader()
    bootstrap_telemetry(
        service_name="snapshot-workers",
        service_instance_id=os.environ.get("FORZE_TEST_INSTANCE_ID"),
        traces=False,
        exporter="none",
        extra_metric_readers=[reader],
    )

    meter = metrics.get_meter("forze")
    hits = meter.create_counter("forze.test.hits")
    meter.create_observable_gauge("forze.test.pid", callbacks=[_pid])

    snapshots = MetricsSnapshots(
        directory=Path(os.environ["FORZE_TEST_SNAPSHOTS"]), interval=INTERVAL, ttl=TTL
    )
    plain = bool(os.environ.get("FORZE_TEST_PLAIN"))
    # Plain mode runs no writer: the plain route renders the reader itself, and two renderers
    # of one reader race for its collector.
    steps = () if plain else (snapshots.writer_step(reader),)
    lifespan = runtime_lifespan(build=lambda: build_runtime(lifecycle_steps=steps))

    router = APIRouter()
    attach_metrics_route(router, reader, snapshots=None if plain else snapshots)

    @router.get("/hit")
    async def hit() -> dict[str, int]:  # pyright: ignore[reportUnusedFunction]
        hits.add(1)

        return {"pid": os.getpid()}

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)

    return app

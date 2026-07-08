"""Integration test (fake-gcs): the generated FastAPI streaming download route end-to-end.

Mirrors the S3 route test on GCS — a plain ``GET`` streams a larger-than-one-part object back via
the adapter's ranged-GET loop (no full-object buffering), and a ``Range`` request is served by a
real backend-ranged fetch. Driven with ``httpx.AsyncClient`` so the app shares the fixture's loop.
"""

import httpx
from fastapi import APIRouter, FastAPI

from forze.application.contracts.storage import StorageSpec
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_storage_routes
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.execution.deps.module import GCSDepsModule
from forze_gcs.kernel.client.client import GCSClient
from forze_kits.aggregates.storage import build_storage_registry
from tests.support.execution_context import context_from_deps

# ----------------------- #

MIB = 1024 * 1024


def _payload(size: int) -> bytes:
    """A deterministic byte payload of exactly *size* bytes."""

    pattern = b"0123456789abcdef"
    return (pattern * (size // len(pattern) + 1))[:size]


def _app(gcs_client: GCSClient, bucket: str) -> FastAPI:
    spec = StorageSpec(name=bucket)
    registry = build_storage_registry(spec).freeze()

    def ctx_dep():
        return context_from_deps(
            GCSDepsModule(
                client=gcs_client,
                storages={bucket: GCSStorageConfig(bucket=bucket)},
            )()
        )

    router = APIRouter(prefix="/files")
    attach_storage_routes(
        router,
        registry=registry,
        ns=spec.default_namespace,
        ctx_dep=ctx_dep,
        style="rest",
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


async def test_streaming_route_round_trips_a_multi_part_object(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    data = _payload(12 * MIB)  # > the 8 MiB part size

    async with _client(_app(gcs_client, gcs_bucket)) as client:
        key = (
            await client.post(
                "/files", files={"file": ("big.bin", data, "application/octet-stream")}
            )
        ).json()["key"]

        resp = await client.get(f"/files/{key}")

    assert resp.status_code == 200
    assert resp.content == data
    assert resp.headers["accept-ranges"] == "bytes"


async def test_streaming_route_serves_a_backend_range(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    data = _payload(10 * MIB)

    async with _client(_app(gcs_client, gcs_bucket)) as client:
        key = (
            await client.post(
                "/files",
                files={"file": ("ranged.bin", data, "application/octet-stream")},
            )
        ).json()["key"]

        start, end = 8 * MIB - 100, 8 * MIB + 100
        resp = await client.get(
            f"/files/{key}", headers={"Range": f"bytes={start}-{end}"}
        )

    assert resp.status_code == 206
    assert resp.content == data[start : end + 1]
    assert resp.headers["content-range"] == f"bytes {start}-{end}/{len(data)}"

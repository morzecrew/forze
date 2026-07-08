"""Integration test (MinIO): the generated FastAPI streaming download route end-to-end.

Proves the bounded-memory route works against a real backend — a plain ``GET`` streams a
larger-than-one-part object back via the adapter's ranged-GET loop (never buffering the whole
object in the app), and a ``Range`` request is served by a real backend-ranged fetch.

Driven through the ASGI app with ``httpx.AsyncClient`` (not the sync ``TestClient``) so the app
runs in the same event loop as the ``s3_client`` fixture.
"""

import httpx
from fastapi import APIRouter, FastAPI

from forze.application.contracts.storage import StorageSpec
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_storage_routes
from forze_kits.aggregates.storage import build_storage_registry
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client
from tests.support.execution_context import context_from_deps

# ----------------------- #

MIB = 1024 * 1024


def _payload(size: int) -> bytes:
    """A deterministic byte payload of exactly *size* bytes."""

    pattern = b"0123456789abcdef"
    return (pattern * (size // len(pattern) + 1))[:size]


def _app(s3_client: S3Client, bucket: str) -> FastAPI:
    spec = StorageSpec(name=bucket)
    registry = build_storage_registry(spec).freeze()

    def ctx_dep():
        return context_from_deps(
            S3DepsModule(
                client=s3_client,
                storages={bucket: S3StorageConfig(bucket=bucket)},
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
    s3_client: S3Client, s3_bucket: str
) -> None:
    # > the 8 MiB default stream part size, so download_stream loops multiple ranged GETs.
    data = _payload(12 * MIB)  # > the 8 MiB part size

    async with _client(_app(s3_client, s3_bucket)) as client:
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
    s3_client: S3Client, s3_bucket: str
) -> None:
    data = _payload(10 * MIB)

    async with _client(_app(s3_client, s3_bucket)) as client:
        key = (
            await client.post(
                "/files",
                files={"file": ("ranged.bin", data, "application/octet-stream")},
            )
        ).json()["key"]

        # A window crossing the 8 MiB part boundary, fetched as a real backend range.
        start, end = 8 * MIB - 100, 8 * MIB + 100
        resp = await client.get(
            f"/files/{key}", headers={"Range": f"bytes={start}-{end}"}
        )

    assert resp.status_code == 206
    assert resp.content == data[start : end + 1]
    assert resp.headers["content-range"] == f"bytes {start}-{end}/{len(data)}"

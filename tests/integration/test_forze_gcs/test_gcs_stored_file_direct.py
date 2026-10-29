"""Integration test (fake-gcs-server): stored-file ranged reads on GCS.

The stored-file rows live in the mock document store; the blobs live in a real bucket behind the
GCS storage adapter, so ranged reads and cache validators are the real adapter's.
"""

import httpx
from fastapi import APIRouter, FastAPI

from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.testing import context_from_modules
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_stored_file_download_route
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.execution.deps.module import GCSDepsModule
from forze_gcs.kernel.client.client import GCSClient
from forze_kits.aggregates.stored_file import (
    DownloadStoredFileRangeRequestDTO,
    StoredFileKernelOp,
    UploadStoredFileRequestDTO,
    freeze_stored_file_registry,
)
from forze_kits.domain.stored_file import StoredFileKitSpec, StoredFileRead
from forze_mock import MockDepsModule, MockState

# ----------------------- #

_KIT = StoredFileKitSpec(name="files", stream_downloads=True, range_downloads=True)
_NS = _KIT.document.default_namespace
_REG = freeze_stored_file_registry(_KIT, tx_route="mock")

_DATA = bytes(range(256)) * 64 + b"tail"


def _ctx(gcs_client: GCSClient, bucket: str) -> ExecutionContext:
    return context_from_modules(
        MockDepsModule(state=MockState()),
        GCSDepsModule(client=gcs_client, storages={"files": GCSStorageConfig(bucket=bucket)}),
    )


async def _ready(ctx: ExecutionContext, data: bytes) -> StoredFileRead:
    created = await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.UPLOAD),
        UploadStoredFileRequestDTO(filename="report.bin", data=data),
        ctx,
    )
    return await ctx.doc.query(_KIT.document).get(created.id)


# ....................... #


async def test_the_range_operation_reads_the_window(gcs_client: GCSClient, gcs_bucket: str) -> None:
    ctx = _ctx(gcs_client, gcs_bucket)
    file = await _ready(ctx, _DATA)

    result = await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.DOWNLOAD_RANGE),
        DownloadStoredFileRangeRequestDTO(id=file.id, start=10, end=19),
        ctx,
    )

    assert result.data == _DATA[10:20]
    assert result.content_range == f"bytes 10-19/{len(_DATA)}"


async def test_the_route_serves_ranges_and_conditionals(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    ctx = _ctx(gcs_client, gcs_bucket)
    file = await _ready(ctx, _DATA)
    url = f"/files/{file.id}/content"

    router = APIRouter(prefix="/files")
    attach_stored_file_download_route(router, registry=_REG, ns=_NS, ctx_dep=lambda: ctx)
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        full = await client.get(url)
        ranged = await client.get(url, headers={"Range": "bytes=100-199"})
        suffix = await client.get(url, headers={"Range": "bytes=-4"})
        past = await client.get(url, headers={"Range": f"bytes={len(_DATA)}-"})
        cached = await client.get(url, headers={"If-None-Match": full.headers["etag"]})

    assert full.status_code == 200
    assert full.content == _DATA
    assert ranged.status_code == 206
    assert ranged.content == _DATA[100:200]
    assert ranged.headers["content-disposition"] == 'attachment; filename="report.bin"'
    assert suffix.status_code == 206
    assert suffix.content == b"tail"
    assert past.status_code == 416
    assert cached.status_code == 304

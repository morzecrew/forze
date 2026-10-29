"""Integration test (RustFS and floci): stored-file ranged reads on S3.

The stored-file rows live in the mock document store; the blobs live in a real bucket behind the
S3 storage adapter, so ranged reads, cache validators and the encryption paths are the real
adapter's.
"""

import httpx
import pytest
from fastapi import APIRouter, FastAPI

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import RANGE_WHOLE_PAYLOAD_UNSUPPORTED_CODE
from forze.application.execution import CryptoDepsModule, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_stored_file_download_route
from forze_kits.aggregates.stored_file import (
    DownloadStoredFileRangeRequestDTO,
    StoredFileKernelOp,
    UploadStoredFileRequestDTO,
    freeze_stored_file_registry,
)
from forze_kits.domain.stored_file import StoredFileKitSpec, StoredFileRead
from forze_mock import MockDepsModule, MockKeyManagement, MockState
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client

# ----------------------- #

MIB = 1024 * 1024

_KIT = StoredFileKitSpec(name="files", stream_downloads=True, range_downloads=True)
_NS = _KIT.document.default_namespace
_REG = freeze_stored_file_registry(_KIT, tx_route="mock")


def _ctx(s3_client: S3Client, bucket: str, *, encrypt: bool = False) -> ExecutionContext:
    modules = [
        MockDepsModule(state=MockState()),
        S3DepsModule(
            client=s3_client,
            storages={"files": S3StorageConfig(bucket=bucket, encrypt=encrypt)},
        ),
    ]

    if encrypt:
        modules.append(
            CryptoDepsModule(
                kms=MockKeyManagement(),
                directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
            )
        )

    return context_from_modules(*modules)


async def _ready(ctx: ExecutionContext, data: bytes) -> StoredFileRead:
    created = await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.UPLOAD),
        UploadStoredFileRequestDTO(filename="report.bin", data=data),
        ctx,
    )
    return await ctx.doc.query(_KIT.document).get(created.id)


def _client(ctx: ExecutionContext) -> httpx.AsyncClient:
    router = APIRouter(prefix="/files")
    attach_stored_file_download_route(router, registry=_REG, ns=_NS, ctx_dep=lambda: ctx)
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


_DATA = bytes(range(256)) * (MIB // 256) + b"tail"


# ....................... #


async def test_the_range_operation_reads_the_window(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    file = await _ready(ctx, _DATA)

    result = await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.DOWNLOAD_RANGE),
        DownloadStoredFileRangeRequestDTO(id=file.id, start=MIB - 2, end=MIB + 1),
        ctx,
    )

    assert result.data == _DATA[MIB - 2 : MIB + 2]
    assert result.content_range == f"bytes {MIB - 2}-{MIB + 1}/{len(_DATA)}"
    assert result.total_size == len(_DATA)


async def test_the_route_serves_ranges_and_conditionals(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    file = await _ready(ctx, _DATA)
    url = f"/files/{file.id}/content"

    async with _client(ctx) as client:
        full = await client.get(url)
        ranged = await client.get(url, headers={"Range": "bytes=100-199"})
        suffix = await client.get(url, headers={"Range": "bytes=-4"})
        past = await client.get(url, headers={"Range": f"bytes={len(_DATA)}-"})
        cached = await client.get(url, headers={"If-None-Match": full.headers["etag"]})

    assert full.status_code == 200
    assert full.content == _DATA
    assert full.headers["content-disposition"] == 'attachment; filename="report.bin"'

    assert ranged.status_code == 206
    assert ranged.content == _DATA[100:200]
    assert ranged.headers["content-range"] == f"bytes 100-199/{len(_DATA)}"
    assert ranged.headers["etag"] == full.headers["etag"]

    assert suffix.status_code == 206
    assert suffix.content == b"tail"

    assert past.status_code == 416
    assert past.headers["content-range"] == f"bytes */{len(_DATA)}"

    assert cached.status_code == 304


async def test_an_encrypted_upload_cannot_be_sliced_so_the_route_serves_it_whole(
    s3_client: S3Client, s3_bucket: str
) -> None:
    # The kit's upload seals the buffered bytes as one AEAD payload, which a range cannot
    # slice: the operation refuses, and the route falls back to the full streamed body.
    ctx = _ctx(s3_client, s3_bucket, encrypt=True)
    file = await _ready(ctx, _DATA)

    with pytest.raises(CoreException) as ei:
        await run_operation(
            _REG,
            _NS.key(StoredFileKernelOp.DOWNLOAD_RANGE),
            DownloadStoredFileRangeRequestDTO(id=file.id, start=0, end=9),
            ctx,
        )

    async with _client(ctx) as client:
        resp = await client.get(f"/files/{file.id}/content", headers={"Range": "bytes=0-9"})

    assert ei.value.code == RANGE_WHOLE_PAYLOAD_UNSUPPORTED_CODE
    assert resp.status_code == 200
    assert resp.content == _DATA

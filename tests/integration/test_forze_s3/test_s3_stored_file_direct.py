"""Integration test (RustFS and floci): stored-file ranged reads and direct uploads on S3.

The stored-file rows live in the mock document store; the blobs live in a real bucket behind the
S3 storage adapter, so ranged reads, cache validators, presigned PUTs, multipart sessions, tenant
key checks and the encryption paths are the real adapter's. The client's PUTs go over plain HTTP
with only the signed URL.
"""

from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from fastapi import APIRouter, FastAPI

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import RANGE_WHOLE_PAYLOAD_UNSUPPORTED_CODE
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CryptoDepsModule, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import FrozenTimeSource, bind_time_source, utcnow
from forze.testing import context_from_modules
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_stored_file_download_route
from forze_kits.aggregates.storage.dto import UploadPartDTO
from forze_kits.aggregates.stored_file import (
    CompleteStoredFileUploadRequestDTO,
    DownloadStoredFileRangeRequestDTO,
    InitiateStoredFileUploadRequestDTO,
    PresignStoredFileUploadPartRequestDTO,
    StoredFileIdDTO,
    StoredFileKernelOp,
    UploadStoredFileRequestDTO,
    freeze_stored_file_registry,
    stored_file_pending_sweep_lifecycle_step,
)
from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileKitSpec,
    StoredFileRead,
    StoredFileStatus,
)
from forze_mock import MockDepsModule, MockKeyManagement, MockRouteConfig, MockState
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client

# ----------------------- #

MIB = 1024 * 1024

_KIT = StoredFileKitSpec(
    name="files",
    stream_downloads=True,
    range_downloads=True,
    direct_uploads=True,
    max_bytes=16 * MIB,
)
_NS = _KIT.document.default_namespace
_REG = freeze_stored_file_registry(_KIT, tx_route="mock")


def _ctx(
    s3_client: S3Client,
    bucket: str,
    *,
    encrypt: bool = False,
    tenant_aware: bool = False,
) -> ExecutionContext:
    modules = [
        MockDepsModule(
            state=MockState(),
            routes={"files": MockRouteConfig(tenant_aware=tenant_aware)},
        ),
        S3DepsModule(
            client=s3_client,
            storages={
                "files": S3StorageConfig(bucket=bucket, encrypt=encrypt, tenant_aware=tenant_aware)
            },
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


# ....................... #


async def _initiate(ctx: ExecutionContext, size: int, *, multipart: bool = False):
    return await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.INITIATE_UPLOAD),
        InitiateStoredFileUploadRequestDTO(
            filename="upload.bin",
            size=size,
            content_type="application/octet-stream",
            multipart=multipart,
            expires_in=timedelta(minutes=5),
        ),
        ctx,
    )


async def _complete(ctx: ExecutionContext, args: CompleteStoredFileUploadRequestDTO):
    return await run_operation(_REG, _NS.key(StoredFileKernelOp.COMPLETE_UPLOAD), args, ctx)


async def _download(ctx: ExecutionContext, file_id) -> bytes:
    result = await run_operation(
        _REG, _NS.key(StoredFileKernelOp.DOWNLOAD), StoredFileIdDTO(id=file_id), ctx
    )
    return result.data


@pytest.mark.parametrize("tenant_aware", [False, True], ids=["global", "tenant"])
async def test_a_presigned_put_becomes_a_ready_file(
    s3_client: S3Client, s3_bucket: str, tenant_aware: bool
) -> None:
    ctx = _ctx(s3_client, s3_bucket, tenant_aware=tenant_aware)
    data = b"direct-upload-bytes"
    tenant = TenantIdentity(tenant_id=uuid4())

    with ctx.inv_ctx.bind_identity(tenant=tenant if tenant_aware else None):
        started = await _initiate(ctx, len(data))

        async with httpx.AsyncClient() as http:
            resp = await http.put(
                started.presigned.url, content=data, headers=started.presigned.headers
            )

        ready = await _complete(ctx, CompleteStoredFileUploadRequestDTO(id=started.file.id))
        downloaded = await _download(ctx, ready.id)

    assert resp.status_code == 200
    assert ready.status is StoredFileStatus.READY
    assert ready.size == len(data)
    assert downloaded == data

    if tenant_aware:
        assert ready.storage_key == f"tenant_{tenant.tenant_id}/{ready.id}"


async def test_multipart_parts_become_a_ready_file(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    parts = [bytes(range(256)) * (5 * MIB // 256), b"last-part"]  # every part but the last >= 5 MiB
    started = await _initiate(ctx, sum(map(len, parts)), multipart=True)
    landed: list[UploadPartDTO] = []

    async with httpx.AsyncClient() as http:
        for number, body in enumerate(parts, start=1):
            url = await run_operation(
                _REG,
                _NS.key(StoredFileKernelOp.PRESIGN_UPLOAD_PART),
                PresignStoredFileUploadPartRequestDTO(
                    id=started.file.id,
                    upload_id=started.upload_id,
                    part_number=number,
                    expires_in=timedelta(minutes=5),
                ),
                ctx,
            )
            resp = await http.put(url.url, content=body, headers=url.headers)
            assert resp.status_code == 200
            landed.append(UploadPartDTO(part_number=number, etag=resp.headers["etag"].strip('"')))

    ready = await _complete(
        ctx,
        CompleteStoredFileUploadRequestDTO(
            id=started.file.id, upload_id=started.upload_id, parts=landed
        ),
    )

    assert ready.status is StoredFileStatus.READY
    assert await _download(ctx, ready.id) == b"".join(parts)


async def test_an_upload_larger_than_declared_is_deleted_and_failed(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    started = await _initiate(ctx, 4)

    async with httpx.AsyncClient() as http:
        await http.put(
            started.presigned.url, content=b"much more than 4", headers=started.presigned.headers
        )

    with pytest.raises(CoreException) as ei:
        await _complete(ctx, CompleteStoredFileUploadRequestDTO(id=started.file.id))

    assert ei.value.code == "upload_mismatch"
    assert (
        await ctx.doc.query(_KIT.document).get(started.file.id)
    ).status is StoredFileStatus.FAILED

    with pytest.raises(CoreException) as gone:
        await ctx.storage.query(_KIT.resolved_storage).head(started.file.storage_key)

    assert gone.value.kind is ExceptionKind.NOT_FOUND


@pytest.mark.parametrize("multipart", [False, True], ids=["single-put", "multipart"])
async def test_an_encrypting_route_refuses_a_direct_upload_before_any_row(
    s3_client: S3Client, s3_bucket: str, multipart: bool
) -> None:
    ctx = _ctx(s3_client, s3_bucket, encrypt=True)

    with pytest.raises(CoreException) as ei:
        await _initiate(ctx, 4, multipart=multipart)

    # Refused when the operation resolves its storage, before the adapter is reached.
    assert ei.value.code == "storage_direct_transfer_encrypted"
    assert await ctx.doc.query(_KIT.document).count() == 0


async def test_the_put_url_cannot_overwrite_a_ready_file(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    started = await _initiate(ctx, 5)
    url, headers = started.presigned.url, started.presigned.headers

    async with httpx.AsyncClient() as http:
        await http.put(url, content=b"hello", headers=headers)
        ready = await _complete(ctx, CompleteStoredFileUploadRequestDTO(id=started.file.id))
        again = await http.put(url, content=b"x" * 1024, headers=headers)

    assert again.status_code == 412
    assert await _download(ctx, ready.id) == b"hello"


async def test_multipart_completion_does_not_replace_an_existing_object(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    started = await _initiate(ctx, 5, multipart=True)
    url = await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.PRESIGN_UPLOAD_PART),
        PresignStoredFileUploadPartRequestDTO(
            id=started.file.id, upload_id=started.upload_id, part_number=1
        ),
        ctx,
    )

    async with httpx.AsyncClient() as http:
        resp = await http.put(url.url, content=b"other", headers=url.headers)

    async with s3_client.client():
        await s3_client.upload_bytes(s3_bucket, started.file.storage_key, b"hello")

    with pytest.raises(CoreException) as ei:
        await _complete(
            ctx,
            CompleteStoredFileUploadRequestDTO(
                id=started.file.id,
                upload_id=started.upload_id,
                parts=[UploadPartDTO(part_number=1, etag=resp.headers["etag"].strip('"'))],
            ),
        )

    async with s3_client.client():
        body = await s3_client.download_bytes(s3_bucket, started.file.storage_key)

    assert ei.value.kind is ExceptionKind.CONFLICT
    assert body.data == b"hello"


async def test_the_pending_sweep_cleans_an_abandoned_put_and_session(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket)

    with bind_time_source(FrozenTimeSource(instant=utcnow() - timedelta(hours=25))):
        put = await _initiate(ctx, 5)
        mpu = await _initiate(ctx, 6 * MIB, multipart=True)
        # Minted then too: each part URL touches the row, keeping it fresh.
        part = await run_operation(
            _REG,
            _NS.key(StoredFileKernelOp.PRESIGN_UPLOAD_PART),
            PresignStoredFileUploadPartRequestDTO(
                id=mpu.file.id, upload_id=mpu.upload_id, part_number=1
            ),
            ctx,
        )

    async with httpx.AsyncClient() as http:
        put_resp = await http.put(
            put.presigned.url, content=b"hello", headers=put.presigned.headers
        )
        part_resp = await http.put(part.url, content=b"abandoned-part", headers=part.headers)

    uploads = ctx.storage.uploads(_KIT.resolved_storage)
    assert (put_resp.status_code, part_resp.status_code) == (200, 200)
    assert [s.upload_id for s in await uploads.list_uploads(mpu.file.storage_key)] == [
        mpu.upload_id
    ]

    sweep = stored_file_pending_sweep_lifecycle_step(_KIT).startup
    await sweep._drain(ctx)  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]

    for started in (put, mpu):
        row = await ctx.doc.query(_KIT.document).get(started.file.id)
        assert row.status is StoredFileStatus.FAILED

    with pytest.raises(CoreException) as gone:
        await ctx.storage.query(_KIT.resolved_storage).head(put.file.storage_key)

    assert gone.value.kind is ExceptionKind.NOT_FOUND
    assert await uploads.list_uploads(mpu.file.storage_key) == []


async def test_the_pending_sweep_skips_a_row_an_encrypting_route_refuses(
    s3_client: S3Client, s3_bucket: str
) -> None:
    # Direct uploads began before the route started encrypting: their rows still hold keys.
    ctx = _ctx(s3_client, s3_bucket, encrypt=True)
    file_id = uuid4()
    key = str(file_id)

    with bind_time_source(FrozenTimeSource(instant=utcnow() - timedelta(hours=25))):
        await ctx.doc.command(_KIT.document).create(
            StoredFileCreateCmd(filename="a.bin", size=5, storage_key=key), id=file_id
        )

    async with s3_client.client():
        await s3_client.upload_bytes(s3_bucket, key, b"hello")

    sweep = stored_file_pending_sweep_lifecycle_step(_KIT).startup
    await sweep._drain(ctx)  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]

    row = await ctx.doc.query(_KIT.document).get(file_id)
    assert (row.status, row.storage_key) == (StoredFileStatus.FAILED, key)  # retried later

    with pytest.raises(CoreException) as gone:
        await ctx.storage.query(_KIT.resolved_storage).head(key)

    assert gone.value.kind is ExceptionKind.NOT_FOUND

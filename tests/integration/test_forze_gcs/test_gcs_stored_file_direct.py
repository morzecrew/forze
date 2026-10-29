"""Integration test (fake-gcs-server): stored-file ranged reads and direct uploads on GCS.

The stored-file rows live in the mock document store; the blobs live in a real bucket behind the
GCS storage adapter, so ranged reads, cache validators, presigned PUTs and compose-based multipart
are the real adapter's. fake-gcs-server does not verify V4 signatures, so the signed URLs are
rebased onto the emulator (see ``test_gcs_presign.py``); what is exercised is the data path and
the kit's checks on what landed.
"""

import json
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import pytest_asyncio
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import APIRouter, FastAPI

from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import FrozenTimeSource, bind_time_source, utcnow
from forze.testing import context_from_modules
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_stored_file_download_route
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.execution.deps.module import GCSDepsModule
from forze_gcs.kernel.client.client import GCSClient
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
from forze_kits.domain.stored_file import StoredFileKitSpec, StoredFileRead, StoredFileStatus
from forze_mock import MockDepsModule, MockState

# ----------------------- #

TEST_PROJECT_ID = "forze-gcs-test"
FAKE_GCS_PUBLIC_HOST = "0.0.0.0"  # the conftest's ``-public-host``

_KIT = StoredFileKitSpec(
    name="files", stream_downloads=True, range_downloads=True, direct_uploads=True
)
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


# ....................... #


@pytest.fixture(scope="module")
def service_account_file(tmp_path_factory: pytest.TempPathFactory) -> str:
    """A throwaway service-account key, so the client signs URLs locally."""

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    path = tmp_path_factory.mktemp("gcs-stored-file") / "service-account.json"
    path.write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": TEST_PROJECT_ID,
                "client_email": "signer@forze-gcs-test.iam.gserviceaccount.com",
                "private_key": pem,
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        )
    )

    return str(path)


@pytest_asyncio.fixture
async def gcs_signing_client(fake_gcs_container: str, service_account_file: str) -> GCSClient:
    _ = fake_gcs_container
    client = GCSClient()
    await client.initialize(TEST_PROJECT_ID, service_file=service_account_file)

    yield client

    await client.close()


async def _put(url: str, headers: dict[str, str], body: bytes, emulator: str) -> httpx.Response:
    """The client's PUT to a signed URL, rebased onto the emulator (which routes by Host)."""

    parts = urlsplit(url)

    async with httpx.AsyncClient() as http:
        return await http.put(
            f"{emulator}{parts.path}?{parts.query}",
            content=body,
            headers={"Host": FAKE_GCS_PUBLIC_HOST, **headers},
        )


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


async def _download(ctx: ExecutionContext, file_id) -> bytes:
    result = await run_operation(
        _REG, _NS.key(StoredFileKernelOp.DOWNLOAD), StoredFileIdDTO(id=file_id), ctx
    )
    return result.data


async def test_a_presigned_put_becomes_a_ready_file(
    gcs_signing_client: GCSClient, gcs_bucket: str, fake_gcs_container: str
) -> None:
    ctx = _ctx(gcs_signing_client, gcs_bucket)
    data = b"direct-upload-bytes"
    started = await _initiate(ctx, len(data))

    resp = await _put(started.presigned.url, started.presigned.headers, data, fake_gcs_container)
    ready = await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.COMPLETE_UPLOAD),
        CompleteStoredFileUploadRequestDTO(id=started.file.id),
        ctx,
    )

    assert resp.status_code == 200
    assert ready.status is StoredFileStatus.READY
    assert await _download(ctx, ready.id) == data


async def test_multipart_parts_compose_into_a_ready_file(
    gcs_signing_client: GCSClient, gcs_bucket: str, fake_gcs_container: str
) -> None:
    ctx = _ctx(gcs_signing_client, gcs_bucket)
    parts = [b"first-part-", b"second-part"]
    started = await _initiate(ctx, sum(map(len, parts)), multipart=True)
    landed: list[UploadPartDTO] = []

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
        resp = await _put(url.url, url.headers, body, fake_gcs_container)
        assert resp.status_code == 200
        landed.append(UploadPartDTO(part_number=number))

    ready = await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.COMPLETE_UPLOAD),
        CompleteStoredFileUploadRequestDTO(
            id=started.file.id, upload_id=started.upload_id, parts=landed
        ),
        ctx,
    )

    assert ready.status is StoredFileStatus.READY
    assert await _download(ctx, ready.id) == b"".join(parts)


async def test_a_content_type_other_than_declared_is_failed(
    gcs_signing_client: GCSClient, gcs_bucket: str, fake_gcs_container: str
) -> None:
    # Real GCS refuses this PUT, since the content type is a signed header. The emulator checks
    # no signature, so it stands in for a store that let a wrong content type through.
    ctx = _ctx(gcs_signing_client, gcs_bucket)
    started = await _initiate(ctx, 4)
    await _put(started.presigned.url, {"Content-Type": "text/html"}, b"evil", fake_gcs_container)

    with pytest.raises(CoreException) as ei:
        await run_operation(
            _REG,
            _NS.key(StoredFileKernelOp.COMPLETE_UPLOAD),
            CompleteStoredFileUploadRequestDTO(id=started.file.id),
            ctx,
        )

    assert ei.value.code == "upload_mismatch"
    assert (await ctx.doc.query(_KIT.document).get(started.file.id)).status is (
        StoredFileStatus.FAILED
    )


async def test_the_put_url_is_signed_create_only(
    gcs_signing_client: GCSClient, gcs_bucket: str
) -> None:
    # fake-gcs-server does not enforce x-goog-if-generation-match on a PUT (a second PUT
    # replaces the object), so this checks what real GCS enforces: the precondition is a
    # signed header the client must send.
    ctx = _ctx(gcs_signing_client, gcs_bucket)
    started = await _initiate(ctx, 5)

    assert started.presigned.headers["x-goog-if-generation-match"] == "0"
    signed = parse_qs(urlsplit(started.presigned.url).query)["X-Goog-SignedHeaders"]
    assert "x-goog-if-generation-match" in signed[0].split(";")


async def test_multipart_completion_does_not_replace_an_existing_object(
    gcs_signing_client: GCSClient, gcs_bucket: str, fake_gcs_container: str
) -> None:
    ctx = _ctx(gcs_signing_client, gcs_bucket)
    started = await _initiate(ctx, 5, multipart=True)

    for number, body in ((1, b"oth"), (2, b"er")):
        url = await run_operation(
            _REG,
            _NS.key(StoredFileKernelOp.PRESIGN_UPLOAD_PART),
            PresignStoredFileUploadPartRequestDTO(
                id=started.file.id, upload_id=started.upload_id, part_number=number
            ),
            ctx,
        )
        await _put(url.url, url.headers, body, fake_gcs_container)

    async with gcs_signing_client.client():
        await gcs_signing_client.upload_bytes(gcs_bucket, started.file.storage_key, b"hello")

    with pytest.raises(CoreException) as ei:
        await run_operation(
            _REG,
            _NS.key(StoredFileKernelOp.COMPLETE_UPLOAD),
            CompleteStoredFileUploadRequestDTO(
                id=started.file.id,
                upload_id=started.upload_id,
                parts=[UploadPartDTO(part_number=1), UploadPartDTO(part_number=2)],
            ),
            ctx,
        )

    async with gcs_signing_client.client():
        body = await gcs_signing_client.download_bytes(gcs_bucket, started.file.storage_key)

    assert ei.value.kind is ExceptionKind.CONFLICT
    assert body.data == b"hello"


async def test_the_pending_sweep_cleans_an_abandoned_put_and_session(
    gcs_signing_client: GCSClient, gcs_bucket: str, fake_gcs_container: str
) -> None:
    ctx = _ctx(gcs_signing_client, gcs_bucket)

    with bind_time_source(FrozenTimeSource(instant=utcnow() - timedelta(hours=25))):
        put = await _initiate(ctx, 5)
        mpu = await _initiate(ctx, 64, multipart=True)

    part = await run_operation(
        _REG,
        _NS.key(StoredFileKernelOp.PRESIGN_UPLOAD_PART),
        PresignStoredFileUploadPartRequestDTO(
            id=mpu.file.id,
            upload_id=mpu.upload_id,
            part_number=1,
            expires_in=timedelta(minutes=5),
        ),
        ctx,
    )
    put_resp = await _put(put.presigned.url, put.presigned.headers, b"hello", fake_gcs_container)
    part_resp = await _put(part.url, part.headers, b"abandoned-part", fake_gcs_container)

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

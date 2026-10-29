"""Integration test (RustFS and floci): stored-file streaming and presigned downloads on S3.

The stored-file rows live in the mock document store; the blobs live in a real bucket behind the
S3 storage adapter, so key validation, tenant prefixes, presigning and the encryption refusal are
the real adapter's.
"""

from datetime import timedelta
from uuid import uuid4

import httpx
import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CryptoDepsModule, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_kits.aggregates.stored_file import (
    PresignStoredFileDownloadRequestDTO,
    StoredFileIdDTO,
    StoredFileIdRevDTO,
    StoredFileKernelOp,
    UploadStoredFileRequestDTO,
    freeze_stored_file_registry,
)
from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileKitSpec,
    StoredFileRead,
    StoredFileStatus,
    StoredFileUpdateCmd,
)
from forze_mock import MockDepsModule, MockKeyManagement, MockRouteConfig, MockState
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client

# ----------------------- #

MIB = 1024 * 1024

_KIT = StoredFileKitSpec(name="files", stream_downloads=True, presign_downloads=True)
_KEY = _KIT.document.default_namespace.key
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
        _KEY(StoredFileKernelOp.UPLOAD),
        UploadStoredFileRequestDTO(filename="report.bin", data=data),
        ctx,
    )
    return await ctx.doc.query(_KIT.document).get(created.id)


def _presign_args(file: StoredFileRead) -> PresignStoredFileDownloadRequestDTO:
    return PresignStoredFileDownloadRequestDTO(id=file.id, expires_in=timedelta(minutes=5))


# ....................... #


@pytest.mark.parametrize("encrypt", [False, True], ids=["plain", "encrypted"])
async def test_a_ready_file_streams_from_the_bucket(
    s3_client: S3Client, s3_bucket: str, encrypt: bool
) -> None:
    ctx = _ctx(s3_client, s3_bucket, encrypt=encrypt)
    data = bytes(range(256)) * (9 * MIB // 256)  # > the 8 MiB stream part size
    file = await _ready(ctx, data)

    result = await run_operation(
        _REG, _KEY(StoredFileKernelOp.DOWNLOAD_STREAM), StoredFileIdDTO(id=file.id), ctx
    )

    assert result.stream.filename == "report.bin"
    assert result.stream.size == (None if encrypt else len(data))
    assert b"".join([chunk async for chunk in result.stream.chunks]) == data


async def test_a_delete_mid_download_fails_the_stream(s3_client: S3Client, s3_bucket: str) -> None:
    # The stream issues one ranged GET per part as it is read. A purge between two parts
    # makes the next GET raise, so a transport aborts the response rather than ending it short.
    ctx = _ctx(s3_client, s3_bucket)
    data = bytes(range(256)) * (9 * MIB // 256)  # two 8 MiB stream parts
    file = await _ready(ctx, data)

    result = await run_operation(
        _REG, _KEY(StoredFileKernelOp.DOWNLOAD_STREAM), StoredFileIdDTO(id=file.id), ctx
    )
    chunks = aiter(result.stream.chunks)
    first = await anext(chunks)

    await run_operation(
        _REG, _KEY(StoredFileKernelOp.DELETE), StoredFileIdRevDTO(id=file.id, rev=file.rev), ctx
    )

    with pytest.raises(CoreException) as ei:
        async for _ in chunks:
            pass

    assert len(first) < len(data)
    assert ei.value.kind is ExceptionKind.NOT_FOUND


async def test_the_presigned_url_fetches_the_file_without_credentials(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    file = await _ready(ctx, b"stored-file-presigned")

    result = await run_operation(
        _REG, _KEY(StoredFileKernelOp.PRESIGN_DOWNLOAD), _presign_args(file), ctx
    )

    async with httpx.AsyncClient() as http:
        resp = await http.get(result.presigned.url)

    assert resp.status_code == 200
    assert resp.content == b"stored-file-presigned"
    assert resp.headers["content-disposition"] == 'attachment; filename="report.bin"'


async def test_an_encrypting_route_refuses_to_presign(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket, encrypt=True)
    file = await _ready(ctx, b"sealed")

    with pytest.raises(CoreException) as ei:
        await run_operation(
            _REG, _KEY(StoredFileKernelOp.PRESIGN_DOWNLOAD), _presign_args(file), ctx
        )

    # Refused when the operation resolves its direct-transfer storage, before any call.
    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "storage_direct_transfer_encrypted"


async def test_a_row_naming_another_tenants_key_is_refused(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket, tenant_aware=True)

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
        theirs = await _ready(ctx, b"tenant-a-only")

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
        # A row of this tenant that points at the other tenant's object, as a corrupted or
        # forged row would: the adapter's tenant-prefix check refuses it on both paths.
        doc = ctx.doc.command(_KIT.document)
        pending = await doc.create(
            StoredFileCreateCmd(filename="forged.bin", size=13, status=StoredFileStatus.PENDING)
        )
        forged = await doc.update(
            pk=pending.id,
            rev=pending.rev,
            dto=StoredFileUpdateCmd(storage_key=theirs.storage_key, status=StoredFileStatus.READY),
        )

        for op, args in (
            (StoredFileKernelOp.DOWNLOAD_STREAM, StoredFileIdDTO(id=forged.id)),
            (StoredFileKernelOp.PRESIGN_DOWNLOAD, _presign_args(forged)),
        ):
            with pytest.raises(CoreException) as ei:
                await run_operation(_REG, _KEY(op), args, ctx)

            assert ei.value.code == "core.storage.key_outside_tenant"

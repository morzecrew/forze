"""Live MinIO round-trips for S3 presigned URLs.

The point of presigning is that the application leaves the data path: every
transfer below goes over **plain HTTP with no credentials** — only the signed
URL (plus any signature-bound headers) authorizes it.
"""

import asyncio
from datetime import timedelta

import httpx
import pytest

from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.base.exceptions import CoreException, ExceptionKind
from forze_s3.kernel.client import S3Client

# ----------------------- #


@pytest.mark.asyncio
async def test_presigned_download_get_over_plain_http(
    s3_client: S3Client, s3_bucket: str
) -> None:
    payload = b"forze-presigned-download"

    async with s3_client.client():
        await s3_client.upload_bytes(
            s3_bucket,
            "docs/report.txt",
            payload,
            content_type="text/plain",
        )

        vo = await s3_client.presign_download_url(
            s3_bucket,
            "docs/report.txt",
            expires_in=timedelta(minutes=5),
        )

    assert vo.method == "GET"

    # No auth, no SDK: the URL alone grants the read.
    async with httpx.AsyncClient() as http:
        resp = await http.get(vo.url)

    assert resp.status_code == 200
    assert resp.content == payload


@pytest.mark.asyncio
async def test_presigned_upload_put_with_bound_content_type(
    s3_client: S3Client, s3_bucket: str
) -> None:
    payload = b"uploaded-via-presigned-put"

    async with s3_client.client():
        vo = await s3_client.presign_upload_url(
            s3_bucket,
            "incoming/data.bin",
            expires_in=timedelta(minutes=5),
            content_type="application/octet-stream",
        )

    assert vo.method == "PUT"
    assert dict(vo.headers) == {"Content-Type": "application/octet-stream"}

    # The signature binds Content-Type: send exactly the headers from the VO.
    async with httpx.AsyncClient() as http:
        resp = await http.put(vo.url, content=payload, headers=dict(vo.headers))

    assert resp.status_code in (200, 204)

    # The object is now readable through the normal client port.
    async with s3_client.client():
        body = await s3_client.download_bytes(s3_bucket, "incoming/data.bin")
        head = await s3_client.head_object(s3_bucket, "incoming/data.bin")

    assert body.data == payload
    assert head.content_type == "application/octet-stream"


@pytest.mark.asyncio
async def test_presigned_upload_rejects_unbound_content_type(
    s3_client: S3Client,
    s3_bucket: str,
    s3_backend,  # noqa: ANN001 - session backend fixture
) -> None:
    """SigV4 binds ContentType: a PUT with a different one must not verify."""

    if s3_backend.name == "floci":
        # Emulator infidelity, not an adapter concern: floci (1.5.32) does not
        # verify signed headers on presigned PUTs, so the mismatched upload is
        # accepted. Real S3 and MinIO reject it; the MinIO leg asserts the
        # property on every run.
        pytest.skip("floci does not enforce SigV4 signed-header binding")

    async with s3_client.client():
        vo = await s3_client.presign_upload_url(
            s3_bucket,
            "incoming/strict.txt",
            expires_in=timedelta(minutes=5),
            content_type="text/plain",
        )

    async with httpx.AsyncClient() as http:
        resp = await http.put(
            vo.url,
            content=b"x",
            headers={"Content-Type": "application/json"},
        )

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_presigned_download_url_expires(
    s3_client: S3Client,
    s3_bucket: str,
    s3_backend,  # noqa: ANN001 - session backend fixture
) -> None:
    if s3_backend.name == "floci":
        # Emulator infidelity, not an adapter concern: floci's presigned-URL
        # verification is immature (floci-io/floci#1841) and its expiry
        # enforcement proved environment-dependent — a 1s-expiry URL dies
        # locally but never expires on CI runners. MinIO asserts the property
        # on every run.
        pytest.skip("floci presigned-URL expiry enforcement is unreliable")

    async with s3_client.client():
        await s3_client.upload_bytes(s3_bucket, "fleeting.txt", b"gone-soon")

        vo = await s3_client.presign_download_url(
            s3_bucket,
            "fleeting.txt",
            expires_in=timedelta(seconds=1),
        )

    # SigV4 expiry has one-second timestamp granularity and the server checks
    # it against its own clock, so the exact moment the URL dies can drift by
    # a few seconds on a loaded runner: poll until it does instead of pinning
    # a single instant.
    await asyncio.sleep(1.5)
    deadline = asyncio.get_running_loop().time() + 15

    async with httpx.AsyncClient() as http:
        while True:
            resp = await http.get(vo.url)

            if resp.status_code == 403:
                break

            assert resp.status_code == 200  # a still-valid read stays well-formed
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail("presigned URL did not expire within 15s")

            await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_presign_rejects_expiry_over_seven_days_before_signing(
    s3_client: S3Client, s3_bucket: str
) -> None:
    async with s3_client.client():
        with pytest.raises(CoreException) as ei:
            await s3_client.presign_download_url(
                s3_bucket,
                "k",
                expires_in=timedelta(days=8),
            )

    assert ei.value.kind is ExceptionKind.VALIDATION


# ----------------------- #
# full port surface: handler-facing adapter -> presign -> raw HTTP


@pytest.mark.asyncio
async def test_storage_adapter_presign_roundtrip(
    s3_client: S3Client, s3_bucket: str
) -> None:
    from forze_s3.execution.deps.configs import S3StorageConfig
    from forze_s3.execution.deps.module import S3DepsModule
    from tests.support.execution_context import context_from_deps

    ctx = context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket)},
        )()
    )
    spec = StorageSpec(name=s3_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    stored = await storage_c.upload(
        UploadedObject(
            filename="contract.txt",
            data=b"adapter-presign-roundtrip",
            prefix="inbox",
        ),
    )

    # Download leg: presign the stored key, fetch it with a plain GET.
    download = await storage_q.presign_download(
        stored.key,
        expires_in=timedelta(minutes=5),
    )

    async with httpx.AsyncClient() as http:
        resp = await http.get(download.url)

    assert resp.status_code == 200
    assert resp.content == b"adapter-presign-roundtrip"

    # Upload leg: presign a caller-supplied key and PUT bytes directly.
    upload = await storage_c.presign_upload(
        "inbox/direct-upload.bin",
        expires_in=timedelta(minutes=5),
        content_type="application/octet-stream",
    )

    async with httpx.AsyncClient() as http:
        resp = await http.put(
            upload.url,
            content=b"direct-bytes",
            headers=dict(upload.headers),
        )

    assert resp.status_code in (200, 204)

    # Presign-uploaded objects bypass the adapter's metadata envelope, so they
    # read back through the raw client port (not the enriched download()).
    async with s3_client.client():
        body = await s3_client.download_bytes(s3_bucket, "inbox/direct-upload.bin")

    assert body.data == b"direct-bytes"

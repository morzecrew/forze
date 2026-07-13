"""Integration tests (MinIO) for resumable multipart upload sessions.

Full real flow through the public ``StorageUploadSessionPort``: begin →
presign N parts → PUT each part directly via httpx **in parallel** (each
non-last part >= 5 MiB, as MinIO enforces the S3 minimum) → collect ETags →
complete → download the assembled object == concatenation. Plus resume
(upload 2 of 3, list, upload the 3rd, complete) and abort.
"""

import asyncio
from datetime import timedelta

import httpx
import pytest

from forze.application.contracts.storage import (
    StorageSpec,
    UploadPart,
)
from forze.base.exceptions import CoreException
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client
from tests.support.execution_context import context_from_deps

# ----------------------- #

# MinIO enforces the S3 5 MiB minimum for every part except the last.
MIB = 1024 * 1024
PART_SIZE = 5 * MIB


def _ctx(s3_client: S3Client, bucket: str):
    return context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={bucket: S3StorageConfig(bucket=bucket)},
        )()
    )


async def _put_part(http: httpx.AsyncClient, url: str, data: bytes) -> str:
    resp = await http.put(url, content=data)
    resp.raise_for_status()
    return resp.headers["ETag"]


# ----------------------- #


@pytest.mark.asyncio
async def test_multipart_full_flow_parallel(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    uploads = ctx.storage.uploads(spec)
    q = ctx.storage.query(spec)

    key = "multipart/parallel.bin"
    bodies = [b"A" * PART_SIZE, b"B" * PART_SIZE, b"C" * 1024]  # last part small

    session = await uploads.begin_upload(key, content_type="application/octet-stream")

    # Presign all parts, then PUT them in PARALLEL.
    urls = await asyncio.gather(
        *(
            uploads.presign_part(session, n, expires_in=timedelta(minutes=10))
            for n in range(1, len(bodies) + 1)
        )
    )

    async with httpx.AsyncClient(timeout=60) as http:
        etags = await asyncio.gather(
            *(_put_part(http, url.url, body) for url, body in zip(urls, bodies, strict=True))
        )

    parts = [UploadPart(part_number=n, etag=etag) for n, etag in enumerate(etags, start=1)]

    head = await uploads.complete_upload(session, parts)
    assert head.size == sum(len(b) for b in bodies)

    dl = await q.download_range(key, start=0)  # full object
    assert dl.data == b"".join(bodies)


@pytest.mark.asyncio
async def test_multipart_resume(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    uploads = ctx.storage.uploads(spec)
    q = ctx.storage.query(spec)

    key = "multipart/resume.bin"
    bodies = [b"X" * PART_SIZE, b"Y" * PART_SIZE, b"Z" * 2048]

    session = await uploads.begin_upload(key)

    # Upload only the first 2 parts.
    collected: dict[int, str] = {}
    async with httpx.AsyncClient(timeout=60) as http:
        for n in (1, 2):
            url = await uploads.presign_part(session, n, expires_in=timedelta(minutes=10))
            collected[n] = await _put_part(http, url.url, bodies[n - 1])

    # Resume: list shows 2 landed parts.
    landed = await uploads.list_parts(session)
    assert sorted(p.part_number for p in landed) == [1, 2]

    # Upload the missing 3rd part.
    async with httpx.AsyncClient(timeout=60) as http:
        url = await uploads.presign_part(session, 3, expires_in=timedelta(minutes=10))
        collected[3] = await _put_part(http, url.url, bodies[2])

    parts = [UploadPart(part_number=n, etag=collected[n]) for n in (1, 2, 3)]
    head = await uploads.complete_upload(session, parts)
    assert head.size == sum(len(b) for b in bodies)

    dl = await q.download_range(key, start=0)
    assert dl.data == b"".join(bodies)


@pytest.mark.asyncio
async def test_multipart_abort_then_use_errors(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    uploads = ctx.storage.uploads(spec)

    key = "multipart/aborted.bin"
    session = await uploads.begin_upload(key)

    async with httpx.AsyncClient(timeout=60) as http:
        url = await uploads.presign_part(session, 1, expires_in=timedelta(minutes=10))
        etag = await _put_part(http, url.url, b"Q" * PART_SIZE)

    await uploads.abort_upload(session)

    # After abort, list / complete error (the upload is gone).
    with pytest.raises(CoreException):
        await uploads.list_parts(session)

    with pytest.raises(CoreException):
        await uploads.complete_upload(session, [UploadPart(part_number=1, etag=etag)])


# ....................... #
# Conditional multipart completion (the client's If-Match seam).
#
# Runs against both matrix backends: MinIO and floci were each probed to honor
# CompleteMultipartUpload's If-Match natively — 412 PreconditionFailed on a
# mismatched ETag, 404 NoSuchKey when the target vanished — so these tests are
# a live fidelity check, not a special-cased emulation.


@pytest.mark.asyncio
async def test_conditional_completion_refuses_a_stale_etag(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """A mismatched If-Match answers `conflict` and the stored bytes survive."""

    from forze.application.contracts.storage import OVERWRITE_PRECONDITION_FAILED_CODE
    from forze.application.integrations.storage.client import ObjectStoragePartInfo

    key = "conditional/stale.bin"

    async with s3_client.client():
        await s3_client.upload_bytes(bucket=s3_bucket, key=key, data=b"current")
        stale = "d41d8cd98f00b204e9800998ecf8427e"  # a valid-shaped, wrong ETag

        upload_id = await s3_client.create_multipart_upload(bucket=s3_bucket, key=key)
        part = await s3_client.upload_multipart_part(
            bucket=s3_bucket, key=key, upload_id=upload_id, part_number=1, data=b"new"
        )

        with pytest.raises(CoreException) as ei:
            await s3_client.complete_multipart_upload(
                bucket=s3_bucket,
                key=key,
                upload_id=upload_id,
                parts=[ObjectStoragePartInfo(part_number=1, etag=part.etag)],
                if_match=stale,
            )

        assert ei.value.code == OVERWRITE_PRECONDITION_FAILED_CODE

        body = await s3_client.download_bytes(bucket=s3_bucket, key=key)
        assert body.data == b"current"


@pytest.mark.asyncio
async def test_conditional_completion_with_the_current_etag_lands(
    s3_client: S3Client, s3_bucket: str
) -> None:
    from forze.application.integrations.storage.client import ObjectStoragePartInfo

    key = "conditional/current.bin"

    async with s3_client.client():
        await s3_client.upload_bytes(bucket=s3_bucket, key=key, data=b"current")
        head = await s3_client.head_object(bucket=s3_bucket, key=key)

        upload_id = await s3_client.create_multipart_upload(bucket=s3_bucket, key=key)
        part = await s3_client.upload_multipart_part(
            bucket=s3_bucket, key=key, upload_id=upload_id, part_number=1, data=b"new"
        )

        await s3_client.complete_multipart_upload(
            bucket=s3_bucket,
            key=key,
            upload_id=upload_id,
            parts=[ObjectStoragePartInfo(part_number=1, etag=part.etag)],
            if_match=head.etag,
        )

        body = await s3_client.download_bytes(bucket=s3_bucket, key=key)
        assert body.data == b"new"


@pytest.mark.asyncio
async def test_conditional_completion_over_a_deleted_object_does_not_recreate_it(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """The resurrection window itself, at the client seam: the object is deleted
    while the multipart upload is in flight; the conditional completion must
    answer not_found and — critically — leave nothing behind at the key."""

    from forze.application.integrations.storage.client import ObjectStoragePartInfo
    from forze.base.exceptions import ExceptionKind

    key = "conditional/deleted.bin"

    async with s3_client.client():
        await s3_client.upload_bytes(bucket=s3_bucket, key=key, data=b"current")
        head = await s3_client.head_object(bucket=s3_bucket, key=key)

        upload_id = await s3_client.create_multipart_upload(bucket=s3_bucket, key=key)
        part = await s3_client.upload_multipart_part(
            bucket=s3_bucket, key=key, upload_id=upload_id, part_number=1, data=b"boo"
        )

        await s3_client.delete_object(bucket=s3_bucket, key=key)

        with pytest.raises(CoreException) as ei:
            await s3_client.complete_multipart_upload(
                bucket=s3_bucket,
                key=key,
                upload_id=upload_id,
                parts=[ObjectStoragePartInfo(part_number=1, etag=part.etag)],
                if_match=head.etag,
            )

        assert ei.value.kind is ExceptionKind.NOT_FOUND
        assert not await s3_client.object_exists(bucket=s3_bucket, key=key)

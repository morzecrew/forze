"""Integration tests (MinIO) for the new storage metadata & access ops.

Exercises head / copy / move / download_range / download_if_changed /
put_object_tags end-to-end through the public storage ports against a real
S3-compatible backend.
"""

import pytest

from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client
from tests.support.execution_context import context_from_deps


def _ctx(s3_client: S3Client, bucket: str):
    return context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={bucket: S3StorageConfig(bucket=bucket)},
        )()
    )


@pytest.mark.asyncio
async def test_head_after_upload(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    q = ctx.storage.query(spec)
    c = ctx.storage.command(spec)

    body = b"head-me-please"
    stored = await c.upload(
        UploadedObject(filename="h.txt", data=body, prefix="heads")
    )

    head = await q.head(stored.key)
    assert head.size == len(body)
    assert head.content_type == "text/plain"
    assert head.etag  # non-empty


@pytest.mark.asyncio
async def test_copy_and_move(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    q = ctx.storage.query(spec)
    c = ctx.storage.command(spec)

    body = b"copy-me"
    stored = await c.upload(
        UploadedObject(filename="c.txt", data=body, prefix="src")
    )

    # copy: both keys exist, same bytes.
    copy_head = await c.copy(stored.key, "dst/copied.txt")
    assert copy_head.size == len(body)

    src_dl = await q.download(stored.key)
    dst_dl = await q.download("dst/copied.txt")
    assert src_dl.data == dst_dl.data == body

    # move: only destination remains.
    await c.move(stored.key, "dst/moved.txt")
    moved = await q.download("dst/moved.txt")
    assert moved.data == body

    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException):
        await q.download(stored.key)


@pytest.mark.asyncio
async def test_download_range(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    q = ctx.storage.query(spec)
    c = ctx.storage.command(spec)

    body = b"0123456789"
    stored = await c.upload(
        UploadedObject(filename="r.bin", data=body, prefix="ranges")
    )

    ranged = await q.download_range(stored.key, start=2, end=5)
    assert ranged.data == b"2345"
    assert ranged.content_range == "bytes 2-5/10"
    assert ranged.total_size == 10

    open_ended = await q.download_range(stored.key, start=7)
    assert open_ended.data == b"789"


@pytest.mark.asyncio
async def test_download_range_unsatisfiable(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    q = ctx.storage.query(spec)
    c = ctx.storage.command(spec)

    stored = await c.upload(
        UploadedObject(filename="small.bin", data=b"abc", prefix="ranges")
    )

    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException) as ei:
        await q.download_range(stored.key, start=99)

    assert ei.value.code == "range_not_satisfiable"


@pytest.mark.asyncio
async def test_download_if_changed(s3_client: S3Client, s3_bucket: str) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    q = ctx.storage.query(spec)
    c = ctx.storage.command(spec)

    body = b"conditional-body"
    stored = await c.upload(
        UploadedObject(filename="cond.txt", data=body, prefix="cond")
    )

    head = await q.head(stored.key)

    # Matching ETag → not modified → None.
    unchanged = await q.download_if_changed(stored.key, if_none_match=head.etag)
    assert unchanged is None

    # Stale ETag → changed → bytes.
    changed = await q.download_if_changed(stored.key, if_none_match='"stale"')
    assert changed is not None
    assert changed.data == body


@pytest.mark.asyncio
async def test_put_object_tags_then_head(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    q = ctx.storage.query(spec)
    c = ctx.storage.command(spec)

    stored = await c.upload(
        UploadedObject(filename="t.txt", data=b"tagme", prefix="tags")
    )

    await c.put_object_tags(stored.key, {"env": "prod", "team": "core"})

    head = await q.head(stored.key, include_tags=True)
    assert head.tags == {"env": "prod", "team": "core"}

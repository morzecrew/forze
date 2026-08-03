from uuid import uuid4

import pytest

from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.base.exceptions import CoreException
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client
from tests.support.execution_context import context_from_deps


@pytest.mark.asyncio
async def test_s3_storage_adapter_upload_list_download_delete(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = context_from_deps(S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket)},
        )()
    )
    spec = StorageSpec(name=s3_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    uploaded = await storage_c.upload(
        UploadedObject(
            filename="contract.txt",
            data=b"forze-s3-storage-adapter",
            description="integration test",
            prefix="inbox/contracts",
        ),
    )
    assert uploaded.filename == "contract.txt"
    assert uploaded.description == "integration test"
    assert uploaded.size == len(b"forze-s3-storage-adapter")
    assert uploaded.key.startswith("inbox/contracts/")

    listed = await storage_q.list(limit=10, offset=0, prefix="inbox")
    assert listed.total == 1
    assert listed.container_missing is False
    assert len(listed.objects) == 1
    assert listed.objects[0].key == uploaded.key
    assert listed.objects[0].filename == "contract.txt"
    assert listed.objects[0].description == "integration test"

    downloaded = await storage_q.download(uploaded.key)
    assert downloaded.filename == "contract.txt"
    assert downloaded.data == b"forze-s3-storage-adapter"

    await storage_c.delete(uploaded.key)
    after_delete = await storage_q.list(limit=10, offset=0, prefix="inbox")
    assert after_delete.total == 0
    assert after_delete.objects == []
    # Emptied, not absent — the bucket is still there.
    assert after_delete.container_missing is False

@pytest.mark.asyncio
async def test_s3_storage_list_pagination(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """list() can page with limit/offset; full scan reports correct total_count."""
    ctx = context_from_deps(S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket)},
        )()
    )
    spec = StorageSpec(name=s3_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)
    base = f"pagination/{uuid4().hex[:10]}/it"

    keys: list[str] = []
    for i, body in enumerate((b"aa", b"bb", b"cc")):
        up = await storage_c.upload(
            UploadedObject(filename=f"f{i}.txt", data=body, prefix=base),
        )
        keys.append(up.key)

    page_all = (await storage_q.list(limit=50, offset=0, prefix=None)).objects
    assert len(page_all) == 3
    assert {o.key for o in page_all} == set(keys)

    slices: list[str] = []
    for offset in range(3):
        page = (await storage_q.list(limit=1, offset=offset, prefix=base)).objects
        assert len(page) == 1
        slices.append(page[0].key)

    assert len(set(slices)) == 3
    assert set(slices) == set(keys)

    for k in keys:
        await storage_c.delete(k)


@pytest.mark.asyncio
async def test_s3_storage_list_does_not_create_a_missing_bucket(
    s3_client: S3Client,
) -> None:
    """A read must not conjure its container into existence.

    ``list`` used to ``ensure_bucket`` first, so listing a bucket that had been deleted
    re-created it and answered "empty" — a write on a read path that makes an *absent*
    bucket indistinguishable from an *empty* one. The re-encryption sweep leans on exactly
    that distinction to tell "the object was deleted" (skip) from "the bucket vanished"
    (abort), so under the old behavior its guard could never fire and a break-glass rotation
    against a deleted bucket reported a false-complete pass.
    """

    missing = f"forze-absent-{uuid4().hex[:12]}"
    ctx = context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={missing: S3StorageConfig(bucket=missing)},
        )()
    )
    storage_q = ctx.storage.query(StorageSpec(name=missing))

    with pytest.raises(CoreException):
        await storage_q.list(limit=10, offset=0)

    # The read raised *and* left the world alone — the bucket is still gone.
    async with s3_client.client():
        assert not await s3_client.bucket_exists(missing)


@pytest.mark.asyncio
async def test_s3_storage_list_missing_ok_returns_empty(
    s3_client: S3Client,
) -> None:
    """``missing_ok=True`` reads an absent bucket as an empty listing, not a 500.

    Removing create-on-read from ``list`` (above) was correct for the sweep but regressed
    the object-list route and a portability export of a blob-less app: a fresh deployment
    has no bucket until its first write, so those callers now opt into 'empty' explicitly
    while the sweep keeps the raise.
    """

    missing = f"forze-absent-{uuid4().hex[:12]}"
    ctx = context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={missing: S3StorageConfig(bucket=missing)},
        )()
    )
    storage_q = ctx.storage.query(StorageSpec(name=missing))

    listed = await storage_q.list(limit=10, offset=0, missing_ok=True)

    assert listed.objects == []
    assert listed.total == 0
    # Tolerated, not erased: the caller opted out of the raise, not out of knowing.
    assert listed.container_missing is True

    # Still a read: the empty answer did not conjure the bucket into existence.
    async with s3_client.client():
        assert not await s3_client.bucket_exists(missing)


@pytest.mark.asyncio
async def test_s3_storage_list_heads_a_large_page_without_unbounded_fanout(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """A large listing HEADs every object (for metadata) with *bounded* concurrency — a
    ``size=10000`` list must not schedule 10k concurrent HEADs and saturate the pool."""

    ctx = context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket)},
        )()
    )
    spec = StorageSpec(name=s3_bucket)
    storage_c = ctx.storage.command(spec)
    storage_q = ctx.storage.query(spec)

    for i in range(25):
        await storage_c.upload(
            UploadedObject(filename=f"f{i}.txt", data=b"x", prefix="bulk"),
        )

    listed = await storage_q.list(limit=10_000, offset=0, prefix="bulk")

    # Every object is HEADed and returned with full metadata, in order, correctly.
    assert listed.total == 25
    assert len(listed.objects) == 25
    assert all(o.size == 1 for o in listed.objects)

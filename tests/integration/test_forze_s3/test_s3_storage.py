import pytest

from uuid import uuid4

from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionContext
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.platform.client import S3Client


@pytest.mark.asyncio
async def test_s3_storage_adapter_upload_list_download_delete(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = ExecutionContext(
        deps=S3DepsModule(
            client=s3_client,
            storages={s3_bucket: {"bucket": s3_bucket}},
        )()
    )
    storage = ctx.storage(StorageSpec(name=s3_bucket))

    uploaded = await storage.upload(
        filename="contract.txt",
        data=b"forze-s3-storage-adapter",
        description="integration test",
        prefix="inbox/contracts",
    )
    assert uploaded["filename"] == "contract.txt"
    assert uploaded["description"] == "integration test"
    assert uploaded["size"] == len(b"forze-s3-storage-adapter")
    assert uploaded["key"].startswith("inbox/contracts/")

    listed, total_count = await storage.list(limit=10, offset=0, prefix="inbox")
    assert total_count == 1
    assert len(listed) == 1
    assert listed[0]["key"] == uploaded["key"]
    assert listed[0]["filename"] == "contract.txt"
    assert listed[0]["description"] == "integration test"

    downloaded = await storage.download(uploaded["key"])
    assert downloaded["filename"] == "contract.txt"
    assert downloaded["data"] == b"forze-s3-storage-adapter"

    await storage.delete(uploaded["key"])
    listed_after_delete, total_after_delete = await storage.list(
        limit=10, offset=0, prefix="inbox"
    )
    assert total_after_delete == 0
    assert listed_after_delete == []


@pytest.mark.asyncio
async def test_s3_storage_list_pagination(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """list() can page with limit/offset; full scan reports correct total_count.

    Note: ``S3Client.list_objects`` may stop the S3 paginator once the
    requested window is satisfied, so *total_count* is only reliable when the
    listing runs to completion (large limit or full result set fits one page).
    """
    ctx = ExecutionContext(
        deps=S3DepsModule(
            client=s3_client,
            storages={s3_bucket: {"bucket": s3_bucket}},
        )()
    )
    storage = ctx.storage(StorageSpec(name=s3_bucket))
    base = f"pagination/{uuid4().hex[:10]}/it"

    keys: list[str] = []
    for i, body in enumerate((b"aa", b"bb", b"cc")):
        up = await storage.upload(
            filename=f"f{i}.txt",
            data=body,
            prefix=base,
        )
        keys.append(up["key"])

    # Bucket is test-isolated; list without prefix to verify three distinct keys.
    page_all, total_all = await storage.list(limit=50, offset=0, prefix=None)
    assert total_all == 3
    assert len(page_all) == 3
    assert {o["key"] for o in page_all} == set(keys)

    slices: list[str] = []
    for offset in range(3):
        page, _total = await storage.list(limit=1, offset=offset, prefix=base)
        assert len(page) == 1
        slices.append(page[0]["key"])

    assert len(set(slices)) == 3
    assert set(slices) == set(keys)

    for k in keys:
        await storage.delete(k)

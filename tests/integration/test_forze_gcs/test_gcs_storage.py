import pytest

from uuid import uuid4

from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.application.execution import ExecutionContext
from forze_gcs.execution.deps.module import GCSDepsModule
from forze_gcs.kernel.platform.client import GCSClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_storage_adapter_upload_list_download_delete(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    ctx = ExecutionContext(
        deps=GCSDepsModule(
            client=gcs_client,
            storages={gcs_bucket: {"bucket": gcs_bucket}},
        )()
    )
    storage = ctx.storage(StorageSpec(name=gcs_bucket))

    uploaded = await storage.upload(
        UploadedObject(
            filename="contract.txt",
            data=b"forze-gcs-storage-adapter",
            description="integration test",
            prefix="inbox/contracts",
        ),
    )
    assert uploaded.filename == "contract.txt"
    assert uploaded.description == "integration test"
    assert uploaded.size == len(b"forze-gcs-storage-adapter")
    assert uploaded.key.startswith("inbox/contracts/")

    listed, total_count = await storage.list(limit=10, offset=0, prefix="inbox")
    assert total_count == 1
    assert len(listed) == 1
    assert listed[0].key == uploaded.key
    assert listed[0].filename == "contract.txt"
    assert listed[0].description == "integration test"

    downloaded = await storage.download(uploaded.key)
    assert downloaded.filename == "contract.txt"
    assert downloaded.data == b"forze-gcs-storage-adapter"

    await storage.delete(uploaded.key)
    listed_after_delete, total_after_delete = await storage.list(
        limit=10, offset=0, prefix="inbox"
    )
    assert total_after_delete == 0
    assert listed_after_delete == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_storage_list_pagination(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    ctx = ExecutionContext(
        deps=GCSDepsModule(
            client=gcs_client,
            storages={gcs_bucket: {"bucket": gcs_bucket}},
        )()
    )
    storage = ctx.storage(StorageSpec(name=gcs_bucket))
    base = f"pagination/{uuid4().hex[:10]}/it"

    keys: list[str] = []
    for i, body in enumerate((b"aa", b"bb", b"cc")):
        up = await storage.upload(
            UploadedObject(filename=f"f{i}.txt", data=body, prefix=base),
        )
        keys.append(up.key)

    page_all, total_all = await storage.list(limit=50, offset=0, prefix=None)
    assert total_all == 3
    assert len(page_all) == 3
    assert {o.key for o in page_all} == set(keys)

    slices: list[str] = []
    for offset in range(3):
        page, _total = await storage.list(limit=1, offset=offset, prefix=base)
        assert len(page) == 1
        slices.append(page[0].key)

    assert len(set(slices)) == 3
    assert set(slices) == set(keys)

    for k in keys:
        await storage.delete(k)

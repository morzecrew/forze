from uuid import uuid4

import pytest

from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.execution.deps.module import GCSDepsModule
from forze_gcs.kernel.client.client import GCSClient
from tests.support.execution_context import context_from_deps


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_storage_adapter_upload_list_download_delete(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    ctx = context_from_deps(GCSDepsModule(
            client=gcs_client,
            storages={gcs_bucket: GCSStorageConfig(bucket=gcs_bucket)},
        )()
    )
    spec = StorageSpec(name=gcs_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    uploaded = await storage_c.upload(
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

    listed = await storage_q.list(limit=10, offset=0, prefix="inbox")
    assert listed.total == 1
    assert listed.container_missing is False
    assert len(listed.objects) == 1
    assert listed.objects[0].key == uploaded.key
    assert listed.objects[0].filename == "contract.txt"
    assert listed.objects[0].description == "integration test"

    downloaded = await storage_q.download(uploaded.key)
    assert downloaded.filename == "contract.txt"
    assert downloaded.data == b"forze-gcs-storage-adapter"

    await storage_c.delete(uploaded.key)
    after_delete = await storage_q.list(limit=10, offset=0, prefix="inbox")
    assert after_delete.total == 0
    assert after_delete.objects == []
    # Emptied, not absent — the bucket is still there.
    assert after_delete.container_missing is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_storage_list_pagination(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    ctx = context_from_deps(GCSDepsModule(
            client=gcs_client,
            storages={gcs_bucket: GCSStorageConfig(bucket=gcs_bucket)},
        )()
    )
    spec = StorageSpec(name=gcs_bucket)
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

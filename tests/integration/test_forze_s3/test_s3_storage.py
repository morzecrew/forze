import pytest

from forze.application.execution import ExecutionContext
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.platform.client import S3Client


@pytest.mark.asyncio
async def test_s3_storage_adapter_upload_list_download_delete(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = ExecutionContext(deps=S3DepsModule(client=s3_client)())
    storage = ctx.storage(s3_bucket)

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

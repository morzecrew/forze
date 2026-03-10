"""Performance tests for S3 storage adapter."""

import pytest

pytest.importorskip("aioboto3")

from forze.application.execution import ExecutionContext
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.platform.client import S3Client


@pytest.mark.perf
@pytest.mark.asyncio
async def test_storage_upload_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark storage adapter upload."""

    ctx = ExecutionContext(deps=S3DepsModule(client=s3_client)())
    storage = ctx.storage(s3_bucket)

    async def run() -> None:
        uploaded = await storage.upload(
            filename="bench.txt",
            data=b"forze-s3-storage-perf",
            description="perf",
            prefix="perf/uploads",
        )
        await storage.delete(uploaded["key"])

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_storage_list_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark storage adapter list."""
    ctx = ExecutionContext(deps=S3DepsModule(client=s3_client)())
    storage = ctx.storage(s3_bucket)

    uploaded = await storage.upload(
        filename="list-bench.txt",
        data=b"list-perf",
        description="perf",
        prefix="perf/list",
    )

    async def run() -> None:
        listed, total = await storage.list(limit=10, offset=0, prefix="perf")
        assert total >= 1

    await async_benchmark(run)

    await storage.delete(uploaded["key"])


@pytest.mark.perf
@pytest.mark.asyncio
async def test_storage_download_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark storage adapter download (object pre-seeded)."""
    ctx = ExecutionContext(deps=S3DepsModule(client=s3_client)())
    storage = ctx.storage(s3_bucket)

    uploaded = await storage.upload(
        filename="download-bench.txt",
        data=b"download-perf-payload",
        description="perf",
        prefix="perf/download",
    )

    async def run() -> None:
        downloaded = await storage.download(uploaded["key"])
        assert downloaded["data"] == b"download-perf-payload"

    await async_benchmark(run)

    await storage.delete(uploaded["key"])


@pytest.mark.perf
@pytest.mark.asyncio
async def test_storage_upload_list_download_delete_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark storage adapter full round-trip."""

    ctx = ExecutionContext(deps=S3DepsModule(client=s3_client)())
    storage = ctx.storage(s3_bucket)

    async def run() -> None:
        uploaded = await storage.upload(
            filename="roundtrip.txt",
            data=b"roundtrip-perf",
            description="perf",
            prefix="perf/roundtrip",
        )
        listed, total = await storage.list(limit=10, offset=0, prefix="perf")
        assert total >= 1
        downloaded = await storage.download(uploaded["key"])
        assert downloaded["data"] == b"roundtrip-perf"
        await storage.delete(uploaded["key"])

    await async_benchmark(run)

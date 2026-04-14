"""Performance tests for S3 storage adapter."""

import pytest

pytest.importorskip("aioboto3")

from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionContext
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.platform.client import S3Client


def _s3_ctx(client: S3Client, bucket: str) -> ExecutionContext:
    return ExecutionContext(
        deps=S3DepsModule(
            client=client,
            storages={bucket: {"bucket": bucket}},
        )()
    )


@pytest.mark.perf
@pytest.mark.asyncio
async def test_s3_storage_upload_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark storage adapter upload."""

    ctx = _s3_ctx(s3_client, s3_bucket)
    storage = ctx.storage(StorageSpec(name=s3_bucket))

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
async def test_s3_storage_list_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark storage adapter list."""
    ctx = _s3_ctx(s3_client, s3_bucket)
    storage = ctx.storage(StorageSpec(name=s3_bucket))

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
async def test_s3_storage_download_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark storage adapter download (object pre-seeded)."""
    ctx = _s3_ctx(s3_client, s3_bucket)
    storage = ctx.storage(StorageSpec(name=s3_bucket))

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
async def test_s3_storage_upload_list_download_delete_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark storage adapter full round-trip."""

    ctx = _s3_ctx(s3_client, s3_bucket)
    storage = ctx.storage(StorageSpec(name=s3_bucket))

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

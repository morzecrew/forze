"""Performance tests for S3Client."""

from uuid import uuid4

import pytest

pytest.importorskip("aioboto3")

from forze_s3.kernel.platform.client import S3Client


def _perf_key(prefix: str) -> str:
    return f"perf/{prefix}/{uuid4().hex[:12]}"


@pytest.mark.perf
@pytest.mark.asyncio
async def test_s3_client_context_benchmark(async_benchmark, s3_client: S3Client) -> None:
    """Benchmark client context manager (open/close)."""

    async def run() -> None:
        async with s3_client.client():
            pass

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_s3_upload_bytes_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark single object upload."""

    async def run() -> None:
        key = _perf_key("upload")
        async with s3_client.client():
            await s3_client.upload_bytes(
                bucket=s3_bucket,
                key=key,
                data=b"x" * 1024,
                content_type="application/octet-stream",
            )
            await s3_client.delete_object(s3_bucket, key)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_s3_download_bytes_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark single object download (object pre-seeded)."""
    key = _perf_key("download")
    data = b"y" * 2048

    async with s3_client.client():
        await s3_client.upload_bytes(
            bucket=s3_bucket,
            key=key,
            data=data,
            content_type="application/octet-stream",
        )

    async def run() -> None:
        async with s3_client.client():
            result = await s3_client.download_bytes(s3_bucket, key)
            assert result == data

    await async_benchmark(run)

    async with s3_client.client():
        await s3_client.delete_object(s3_bucket, key)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_s3_head_object_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark head_object (metadata fetch)."""
    key = _perf_key("head")

    async with s3_client.client():
        await s3_client.upload_bytes(
            bucket=s3_bucket,
            key=key,
            data=b"z" * 512,
            content_type="text/plain",
            metadata={"filename": "bench.txt"},
        )

    async def run() -> None:
        async with s3_client.client():
            head = await s3_client.head_object(s3_bucket, key)
            assert head["size"] == 512

    await async_benchmark(run)

    async with s3_client.client():
        await s3_client.delete_object(s3_bucket, key)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_s3_list_objects_small_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark list_objects with a small prefix (few objects)."""
    prefix = f"perf/list/{uuid4().hex[:8]}"

    async with s3_client.client():
        for i in range(5):
            await s3_client.upload_bytes(
                bucket=s3_bucket,
                key=f"{prefix}/obj{i}.txt",
                data=b"list",
                content_type="text/plain",
            )

    async def run() -> None:
        async with s3_client.client():
            items, total = await s3_client.list_objects(
                bucket=s3_bucket,
                prefix=prefix,
                limit=10,
                offset=0,
            )
            assert total == 5

    await async_benchmark(run)

    async with s3_client.client():
        for i in range(5):
            await s3_client.delete_object(s3_bucket, f"{prefix}/obj{i}.txt")


@pytest.mark.perf
@pytest.mark.asyncio
async def test_s3_upload_download_roundtrip_benchmark(
    async_benchmark, s3_client: S3Client, s3_bucket: str
) -> None:
    """Benchmark full round-trip: upload, download, delete."""

    async def run() -> None:
        key = _perf_key("roundtrip")
        data = b"roundtrip-payload"
        async with s3_client.client():
            await s3_client.upload_bytes(
                bucket=s3_bucket,
                key=key,
                data=data,
                content_type="application/octet-stream",
            )
            downloaded = await s3_client.download_bytes(s3_bucket, key)
            assert downloaded == data
            await s3_client.delete_object(s3_bucket, key)

    await async_benchmark(run)

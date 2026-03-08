import pytest

from forze_s3.kernel.platform.client import S3Client


@pytest.mark.asyncio
async def test_s3_client_bucket_and_object_crud(
    s3_client: S3Client, s3_bucket: str
) -> None:
    key = "docs/readme.txt"
    data = b"hello from forze s3 integration tests"
    metadata = {
        "filename": "readme.txt",
        "created_at": "2026-01-01",
        "size": str(len(data)),
    }

    async with s3_client.client():
        assert await s3_client.bucket_exists(s3_bucket)
        await s3_client.ensure_bucket(s3_bucket)

        await s3_client.upload_bytes(
            bucket=s3_bucket,
            key=key,
            data=data,
            content_type="text/plain",
            metadata=metadata,
            tags={"kind": "integration"},
        )

        assert await s3_client.object_exists(s3_bucket, key)

        head = await s3_client.head_object(s3_bucket, key)
        assert head["content_type"] == "text/plain"
        assert head["metadata"]["filename"] == "readme.txt"
        assert head["size"] == len(data)

        downloaded = await s3_client.download_bytes(s3_bucket, key)
        assert downloaded == data

        items, total_count = await s3_client.list_objects(
            bucket=s3_bucket,
            prefix="docs",
            limit=10,
            offset=0,
        )
        assert total_count == 1
        assert len(items) == 1
        assert items[0]["Key"] == key

        await s3_client.delete_object(s3_bucket, key)
        assert not await s3_client.object_exists(s3_bucket, key)

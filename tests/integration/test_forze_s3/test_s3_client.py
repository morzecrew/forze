from uuid import uuid4

import pytest

from forze_s3.kernel.client import S3Client


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
        assert head.content_type == "text/plain"
        assert head.metadata["filename"] == "readme.txt"
        assert head.size == len(data)

        downloaded = await s3_client.download_bytes(s3_bucket, key)
        assert downloaded.data == data

        items, total_count = await s3_client.list_objects(
            bucket=s3_bucket,
            prefix="docs",
            limit=10,
            offset=0,
        )
        assert total_count == 1
        assert len(items) == 1
        assert items[0].key == key

        await s3_client.delete_object(s3_bucket, key)
        assert not await s3_client.object_exists(s3_bucket, key)


@pytest.mark.asyncio
async def test_s3_ensure_bucket_creates_missing_bucket_and_is_idempotent(
    s3_client: S3Client,
) -> None:
    bucket = f"forze-s3-ensure-{uuid4().hex[:16]}"

    async with s3_client.client():
        assert not await s3_client.bucket_exists(bucket)

        await s3_client.ensure_bucket(bucket)
        assert await s3_client.bucket_exists(bucket)

        # Second call is a no-op on an existing bucket.
        await s3_client.ensure_bucket(bucket)
        assert await s3_client.bucket_exists(bucket)


@pytest.mark.asyncio
async def test_sequential_operations_reuse_single_aiobotocore_client(
    minio_container,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``initialize`` opens one aiobotocore client; sequential ops reuse it.

    MinIO requires explicit static credentials, so the credential-chain
    (``access_key_id=None``) path is exercised by unit tests only.
    """
    import aioboto3

    from forze_s3.kernel.client import S3Config

    _container, endpoint = minio_container

    create_calls = 0
    original_client = aioboto3.Session.client

    def _counting_client(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        nonlocal create_calls
        create_calls += 1
        return original_client(self, *args, **kwargs)

    monkeypatch.setattr(aioboto3.Session, "client", _counting_client)

    client = S3Client()
    await client.initialize(
        endpoint=endpoint,
        access_key_id="minioadmin",  # MinIO root creds from conftest fixture
        secret_access_key="minioadmin",
        config=S3Config(s3={"addressing_style": "path"}),
    )

    try:
        bucket = f"forze-s3-reuse-{uuid4().hex[:16]}"

        async with client.client():
            await client.create_bucket(bucket)

        async with client.client():
            await client.upload_bytes(bucket, "reuse/key.txt", b"payload")

        async with client.client():
            assert (await client.download_bytes(bucket, "reuse/key.txt")).data == b"payload"

        assert create_calls == 1

    finally:
        await client.close()


@pytest.mark.asyncio
async def test_s3_include_tags_guarantee_on_head_and_list(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """MinIO supports the tagging API: ``include_tags=True`` round-trips tags."""

    tags_by_key = {
        "tagged/a.txt": {"env": "dev", "team": "core"},
        "tagged/b.txt": {"env": "prod"},
    }

    async with s3_client.client():
        for key, tags in tags_by_key.items():
            await s3_client.upload_bytes(
                bucket=s3_bucket,
                key=key,
                data=b"payload",
                content_type="text/plain",
                tags=tags,
            )

        # head: False -> tags may be absent (S3 heads never carry them)
        head_plain = await s3_client.head_object(s3_bucket, "tagged/a.txt")
        assert dict(head_plain.tags) == {}

        # head: True -> guaranteed populated via GetObjectTagging
        head_tagged = await s3_client.head_object(
            s3_bucket,
            "tagged/a.txt",
            include_tags=True,
        )
        assert dict(head_tagged.tags) == tags_by_key["tagged/a.txt"]

        # list: False -> no tags on listed objects
        items_plain, _ = await s3_client.list_objects(
            bucket=s3_bucket,
            prefix="tagged/",
        )
        assert all(dict(item.tags) == {} for item in items_plain)

        # list: True -> per-object tags fanned out via GetObjectTagging
        items_tagged, total = await s3_client.list_objects(
            bucket=s3_bucket,
            prefix="tagged/",
            include_tags=True,
        )
        assert total == len(tags_by_key)
        assert {item.key: dict(item.tags) for item in items_tagged} == tags_by_key

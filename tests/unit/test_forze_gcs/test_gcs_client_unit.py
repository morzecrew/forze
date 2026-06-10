import os
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives.owned_temp_path import OwnedTempPath

from forze_gcs.kernel.client.client import TAG_METADATA_PREFIX, GCSClient
from forze_gcs.kernel.client.value_objects import GCSConfig


@pytest.mark.asyncio
async def test_initialize_creates_storage_client() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.close = AsyncMock()

    with patch(
        "forze_gcs.kernel.client.client.Storage",
        return_value=fake_storage,
    ) as storage_ctor:
        with patch.dict(
            os.environ,
            {"STORAGE_EMULATOR_HOST": "http://localhost:4443"},
        ):
            await client.initialize("test-project")

    storage_ctor.assert_called_once_with(
        service_file=None,
        api_root="http://localhost:4443",
    )
    assert client._GCSClient__storage is fake_storage
    assert client._GCSClient__project_id == "test-project"


@pytest.mark.asyncio
async def test_initialize_uses_service_file_from_config() -> None:
    client = GCSClient()
    fake_storage = MagicMock()

    with patch(
        "forze_gcs.kernel.client.client.Storage",
        return_value=fake_storage,
    ) as storage_ctor:
        with patch.dict(
            os.environ,
            {"STORAGE_EMULATOR_HOST": ""},
        ):
            await client.initialize(
                project_id="test-project",
                config=GCSConfig(
                    service_file="/keys/sa.json",
                    timeout=timedelta(seconds=60),
                ),
            )

    storage_ctor.assert_called_once_with(
        api_root=None,
        service_file="/keys/sa.json",
    )


@pytest.mark.asyncio
async def test_list_objects_applies_offset_and_limit() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    bucket_ref = MagicMock()
    bucket_ref.list_blobs = AsyncMock(return_value=["a", "b", "c", "d"])
    fake_storage.get_bucket.return_value = bucket_ref
    client._GCSClient__storage = fake_storage

    items, total_count = await client.list_objects(
        bucket="bucket",
        prefix="",
        limit=2,
        offset=1,
    )

    assert [item.key for item in items] == ["b", "c"]
    assert total_count == 4
    bucket_ref.list_blobs.assert_awaited_once_with(prefix="")


@pytest.mark.asyncio
async def test_list_objects_rejects_invalid_limit() -> None:
    client = GCSClient()
    client._GCSClient__storage = MagicMock()

    with pytest.raises(CoreException, match="limit must be > 0"):
        await client.list_objects(bucket="b", limit=0)


@pytest.mark.asyncio
async def test_ensure_bucket_creates_when_missing() -> None:
    client = GCSClient()
    client._GCSClient__storage = MagicMock()

    with (
        patch.object(
            client, "bucket_exists", new_callable=AsyncMock, return_value=False
        ),
        patch.object(client, "create_bucket", new_callable=AsyncMock) as create_mock,
    ):
        await client.ensure_bucket("new-bucket")
        create_mock.assert_awaited_once_with("new-bucket")


@pytest.mark.asyncio
async def test_client_context_manager_nested_depth() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    client._GCSClient__storage = fake_storage

    async with client.client() as c:
        assert c is fake_storage
        async with client.client() as c2:
            assert c2 is fake_storage


@pytest.mark.asyncio
async def test_upload_bytes_passes_nested_metadata() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.upload = AsyncMock()
    client._GCSClient__storage = fake_storage

    await client.upload_bytes(
        "bucket",
        "key",
        b"data",
        content_type="text/plain",
        metadata={"filename": "x"},
    )

    fake_storage.upload.assert_awaited_once_with(
        "bucket",
        "key",
        b"data",
        content_type="text/plain",
        metadata={"metadata": {"filename": "x"}},
        timeout=30,
    )


@pytest.mark.asyncio
async def test_upload_bytes_namespaces_tags_into_custom_metadata() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.upload = AsyncMock()
    client._GCSClient__storage = fake_storage

    await client.upload_bytes(
        "bucket",
        "key",
        b"data",
        content_type="text/plain",
        metadata={"filename": "x"},
        tags={"env": "dev", "team": "core"},
    )

    fake_storage.upload.assert_awaited_once_with(
        "bucket",
        "key",
        b"data",
        content_type="text/plain",
        metadata={
            "metadata": {
                "filename": "x",
                f"{TAG_METADATA_PREFIX}env": "dev",
                f"{TAG_METADATA_PREFIX}team": "core",
            }
        },
        timeout=30,
    )


@pytest.mark.asyncio
async def test_upload_bytes_tags_without_metadata() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.upload = AsyncMock()
    client._GCSClient__storage = fake_storage

    await client.upload_bytes("bucket", "key", b"data", tags={"env": "dev"})

    fake_storage.upload.assert_awaited_once_with(
        "bucket",
        "key",
        b"data",
        content_type=None,
        metadata={"metadata": {f"{TAG_METADATA_PREFIX}env": "dev"}},
        timeout=30,
    )


@pytest.mark.asyncio
async def test_head_object_splits_tags_from_custom_metadata() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.download_metadata = AsyncMock(
        return_value={
            "contentType": "text/plain",
            "metadata": {
                "filename": "Zm9v",
                f"{TAG_METADATA_PREFIX}env": "dev",
            },
            "size": 3,
        },
    )
    client._GCSClient__storage = fake_storage

    head = await client.head_object("bucket", "key")

    assert head.metadata == {"filename": "Zm9v"}
    assert head.tags == {"env": "dev"}


@pytest.mark.asyncio
async def test_head_object_maps_download_metadata() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.download_metadata = AsyncMock(
        return_value={
            "contentType": "text/plain",
            "metadata": {"filename": "Zm9v"},
            "size": 42,
            "updated": "2025-01-15T12:00:00Z",
            "etag": '"abc"',
        },
    )
    client._GCSClient__storage = fake_storage

    head = await client.head_object("bucket", "key")

    assert head.content_type == "text/plain"
    assert head.metadata == {"filename": "Zm9v"}
    assert head.size == 42
    assert head.etag == "abc"


@pytest.mark.asyncio
async def test_close_unlinks_owned_service_file() -> None:
    sa_json = '{"type":"service_account","project_id":"p"}'
    credential_path = OwnedTempPath.materialize_text(sa_json, prefix="forze-gcs-test-")
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.close = AsyncMock()

    with patch(
        "forze_gcs.kernel.client.client.Storage",
        return_value=fake_storage,
    ):
        await client.initialize(
            "test-project",
            service_file=credential_path.path,
            service_file_owned=credential_path.owned,
        )
        await client.close()

    assert credential_path.path is not None
    assert not Path(credential_path.path).exists()

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
    ) as storage_ctor, patch.dict(
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
    ) as storage_ctor, patch.dict(
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


def _page(names: list[str], next_token: str = "") -> dict[str, object]:
    return {"items": [{"name": n} for n in names], "nextPageToken": next_token}


@pytest.mark.asyncio
async def test_list_objects_applies_offset_and_limit() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.list_objects = AsyncMock(return_value=_page(["a", "b", "c", "d"]))
    client._GCSClient__storage = fake_storage

    items, total_count = await client.list_objects(
        bucket="bucket",
        prefix="",
        limit=2,
        offset=1,
    )

    assert [item.key for item in items] == ["b", "c"]
    assert total_count == 4


@pytest.mark.asyncio
async def test_list_objects_streams_pages_and_windows_without_buffering_all() -> None:
    client = GCSClient()
    fake_storage = MagicMock()
    # Three pages; the window (offset 2, limit 3) spans the page boundary, and the
    # exact total counts every key across all pages.
    fake_storage.list_objects = AsyncMock(
        side_effect=[
            _page(["a", "b", "c"], next_token="t1"),
            _page(["d", "e", "f"], next_token="t2"),
            _page(["g", "h"], next_token=""),
        ]
    )
    client._GCSClient__storage = fake_storage

    items, total_count = await client.list_objects(
        bucket="bucket",
        prefix="docs/",
        limit=3,
        offset=2,
    )

    assert [item.key for item in items] == ["c", "d", "e"]
    assert total_count == 8
    assert fake_storage.list_objects.await_count == 3


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
async def test_upload_bytes_rejects_reserved_tag_prefix_in_metadata() -> None:
    # A user metadata key in the reserved tag namespace would be misread as a
    # tag on read-back, so it must be rejected at write time.
    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.upload = AsyncMock()
    client._GCSClient__storage = fake_storage

    with pytest.raises(CoreException, match="reserved tag prefix"):
        await client.upload_bytes(
            "bucket",
            "key",
            b"data",
            metadata={f"{TAG_METADATA_PREFIX}env": "smuggled"},
        )

    fake_storage.upload.assert_not_awaited()


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


@pytest.mark.asyncio
async def test_list_objects_include_tags_is_a_free_no_op() -> None:
    """GCS tags ride on head metadata; ``include_tags`` adds no extra calls."""

    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.list_objects = AsyncMock(return_value=_page(["a", "b"]))
    client._GCSClient__storage = fake_storage

    without_flag = await client.list_objects(bucket="bucket", prefix="")
    with_flag = await client.list_objects(bucket="bucket", prefix="", include_tags=True)

    assert with_flag == without_flag
    # One list call per invocation -- the flag triggered no extra requests.
    assert fake_storage.list_objects.await_count == 2


@pytest.mark.asyncio
async def test_head_object_include_tags_is_a_free_no_op() -> None:
    """Tags are already round-tripped from custom metadata regardless of the flag."""

    client = GCSClient()
    fake_storage = MagicMock()
    fake_storage.download_metadata = AsyncMock(
        return_value={
            "contentType": "text/plain",
            "metadata": {
                "plain": "value",
                f"{TAG_METADATA_PREFIX}env": "dev",
            },
            "size": "3",
        }
    )
    client._GCSClient__storage = fake_storage

    without_flag = await client.head_object("bucket", "key")
    with_flag = await client.head_object("bucket", "key", include_tags=True)

    assert with_flag == without_flag
    assert dict(with_flag.tags) == {"env": "dev"}
    assert fake_storage.download_metadata.await_count == 2

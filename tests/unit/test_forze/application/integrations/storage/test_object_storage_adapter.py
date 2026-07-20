"""Unit tests for :class:`~forze.application.integrations.storage.ObjectStorageAdapter`."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.storage import UploadedObject
from forze.application.integrations.storage import (
    ObjectStorageAdapter,
    ObjectStorageHead,
    ObjectStorageListedObject,
)
from forze.base.exceptions import CoreException


async def _resolve_static_bucket(_spec: str, _tenant_id: UUID | None) -> str:
    return "test-bucket"


@pytest.fixture
def storage_adapter() -> ObjectStorageAdapter:
    client = MagicMock()
    return ObjectStorageAdapter(
        client=client,
        bucket_spec="test-bucket",
        resolve_bucket=_resolve_static_bucket,
    )


def test_validate_prefix_valid(storage_adapter: ObjectStorageAdapter) -> None:
    storage_adapter._validate_prefix(None)
    storage_adapter._validate_prefix("")
    storage_adapter._validate_prefix("validPrefix")
    storage_adapter._validate_prefix("prefix/with/slash")
    storage_adapter._validate_prefix("prefix-with-dash_and.dot")
    storage_adapter._validate_prefix("!-_.*'()/")


def test_validate_prefix_invalid(storage_adapter: ObjectStorageAdapter) -> None:
    invalid_prefixes = [
        "prefix with space",
        "prefix@invalid",
        "prefix#invalid",
    ]
    for prefix in invalid_prefixes:
        with pytest.raises(CoreException) as excinfo:
            storage_adapter._validate_prefix(prefix)
        assert f"Invalid object storage prefix: {prefix}" in str(excinfo.value)


def test_validate_key_accepts_minted_keys(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    # Keys this adapter produces (validated prefix + generated id) pass.
    storage_adapter._validate_key("018f-uuid7-id")
    storage_adapter._validate_key("tenant-abc/prefix/018f-uuid7-id")
    storage_adapter._validate_key(storage_adapter.construct_key("docs"))


def test_validate_key_rejects_unsafe_keys(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    for bad in (
        "",
        "../etc/passwd",
        "a/../../b",
        "/absolute/key",
        "key with space",
        "key@bad",
        "ctrl\nchar",
    ):
        with pytest.raises(CoreException):
            storage_adapter._validate_key(bad)


@pytest.mark.asyncio
async def test_delete_rejects_traversal_key(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    storage_adapter.client.delete_object = AsyncMock()

    with pytest.raises(CoreException):
        await storage_adapter.delete("../../secret")

    storage_adapter.client.delete_object.assert_not_called()


@pytest.mark.asyncio
async def test_download_rejects_traversal_key(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    storage_adapter.client.head_object = AsyncMock()

    with pytest.raises(CoreException):
        await storage_adapter.download("../../secret")

    storage_adapter.client.head_object.assert_not_called()


@pytest.mark.asyncio
async def test_upload_invalid_prefix_raises(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    with pytest.raises(CoreException):
        await storage_adapter.upload(
            UploadedObject(filename="file.txt", data=b"data", prefix="invalid prefix"),
        )


@pytest.mark.asyncio
async def test_list_invalid_prefix_raises(storage_adapter: ObjectStorageAdapter) -> None:
    with pytest.raises(CoreException):
        await storage_adapter.list(10, 0, prefix="invalid prefix")


@pytest.mark.asyncio
async def test_upload_valid_prefix_passes_validation(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.upload_bytes = AsyncMock()

    await storage_adapter.upload(
        UploadedObject(filename="file.txt", data=b"data", prefix="valid/prefix"),
    )


@pytest.mark.asyncio
async def test_list_valid_prefix_passes_validation(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.list_objects = AsyncMock(return_value=([], 0))

    await storage_adapter.list(10, 0, prefix="valid/prefix")


@pytest.mark.asyncio
async def test_list_enriches_objects_from_head_metadata(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.list_objects = AsyncMock(
        return_value=([ObjectStorageListedObject(key="docs/k1")], 1),
    )
    storage_adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(
            content_type="text/plain",
            metadata={
                "filename": "Zm9v.txt",
                "size": "3",
                "created_at": "2025-01-15T12:00:00+00:00",
            },
        ),
    )

    objects, total = await storage_adapter.list(10, 0, prefix="docs")

    assert total == 1
    assert len(objects) == 1
    assert objects[0].key == "docs/k1"
    assert objects[0].content_type == "text/plain"
    assert objects[0].tags is None


@pytest.mark.asyncio
async def test_list_falls_back_for_envelope_less_objects(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    """Raw objects (presigned PUT / completed multipart) carry no envelope, so
    ``list`` must not raise — it falls back to the key basename + head fields,
    mirroring ``download``."""

    modified = datetime(2025, 1, 15, 12, 0, tzinfo=UTC)
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.list_objects = AsyncMock(
        return_value=([ObjectStorageListedObject(key="docs/raw.bin")], 1),
    )
    storage_adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(
            content_type="application/octet-stream",
            metadata={},  # no envelope
            size=42,
            last_modified=modified,
        ),
    )

    objects, total = await storage_adapter.list(10, 0, prefix="docs")

    assert total == 1
    assert objects[0].key == "docs/raw.bin"
    assert objects[0].filename == "raw.bin"
    assert objects[0].description is None
    assert objects[0].size == 42
    assert objects[0].created_at == modified


@pytest.mark.asyncio
async def test_upload_passes_tags_to_client_and_returns_them(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.upload_bytes = AsyncMock()

    stored = await storage_adapter.upload(
        UploadedObject(filename="file.txt", data=b"data", tags={"env": "dev"}),
    )

    kwargs = storage_adapter.client.upload_bytes.await_args.kwargs
    assert kwargs["tags"] == {"env": "dev"}
    assert stored.tags == {"env": "dev"}


@pytest.mark.asyncio
async def test_list_surfaces_tags_from_head(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.list_objects = AsyncMock(
        return_value=([ObjectStorageListedObject(key="docs/k1")], 1),
    )
    storage_adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(
            content_type="text/plain",
            metadata={
                "filename": "Zm9v.txt",
                "size": "3",
                "created_at": "2025-01-15T12:00:00+00:00",
            },
            tags={"env": "dev"},
        ),
    )

    objects, _ = await storage_adapter.list(10, 0, prefix="docs")

    assert objects[0].tags == {"env": "dev"}


@pytest.mark.asyncio
async def test_list_threads_include_tags_to_client(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.list_objects = AsyncMock(return_value=([], 0))

    await storage_adapter.list(10, 0, include_tags=True)

    kwargs = storage_adapter.client.list_objects.await_args.kwargs
    assert kwargs["include_tags"] is True


@pytest.mark.asyncio
async def test_list_prefers_listed_object_tags_over_head_tags(
    storage_adapter: ObjectStorageAdapter,
) -> None:
    """S3-style: tags ride on the listed object when ``include_tags=True``."""

    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.list_objects = AsyncMock(
        return_value=(
            [ObjectStorageListedObject(key="docs/k1", tags={"env": "dev"})],
            1,
        ),
    )
    storage_adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(
            content_type="text/plain",
            metadata={
                "filename": "Zm9v.txt",
                "size": "3",
                "created_at": "2025-01-15T12:00:00+00:00",
            },
        ),
    )

    objects, _ = await storage_adapter.list(10, 0, include_tags=True)

    assert objects[0].tags == {"env": "dev"}

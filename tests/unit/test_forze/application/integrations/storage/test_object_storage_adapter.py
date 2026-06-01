"""Unit tests for :class:`~forze.application.integrations.storage.ObjectStorageAdapter`."""

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

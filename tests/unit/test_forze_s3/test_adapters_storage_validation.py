import unittest.mock
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.errors import ValidationError
from forze_s3.adapters.storage import S3StorageAdapter

@pytest.fixture
def storage_adapter():
    client = MagicMock()
    return S3StorageAdapter(client=client, bucket="test-bucket")

def test_validate_prefix_valid(storage_adapter):
    # Should not raise any exception
    storage_adapter._validate_prefix(None)
    storage_adapter._validate_prefix("")
    storage_adapter._validate_prefix("validPrefix")
    storage_adapter._validate_prefix("prefix/with/slash")
    storage_adapter._validate_prefix("prefix-with-dash_and.dot")
    storage_adapter._validate_prefix("!-_.*'()/")

def test_validate_prefix_invalid(storage_adapter):
    invalid_prefixes = [
        "prefix with space",
        "prefix@invalid",
        "prefix#invalid",
        "prefix$invalid",
        "prefix%invalid",
        "prefix^invalid",
        "prefix&invalid",
        "prefix+invalid",
        "prefix=invalid",
        "prefix[invalid",
        "prefix]invalid",
        "prefix{invalid",
        "prefix}invalid",
        "prefix|invalid",
        "prefix\\invalid",
        "prefix\"invalid",
        "prefix<invalid",
        "prefix>invalid",
        "prefix?invalid",
        "prefix`invalid",
    ]
    for prefix in invalid_prefixes:
        with pytest.raises(ValidationError) as excinfo:
            storage_adapter._validate_prefix(prefix)
        assert f"Invalid S3 prefix: {prefix}" in str(excinfo.value)

@pytest.mark.asyncio
async def test_upload_invalid_prefix_raises(storage_adapter):
    with pytest.raises(ValidationError):
        await storage_adapter.upload("file.txt", b"data", prefix="invalid prefix")

@pytest.mark.asyncio
async def test_list_invalid_prefix_raises(storage_adapter):
    with pytest.raises(ValidationError):
        await storage_adapter.list(10, 0, prefix="invalid prefix")

@pytest.mark.asyncio
async def test_upload_valid_prefix_passes_validation(storage_adapter):
    # Mocking dependencies to ensure it proceeds past validation
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.upload_bytes = AsyncMock()

    # Should not raise ValidationError
    await storage_adapter.upload("file.txt", b"data", prefix="valid/prefix")

@pytest.mark.asyncio
async def test_list_valid_prefix_passes_validation(storage_adapter):
    # Mocking dependencies to ensure it proceeds past validation
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.list_objects = AsyncMock(return_value=([], 0))

    # Should not raise ValidationError
    await storage_adapter.list(10, 0, prefix="valid/prefix")

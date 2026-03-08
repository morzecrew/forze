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
async def test_upload_calls_validation(storage_adapter):
    with unittest.mock.patch.object(S3StorageAdapter, "_validate_prefix") as mock_validate:
        # Mocking dependencies for upload
        storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
        storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
        storage_adapter.client.ensure_bucket = AsyncMock()
        storage_adapter.client.upload_bytes = AsyncMock()

        await storage_adapter.upload("file.txt", b"data", prefix="some/prefix")
        mock_validate.assert_called_once_with(storage_adapter, "some/prefix")

@pytest.mark.asyncio
async def test_list_calls_validation(storage_adapter):
    with unittest.mock.patch.object(S3StorageAdapter, "_validate_prefix") as mock_validate:
        # Mocking dependencies for list
        storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
        storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
        storage_adapter.client.ensure_bucket = AsyncMock()
        storage_adapter.client.list_objects = AsyncMock(return_value=([], 0))

        await storage_adapter.list(10, 0, prefix="some/prefix")
        mock_validate.assert_called_once_with(storage_adapter, "some/prefix")

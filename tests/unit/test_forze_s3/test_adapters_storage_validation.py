import pytest
from unittest.mock import MagicMock
from forze_s3.adapters.storage import S3StorageAdapter
from forze.base.errors import ValidationError

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
    storage_adapter._validate_prefix = MagicMock()

    # Mocking dependencies for upload
    storage_adapter.client.client.return_value.__aenter__.return_value = MagicMock()
    storage_adapter.client.ensure_bucket = MagicMock()
    storage_adapter.client.upload_bytes = MagicMock()

    await storage_adapter.upload("file.txt", b"data", prefix="some/prefix")
    storage_adapter._validate_prefix.assert_called_once_with("some/prefix")

@pytest.mark.asyncio
async def test_list_calls_validation(storage_adapter):
    storage_adapter._validate_prefix = MagicMock()

    # Mocking dependencies for list
    storage_adapter.client.client.return_value.__aenter__.return_value = MagicMock()
    storage_adapter.client.ensure_bucket = MagicMock()
    storage_adapter.client.list_objects.return_value = ([], 0)

    await storage_adapter.list(10, 0, prefix="some/prefix")
    storage_adapter._validate_prefix.assert_called_once_with("some/prefix")

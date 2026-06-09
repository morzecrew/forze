"""Smoke tests for S3 storage adapter wiring."""

from forze.application.integrations.storage import ObjectStorageAdapter
from forze_s3.adapters.storage import S3StorageAdapter


def test_s3_storage_adapter_subclasses_object_storage_adapter() -> None:
    assert issubclass(S3StorageAdapter, ObjectStorageAdapter)

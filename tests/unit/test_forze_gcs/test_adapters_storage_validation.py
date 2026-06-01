"""Smoke tests for GCS storage adapter wiring."""

from forze.application.integrations.storage import ObjectStorageAdapter
from forze_gcs.adapters.storage import GCSStorageAdapter


def test_gcs_storage_adapter_subclasses_object_storage_adapter() -> None:
    assert issubclass(GCSStorageAdapter, ObjectStorageAdapter)

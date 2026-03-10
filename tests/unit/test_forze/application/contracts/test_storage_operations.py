"""Tests for forze.application.composition.storage.operations."""

from forze.application.composition.storage.operations import StorageOperation


class TestStorageOperation:
    def test_upload_value(self) -> None:
        assert StorageOperation.UPLOAD == "storage.upload"

    def test_list_value(self) -> None:
        assert StorageOperation.LIST == "storage.list"

    def test_download_value(self) -> None:
        assert StorageOperation.DOWNLOAD == "storage.download"

    def test_delete_value(self) -> None:
        assert StorageOperation.DELETE == "storage.delete"

    def test_is_str_enum(self) -> None:
        assert isinstance(StorageOperation.UPLOAD, str)

    def test_all_members(self) -> None:
        members = set(StorageOperation)
        assert len(members) == 4

    def test_str_comparison(self) -> None:
        assert StorageOperation.UPLOAD == "storage.upload"
        assert StorageOperation.LIST != "storage.upload"

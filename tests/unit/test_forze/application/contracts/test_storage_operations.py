"""Tests for forze.application.composition.storage.operations."""

from forze.application.composition.storage.operations import StorageKernelOp
from forze.base.primitives import StrKeyNamespace

_STORAGE_KEYS = StrKeyNamespace(prefix="storage")


class TestStorageKernelOp:
    def test_upload_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.UPLOAD) == "upload"
        assert _STORAGE_KEYS.key(StorageKernelOp.UPLOAD) == "storage.upload"

    def test_list_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.LIST) == "list"
        assert _STORAGE_KEYS.key(StorageKernelOp.LIST) == "storage.list"

    def test_download_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.DOWNLOAD) == "download"
        assert _STORAGE_KEYS.key(StorageKernelOp.DOWNLOAD) == "storage.download"

    def test_delete_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.DELETE) == "delete"
        assert _STORAGE_KEYS.key(StorageKernelOp.DELETE) == "storage.delete"

    def test_is_str_enum(self) -> None:
        assert isinstance(StorageKernelOp.UPLOAD, str)

    def test_all_members(self) -> None:
        members = set(StorageKernelOp)
        assert len(members) == 4

    def test_str_comparison(self) -> None:
        assert _STORAGE_KEYS.key(StorageKernelOp.UPLOAD) == "storage.upload"
        assert _STORAGE_KEYS.key(StorageKernelOp.LIST) != "storage.upload"

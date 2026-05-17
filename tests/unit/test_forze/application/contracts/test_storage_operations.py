"""Tests for forze.application.composition.storage.operations."""

from forze.application.execution import OperationNamespace
from forze.application.composition.storage.operations import StorageKernelOp

_STORAGE_KEYS = OperationNamespace(prefix="storage")


class TestStorageKernelOp:
    def test_upload_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.UPLOAD) == "upload"
        assert _STORAGE_KEYS.op(StorageKernelOp.UPLOAD) == "storage.upload"

    def test_list_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.LIST) == "list"
        assert _STORAGE_KEYS.op(StorageKernelOp.LIST) == "storage.list"

    def test_download_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.DOWNLOAD) == "download"
        assert _STORAGE_KEYS.op(StorageKernelOp.DOWNLOAD) == "storage.download"

    def test_delete_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.DELETE) == "delete"
        assert _STORAGE_KEYS.op(StorageKernelOp.DELETE) == "storage.delete"

    def test_is_str_enum(self) -> None:
        assert isinstance(StorageKernelOp.UPLOAD, str)

    def test_all_members(self) -> None:
        members = set(StorageKernelOp)
        assert len(members) == 4

    def test_str_comparison(self) -> None:
        assert _STORAGE_KEYS.op(StorageKernelOp.UPLOAD) == "storage.upload"
        assert _STORAGE_KEYS.op(StorageKernelOp.LIST) != "storage.upload"

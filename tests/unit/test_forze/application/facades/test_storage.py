"""Unit tests for StorageKernelOp and StorageUsecasesFacade."""

import pytest

from forze.application.composition.storage import (
    StorageKernelOp,
    StorageUsecasesFacade,
)
from forze.application.execution import ExecutionContext, OperationNamespace, UsecaseRegistry

# ----------------------- #

_STORAGE_KEYS = OperationNamespace(prefix="storage")


class TestStorageKernelOp:
    """Tests for :class:`StorageKernelOp` and default keyspace."""

    def test_upload_wire_key(self) -> None:
        assert _STORAGE_KEYS.op(StorageKernelOp.UPLOAD) == "storage.upload"

    def test_list_wire_key(self) -> None:
        assert _STORAGE_KEYS.op(StorageKernelOp.LIST) == "storage.list"

    def test_download_wire_key(self) -> None:
        assert _STORAGE_KEYS.op(StorageKernelOp.DOWNLOAD) == "storage.download"

    def test_delete_wire_key(self) -> None:
        assert _STORAGE_KEYS.op(StorageKernelOp.DELETE) == "storage.delete"


class TestStorageUsecasesFacade:
    """Tests for StorageUsecasesFacade."""

    @pytest.fixture
    def mock_upload_usecase(self) -> UsecaseRegistry:
        """Registry with UPLOAD operation only."""
        from forze.application.execution import Usecase

        class StubUploadUsecase(Usecase[dict, dict]):
            async def main(self, args: dict) -> dict:
                return {"ok": True}

        reg = UsecaseRegistry().register(
            _STORAGE_KEYS.op(StorageKernelOp.UPLOAD),
            lambda ctx: StubUploadUsecase(ctx=ctx),
        )
        reg.finalize("storage_facade")
        return reg

    def test_upload_descriptor_resolves_usecase(
        self,
        stub_ctx: ExecutionContext,
        mock_upload_usecase: UsecaseRegistry,
    ) -> None:
        facade = StorageUsecasesFacade(
            ctx=stub_ctx,
            registry=mock_upload_usecase,
            namespace=_STORAGE_KEYS,
        )
        uc = facade.upload

        assert uc is not None

    def test_download_raises_when_not_registered(
        self,
        stub_ctx: ExecutionContext,
        mock_upload_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = StorageUsecasesFacade(
            ctx=stub_ctx,
            registry=mock_upload_usecase,
            namespace=_STORAGE_KEYS,
        )

        with pytest.raises(
            CoreError,
            match="not registered for operation: storage.download",
        ):
            facade.download()

"""Unit tests for StorageOperation and StorageUsecasesFacade."""

import pytest

from forze.application.composition.storage import (
    StorageOperation,
    StorageUsecasesFacade,
)
from forze.application.execution import ExecutionContext, UsecaseRegistry

# ----------------------- #


class TestStorageOperation:
    """Tests for StorageOperation enum."""

    def test_upload_value(self) -> None:
        assert StorageOperation.UPLOAD == "storage.upload"

    def test_list_value(self) -> None:
        assert StorageOperation.LIST == "storage.list"

    def test_download_value(self) -> None:
        assert StorageOperation.DOWNLOAD == "storage.download"

    def test_delete_value(self) -> None:
        assert StorageOperation.DELETE == "storage.delete"


class TestStorageUsecasesFacade:
    """Tests for StorageUsecasesFacade."""

    @pytest.fixture
    def mock_upload_usecase(self) -> UsecaseRegistry:
        """Registry with UPLOAD operation only."""
        from forze.application.execution import Usecase

        class StubUploadUsecase(Usecase[dict, dict]):
            async def main(self, args: dict) -> dict:
                return {"ok": True}

        return UsecaseRegistry().register(
            StorageOperation.UPLOAD,
            lambda ctx: StubUploadUsecase(ctx=ctx),
        )

    def test_upload_descriptor_resolves_usecase(
        self,
        stub_ctx: ExecutionContext,
        mock_upload_usecase: UsecaseRegistry,
    ) -> None:
        facade = StorageUsecasesFacade(ctx=stub_ctx, reg=mock_upload_usecase)
        uc = facade.upload

        assert uc is not None

    def test_download_raises_when_not_registered(
        self,
        stub_ctx: ExecutionContext,
        mock_upload_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = StorageUsecasesFacade(ctx=stub_ctx, reg=mock_upload_usecase)

        with pytest.raises(
            CoreError,
            match="not registered for operation: storage.download",
        ):
            facade.download()

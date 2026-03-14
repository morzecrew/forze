"""Unit tests for forze.application.composition.storage."""

from forze.application.composition.storage import (
    StorageDTOs,
    StorageOperation,
    StorageUsecasesFacade,
    build_storage_registry,
)
from forze.application.execution import UsecaseRegistry

# ----------------------- #


class TestBuildStorageRegistry:
    """Tests for build_storage_registry."""

    def test_returns_registry(self) -> None:
        reg = build_storage_registry("files", StorageDTOs())
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        reg = build_storage_registry("files", StorageDTOs())
        assert reg.exists(StorageOperation.UPLOAD)
        assert reg.exists(StorageOperation.LIST)
        assert reg.exists(StorageOperation.DOWNLOAD)
        assert reg.exists(StorageOperation.DELETE)

    def test_resolve_upload_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        reg = build_storage_registry("files", StorageDTOs())
        uc = reg.resolve(StorageOperation.UPLOAD, composition_ctx)
        assert uc is not None


class TestStorageFacadeWithRegistry:
    """Tests for StorageUsecasesFacade with build_storage_registry."""

    def test_facade_resolves_upload_usecase(
        self,
        composition_ctx,
    ) -> None:
        reg = build_storage_registry("files", StorageDTOs())
        facade = StorageUsecasesFacade(ctx=composition_ctx, reg=reg)
        uc = facade.upload
        assert uc is not None

"""Unit tests for forze.application.composition.storage."""

from forze.application.composition.storage import (
    StorageKernelOp,
    StorageUsecasesFacade,
    build_storage_registry,
)
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import UsecaseRegistry, operation_namespace_for

# ----------------------- #

_FILES = StorageSpec(name="files")


class TestBuildStorageRegistry:
    """Tests for build_storage_registry."""

    def test_returns_registry(self) -> None:
        reg = build_storage_registry(_FILES)
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        reg = build_storage_registry(_FILES)
        ops = operation_namespace_for(_FILES)
        assert reg.exists(ops.op(StorageKernelOp.UPLOAD))
        assert reg.exists(ops.op(StorageKernelOp.LIST))
        assert reg.exists(ops.op(StorageKernelOp.DOWNLOAD))
        assert reg.exists(ops.op(StorageKernelOp.DELETE))

    def test_resolve_upload_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        reg = build_storage_registry(_FILES)
        reg.finalize("storage")
        uc = reg.resolve(operation_namespace_for(_FILES).op(StorageKernelOp.UPLOAD), composition_ctx)
        assert uc is not None


class TestStorageFacadeWithRegistry:
    """Tests for StorageUsecasesFacade with build_storage_registry."""

    def test_facade_resolves_upload_usecase(
        self,
        composition_ctx,
    ) -> None:
        reg = build_storage_registry(_FILES)
        reg.finalize("storage")
        facade = StorageUsecasesFacade(
            ctx=composition_ctx,
            registry=reg,
            namespace=operation_namespace_for(_FILES),
        )
        uc = facade.upload
        assert uc is not None

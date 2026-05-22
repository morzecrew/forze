"""Unit tests for forze.application.composition.storage."""

from forze.application.composition.storage import (
    StorageFacade,
    StorageKernelOp,
    build_storage_registry,
)
from forze.application.contracts.storage import StorageSpec
from forze.application.execution.registry import OperationRegistry

from ..registry_helpers import registry_has_handler

# ----------------------- #

_FILES = StorageSpec(name="files")


class TestBuildStorageRegistry:
    """Tests for build_storage_registry."""

    def test_returns_registry(self) -> None:
        reg = build_storage_registry(_FILES)
        assert isinstance(reg, OperationRegistry)

    def test_has_core_operations(self) -> None:
        reg = build_storage_registry(_FILES)
        ns = _FILES.default_namespace
        assert registry_has_handler(reg, ns.key(StorageKernelOp.UPLOAD))
        assert registry_has_handler(reg, ns.key(StorageKernelOp.LIST))
        assert registry_has_handler(reg, ns.key(StorageKernelOp.DOWNLOAD))
        assert registry_has_handler(reg, ns.key(StorageKernelOp.DELETE))

    def test_resolve_upload_returns_handler(
        self,
        composition_ctx,
    ) -> None:
        reg = build_storage_registry(_FILES).freeze()
        op = _FILES.default_namespace.key(StorageKernelOp.UPLOAD)
        resolved = reg.resolve(op, composition_ctx)
        assert resolved is not None


class TestStorageFacadeWithRegistry:
    """Tests for StorageFacade with build_storage_registry."""

    def test_facade_resolves_upload(
        self,
        composition_ctx,
    ) -> None:
        reg = build_storage_registry(_FILES).freeze()
        facade = StorageFacade(
            ctx=composition_ctx,
            registry=reg,
            namespace=_FILES.default_namespace,
        )
        assert facade.upload is not None

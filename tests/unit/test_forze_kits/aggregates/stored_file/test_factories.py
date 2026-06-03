"""Unit tests for build_stored_file_registry."""

from forze_kits.aggregates.stored_file import (
    StoredFileFacade,
    StoredFileKernelOp,
    build_stored_file_registry,
)
from forze_kits.aggregates.search.operations import SearchKernelOp
from forze_kits.domain.stored_file import StoredFileKitSpec
from forze.application.execution.operations.registry import OperationRegistry

from tests.unit.test_forze_kits.registry_helpers import registry_has_handler


def _kit(*, with_search: bool = False) -> StoredFileKitSpec:
    return StoredFileKitSpec(
        name="files",
        search=StoredFileKitSpec.default_search("files") if with_search else None,
    )


class TestBuildStoredFileRegistry:
    def test_returns_registry(self) -> None:
        reg = build_stored_file_registry(_kit())
        assert isinstance(reg, OperationRegistry)

    def test_has_core_operations(self) -> None:
        kit = _kit()
        reg = build_stored_file_registry(kit)
        ns = kit.document.default_namespace
        assert registry_has_handler(reg, ns.key(StoredFileKernelOp.UPLOAD))
        assert registry_has_handler(reg, ns.key(StoredFileKernelOp.DOWNLOAD))
        assert registry_has_handler(reg, ns.key(StoredFileKernelOp.DELETE))
        assert registry_has_handler(reg, ns.key(StoredFileKernelOp.GET))
        assert registry_has_handler(reg, ns.key(StoredFileKernelOp.LIST))

    def test_search_merge_when_enabled(self) -> None:
        kit = _kit(with_search=True)
        reg = build_stored_file_registry(kit)
        ns = kit.document.default_namespace
        assert registry_has_handler(reg, ns.key(StoredFileKernelOp.SEARCH))
        assert registry_has_handler(reg, ns.key(SearchKernelOp.TYPED))

    def test_facade_resolves_upload(self, composition_ctx) -> None:
        kit = _kit()
        reg = build_stored_file_registry(kit).freeze()
        facade = StoredFileFacade(
            ctx=composition_ctx,
            registry=reg,
            namespace=kit.document.default_namespace,
        )
        assert facade.upload is not None

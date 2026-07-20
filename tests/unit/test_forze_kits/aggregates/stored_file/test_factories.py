"""Unit tests for build_stored_file_registry."""

from forze.application.execution.operations.registry import OperationRegistry
from forze_kits.aggregates.search.operations import SearchKernelOp
from forze_kits.aggregates.stored_file import (
    StoredFileFacade,
    StoredFileKernelOp,
    build_stored_file_registry,
)
from forze_kits.domain.stored_file import StoredFileKitSpec
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

    def test_catalog_classifies_read_vs_write(self) -> None:
        kit = _kit(with_search=True)
        ns = kit.document.default_namespace
        cat = build_stored_file_registry(kit).freeze().catalog()

        # Reads (incl. the merged search ops).
        assert cat[ns.key(StoredFileKernelOp.GET)].is_read_only is True
        assert cat[ns.key(StoredFileKernelOp.LIST)].is_read_only is True
        assert cat[ns.key(StoredFileKernelOp.DOWNLOAD)].is_read_only is True
        assert cat[ns.key(StoredFileKernelOp.SEARCH)].is_read_only is True
        assert cat[ns.key(SearchKernelOp.TYPED)].is_read_only is True

        # Writes.
        assert cat[ns.key(StoredFileKernelOp.UPLOAD)].is_read_only is False
        assert cat[ns.key(StoredFileKernelOp.DELETE)].is_read_only is False

    def test_descriptors_present_for_core_ops(self) -> None:
        kit = _kit()
        ns = kit.document.default_namespace
        cat = build_stored_file_registry(kit).freeze().catalog()

        get = cat[ns.key(StoredFileKernelOp.GET)].descriptor
        assert get is not None and get.output_schema() is not None
        list_ = cat[ns.key(StoredFileKernelOp.LIST)].descriptor
        assert list_ is not None
        assert "hits" in (list_.output_schema() or {}).get("properties", {})

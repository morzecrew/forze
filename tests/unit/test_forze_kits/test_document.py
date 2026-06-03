"""Unit tests for forze_kits.aggregates.document."""

import pytest

from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentFacade,
    DocumentKernelOp,
    build_document_registry,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.soft_deletion import (
    SoftDeletionKernelOp,
    build_soft_deletion_registry,
)
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from .registry_helpers import handler_at, registry_has_handler

# ----------------------- #


class _SoftDoc(DocWithSoftDeletion):
    """Document with soft-delete for registry tests."""

    pass


def _minimal_spec(
    supports_update: bool = False, supports_soft_delete: bool = False
) -> DocumentSpec:
    class UpdateCmd(BaseDTO):
        title: str | None = None

    update_cmd = UpdateCmd if supports_update else type("EmptyUpdate", (BaseDTO,), {})
    domain = _SoftDoc if supports_soft_delete else Document
    return DocumentSpec(
        name="test",
        read=ReadDocument,
        write={
            "domain": domain,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": update_cmd,
        },
    )


def _minimal_dtos(supports_update: bool = False) -> DocumentDTOs:
    class UpdateCmd(BaseDTO):
        title: str | None = None

    empty_update = type("EmptyUpdate", (BaseDTO,), {})
    return DocumentDTOs(
        read=ReadDocument,
        create=CreateDocumentCmd,
        update=UpdateCmd if supports_update else empty_update,
    )


class TestBuildDocumentRegistry:
    """Tests for build_document_registry."""

    def test_returns_registry(self) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos)
        assert isinstance(reg, OperationRegistry)

    def test_has_core_operations(self) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.GET))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.CREATE))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.KILL))

    def test_update_registered_when_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=True)
        dtos = _minimal_dtos(supports_update=True)
        reg = build_document_registry(spec, dtos)
        assert registry_has_handler(
            reg, spec.default_namespace.key(DocumentKernelOp.UPDATE)
        )

    def test_update_not_registered_when_no_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=False)
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos)
        assert not registry_has_handler(
            reg, spec.default_namespace.key(DocumentKernelOp.UPDATE)
        )

    def test_resolve_get_returns_handler(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos).freeze()
        op = spec.default_namespace.key(DocumentKernelOp.GET)
        resolved = reg.resolve(op, composition_ctx)
        assert resolved is not None

    def test_custom_namespace_registers_prefixed_keys(self) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        custom = StrKeyNamespace(prefix="orders")
        reg = build_document_registry(spec, dtos, ns=custom)
        assert registry_has_handler(reg, custom.key(DocumentKernelOp.GET))
        assert not registry_has_handler(
            reg, spec.default_namespace.key(DocumentKernelOp.GET)
        )


class TestDocumentFacadeWithRegistry:
    """Tests for DocumentFacade with build_document_registry."""

    def test_facade_resolves_get_operation(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec(supports_update=True, supports_soft_delete=True)
        dtos = _minimal_dtos(supports_update=True)
        reg = build_document_registry(spec, dtos).freeze()
        facade = DocumentFacade(
            ctx=composition_ctx,
            registry=reg,
            namespace=spec.default_namespace,
        )
        op = facade.get
        assert op is not None


class TestSoftDeletionRegistryMerge:
    def test_soft_delete_ops_from_contrib_registry(self) -> None:
        spec = _minimal_spec(supports_update=True, supports_soft_delete=True)
        dtos = _minimal_dtos(supports_update=True)
        doc_reg = build_document_registry(spec, dtos)
        soft_reg = build_soft_deletion_registry(spec)
        reg = OperationRegistry.merge(doc_reg, soft_reg)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(SoftDeletionKernelOp.DELETE))
        assert registry_has_handler(reg, ns.key(SoftDeletionKernelOp.RESTORE))

    def test_no_soft_delete_without_contrib_registry(self) -> None:
        spec = _minimal_spec(supports_soft_delete=True)
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos)
        ns = spec.default_namespace
        assert not registry_has_handler(reg, ns.key(SoftDeletionKernelOp.DELETE))

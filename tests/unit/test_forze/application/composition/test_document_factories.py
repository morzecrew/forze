"""Tests for forze.application.composition.document.factories."""

from forze.application.composition.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution.registry import OperationRegistry
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_patterns.soft_deletion.composition import (
    SoftDeletionKernelOp,
    build_soft_deletion_registry,
)
from forze_patterns.soft_deletion.models import DocWithSoftDeletion

from ..registry_helpers import registry_has_handler

# ----------------------- #


class _UpdateCmd(BaseDTO):
    name: str


class _EmptyUpdateCmd(BaseDTO):
    pass


class _SoftDoc(DocWithSoftDeletion):
    name: str


def _write_spec(
    domain: type = Document,
    update_cmd: type = _UpdateCmd,
) -> DocumentSpec:
    return DocumentSpec(
        name="test",
        read=ReadDocument,
        write={
            "domain": domain,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": update_cmd,
        },
    )


def _read_only_spec() -> DocumentSpec:
    return DocumentSpec(
        name="test",
        read=ReadDocument,
    )


def _write_dtos(update_cmd: type = _UpdateCmd) -> DocumentDTOs:
    return DocumentDTOs(
        read=ReadDocument,
        create=CreateDocumentCmd,
        update=update_cmd,
    )


def _read_only_dtos() -> DocumentDTOs:
    return DocumentDTOs(read=ReadDocument)


class TestBuildDocumentRegistry:
    def test_registers_get(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        reg = build_document_registry(spec, dtos)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.GET))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.LIST_CURSOR))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.RAW_LIST_CURSOR))

    def test_registers_create_and_kill(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        reg = build_document_registry(spec, dtos)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.CREATE))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.KILL))

    def test_registers_update_when_update_cmd_has_fields(self) -> None:
        spec = _write_spec(update_cmd=_UpdateCmd)
        dtos = _write_dtos(update_cmd=_UpdateCmd)
        reg = build_document_registry(spec, dtos)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.UPDATE))

    def test_skips_update_when_update_cmd_empty(self) -> None:
        spec = _write_spec(update_cmd=_EmptyUpdateCmd)
        dtos = _write_dtos(update_cmd=_EmptyUpdateCmd)
        reg = build_document_registry(spec, dtos)
        ns = spec.default_namespace
        assert not registry_has_handler(reg, ns.key(DocumentKernelOp.UPDATE))

    def test_soft_delete_via_contrib_registry(self) -> None:
        spec = _write_spec(domain=_SoftDoc)
        dtos = _write_dtos()
        reg = OperationRegistry.merge(
            build_document_registry(spec, dtos),
            build_soft_deletion_registry(spec),
        )
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(SoftDeletionKernelOp.DELETE))
        assert registry_has_handler(reg, ns.key(SoftDeletionKernelOp.RESTORE))

    def test_no_delete_restore_without_contrib_registry(self) -> None:
        spec = _write_spec(domain=Document)
        dtos = _write_dtos()
        reg = build_document_registry(spec, dtos)
        ns = spec.default_namespace
        assert not registry_has_handler(reg, ns.key(SoftDeletionKernelOp.DELETE))

    def test_read_only_spec_only_get(self) -> None:
        spec = _read_only_spec()
        dtos = _read_only_dtos()
        reg = build_document_registry(spec, dtos)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.GET))
        assert not registry_has_handler(reg, ns.key(DocumentKernelOp.CREATE))

"""Tests for forze_kits.aggregates.document.factories."""

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)
from forze_kits.aggregates.document.factories import (
    _default_create_mapper,
    _default_update_mapper,
)
from forze_kits.aggregates.soft_deletion import (
    SoftDeletionKernelOp,
    build_soft_deletion_registry,
)
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion

from .registry_helpers import registry_has_handler

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


class TestDefaultMappers:
    def test_create_mapper_requires_dto_and_cmd(self) -> None:
        spec = _write_spec()
        dtos = DocumentDTOs(read=ReadDocument, create=None, update=_UpdateCmd)

        with pytest.raises(exc, match="Create DTO"):
            _default_create_mapper(spec, dtos)

    def test_update_mapper_requires_dto_and_cmd(self) -> None:
        spec = DocumentSpec(
            name="test",
            read=ReadDocument,
            write={"domain": Document, "create_cmd": CreateDocumentCmd},
        )
        dtos = DocumentDTOs(read=ReadDocument, create=CreateDocumentCmd, update=None)

        with pytest.raises(exc, match="Update DTO"):
            _default_update_mapper(spec, dtos)


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


class TestDtosFromSpec:
    def test_derives_read_create_update_from_write_spec(self) -> None:
        dtos = DocumentDTOs.from_spec(_write_spec(update_cmd=_UpdateCmd))
        assert dtos.read is ReadDocument
        assert dtos.create is CreateDocumentCmd
        assert dtos.update is _UpdateCmd

    def test_read_only_spec_has_no_write_dtos(self) -> None:
        dtos = DocumentDTOs.from_spec(_read_only_spec())
        assert dtos.read is ReadDocument
        assert dtos.create is None
        assert dtos.update is None

    def test_omitted_dtos_are_derived_from_spec(self) -> None:
        # build_document_registry(spec) with no dtos registers the same write ops as
        # passing the explicitly-derived DTOs.
        spec = _write_spec()
        ns = spec.default_namespace
        reg = build_document_registry(spec)

        assert registry_has_handler(reg, ns.key(DocumentKernelOp.GET))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.CREATE))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.UPDATE))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.KILL))

    def test_explicit_dtos_still_override_to_disable_an_op(self) -> None:
        # The override path survives: create=None disables CREATE despite the spec
        # declaring a create command.
        spec = _write_spec()
        ns = spec.default_namespace
        reg = build_document_registry(
            spec, DocumentDTOs(read=ReadDocument, create=None, update=_UpdateCmd)
        )

        assert not registry_has_handler(reg, ns.key(DocumentKernelOp.CREATE))
        assert registry_has_handler(reg, ns.key(DocumentKernelOp.UPDATE))


class TestDocumentCatalog:
    def test_read_ops_are_query_and_writes_are_command(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        ns = spec.default_namespace
        cat = build_document_registry(spec, dtos).freeze().catalog()

        assert cat[ns.key(DocumentKernelOp.GET)].is_read_only is True
        assert cat[ns.key(DocumentKernelOp.LIST)].is_read_only is True
        assert cat[ns.key(DocumentKernelOp.AGG_LIST)].is_read_only is True
        assert cat[ns.key(DocumentKernelOp.CREATE)].is_read_only is False
        assert cat[ns.key(DocumentKernelOp.UPDATE)].is_read_only is False
        assert cat[ns.key(DocumentKernelOp.KILL)].is_read_only is False

    def test_descriptors_carry_schemas(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        ns = spec.default_namespace
        cat = build_document_registry(spec, dtos).freeze().catalog()

        get = cat[ns.key(DocumentKernelOp.GET)].descriptor
        assert get is not None
        assert get.input_schema() is not None
        assert get.output_schema() is not None

        # LIST output is a generic Paginated[read] envelope.
        list_out = cat[ns.key(DocumentKernelOp.LIST)].descriptor
        assert list_out is not None
        assert "hits" in (list_out.output_schema() or {}).get("properties", {})

        # KILL returns nothing.
        kill = cat[ns.key(DocumentKernelOp.KILL)].descriptor
        assert kill is not None
        assert kill.output_schema() is None


class TestSensitivePropagation:
    def test_descriptors_not_sensitive_by_default(self) -> None:
        spec = _write_spec()
        cat = build_document_registry(spec, _write_dtos()).freeze().catalog()

        assert all(
            entry.descriptor is not None and entry.descriptor.sensitive is False
            for entry in cat.values()
        )

    def test_sensitive_spec_marks_every_descriptor(self) -> None:
        spec = DocumentSpec(
            name="test",
            read=ReadDocument,
            write={
                "domain": Document,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": _UpdateCmd,
            },
            sensitive=True,
        )
        cat = build_document_registry(spec, _write_dtos()).freeze().catalog()

        assert cat
        assert all(
            entry.descriptor is not None and entry.descriptor.sensitive is True
            for entry in cat.values()
        )

    def test_sensitive_spec_marks_soft_deletion_descriptors(self) -> None:
        spec = DocumentSpec(
            name="test",
            read=ReadDocument,
            write={
                "domain": _SoftDoc,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": _UpdateCmd,
            },
            sensitive=True,
        )
        ns = spec.default_namespace
        cat = build_soft_deletion_registry(spec).freeze().catalog()

        for op in (SoftDeletionKernelOp.DELETE, SoftDeletionKernelOp.RESTORE):
            descriptor = cat[ns.key(op)].descriptor
            assert descriptor is not None
            assert descriptor.sensitive is True

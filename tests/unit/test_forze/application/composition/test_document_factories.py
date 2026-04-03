"""Tests for forze.application.composition.document.factories."""

import pytest

from forze.application.composition.document import DocumentDTOs
from forze.application.composition.document.factories import (
    build_document_create_mapper,
    build_document_registry,
)
from forze.application.composition.document.operations import DocumentOperation
from forze.application.contracts.document import DocumentSpec
from forze.application.composition.mapping import DTOMapper, NumberIdStep
from forze.application.contracts.counter import CounterSpec
from forze.base.errors import CoreError
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _UpdateCmd(BaseDTO):
    name: str


class _EmptyUpdateCmd(BaseDTO):
    pass


class _SoftDoc(Document, SoftDeletionMixin):
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


# ----------------------- #


class TestBuildDocumentCreateMapper:
    def test_basic(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        mapper = build_document_create_mapper(spec, dtos)
        assert isinstance(mapper, DTOMapper)

    def test_numbered(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        mapper = build_document_create_mapper(
            spec, dtos, steps=(NumberIdStep(spec=CounterSpec(name="test")),)
        )
        assert isinstance(mapper, DTOMapper)

    def test_read_only_spec_raises(self) -> None:
        spec = _read_only_spec()
        dtos = _read_only_dtos()
        with pytest.raises(CoreError, match="does not support write"):
            build_document_create_mapper(spec, dtos)


class TestBuildDocumentRegistry:
    def test_registers_get(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        reg = build_document_registry(spec, dtos)
        assert reg.exists(DocumentOperation.GET)

    def test_registers_create_and_kill(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        reg = build_document_registry(spec, dtos)
        assert reg.exists(DocumentOperation.CREATE)
        assert reg.exists(DocumentOperation.KILL)

    def test_registers_update_when_update_cmd_has_fields(self) -> None:
        spec = _write_spec(update_cmd=_UpdateCmd)
        dtos = _write_dtos(update_cmd=_UpdateCmd)
        reg = build_document_registry(spec, dtos)
        assert reg.exists(DocumentOperation.UPDATE)

    def test_skips_update_when_update_cmd_empty(self) -> None:
        spec = _write_spec(update_cmd=_EmptyUpdateCmd)
        dtos = _write_dtos(update_cmd=_EmptyUpdateCmd)
        reg = build_document_registry(spec, dtos)
        assert not reg.exists(DocumentOperation.UPDATE)

    def test_registers_delete_restore_for_soft_delete(self) -> None:
        spec = _write_spec(domain=_SoftDoc)
        dtos = _write_dtos()
        reg = build_document_registry(spec, dtos)
        assert reg.exists(DocumentOperation.DELETE)
        assert reg.exists(DocumentOperation.RESTORE)

    def test_no_delete_restore_without_soft_delete(self) -> None:
        spec = _write_spec(domain=Document)
        dtos = _write_dtos()
        reg = build_document_registry(spec, dtos)
        assert not reg.exists(DocumentOperation.DELETE)
        assert not reg.exists(DocumentOperation.RESTORE)

    def test_read_only_spec_only_get(self) -> None:
        spec = _read_only_spec()
        dtos = _read_only_dtos()
        reg = build_document_registry(spec, dtos)
        assert reg.exists(DocumentOperation.GET)
        assert not reg.exists(DocumentOperation.CREATE)

    def test_custom_create_steps(self) -> None:
        spec = _write_spec()
        dtos = _write_dtos()
        reg = build_document_registry(
            spec, dtos, create_steps=(NumberIdStep(spec=CounterSpec(name="test")),)
        )
        assert reg.exists(DocumentOperation.CREATE)

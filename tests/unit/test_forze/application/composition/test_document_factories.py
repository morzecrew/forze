"""Tests for forze.application.composition.document.factories."""

import pytest

from forze.application.composition.document.factories import (
    build_document_create_mapper,
    build_document_plan,
    build_document_registry,
)
from forze.application.composition.document.operations import DocumentOperation
from forze.application.contracts.document import DocumentSpec
from forze.application.mapping import DTOMapper
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
        namespace="test",
        read={"source": "test_r", "model": ReadDocument},
        write={
            "source": "test_w",
            "models": {
                "domain": domain,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": update_cmd,
            },
        },
    )


def _read_only_spec() -> DocumentSpec:
    return DocumentSpec(
        namespace="test",
        read={"source": "test_r", "model": ReadDocument},
    )


# ----------------------- #


class TestBuildDocumentPlan:
    def test_default_with_tx(self) -> None:
        plan = build_document_plan()
        assert plan is not None

    def test_without_tx(self) -> None:
        plan = build_document_plan(tx_on_write=False)
        assert plan is not None


class TestBuildDocumentCreateMapper:
    def test_basic(self) -> None:
        spec = _write_spec()
        mapper = build_document_create_mapper(spec)
        assert isinstance(mapper, DTOMapper)

    def test_numbered(self) -> None:
        spec = _write_spec()
        mapper = build_document_create_mapper(spec, numbered=True)
        assert isinstance(mapper, DTOMapper)

    def test_read_only_spec_raises(self) -> None:
        spec = _read_only_spec()
        with pytest.raises(CoreError, match="does not support write"):
            build_document_create_mapper(spec)


class TestBuildDocumentRegistry:
    def test_registers_get(self) -> None:
        spec = _write_spec()
        reg = build_document_registry(spec)
        assert reg.exists(DocumentOperation.GET)

    def test_registers_create_and_kill(self) -> None:
        spec = _write_spec()
        reg = build_document_registry(spec)
        assert reg.exists(DocumentOperation.CREATE)
        assert reg.exists(DocumentOperation.KILL)

    def test_registers_update_when_update_cmd_has_fields(self) -> None:
        spec = _write_spec(update_cmd=_UpdateCmd)
        reg = build_document_registry(spec)
        assert reg.exists(DocumentOperation.UPDATE)

    def test_skips_update_when_update_cmd_empty(self) -> None:
        spec = _write_spec(update_cmd=_EmptyUpdateCmd)
        reg = build_document_registry(spec)
        assert not reg.exists(DocumentOperation.UPDATE)

    def test_registers_delete_restore_for_soft_delete(self) -> None:
        spec = _write_spec(domain=_SoftDoc)
        reg = build_document_registry(spec)
        assert reg.exists(DocumentOperation.DELETE)
        assert reg.exists(DocumentOperation.RESTORE)

    def test_no_delete_restore_without_soft_delete(self) -> None:
        spec = _write_spec(domain=Document)
        reg = build_document_registry(spec)
        assert not reg.exists(DocumentOperation.DELETE)
        assert not reg.exists(DocumentOperation.RESTORE)

    def test_read_only_spec_only_get(self) -> None:
        spec = _read_only_spec()
        reg = build_document_registry(spec)
        assert reg.exists(DocumentOperation.GET)
        assert not reg.exists(DocumentOperation.CREATE)

    def test_custom_create_mapper(self) -> None:
        spec = _write_spec()
        custom_mapper = DTOMapper(out=CreateDocumentCmd)
        reg = build_document_registry(spec, replace_create_mapper=custom_mapper)
        assert reg.exists(DocumentOperation.CREATE)

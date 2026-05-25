"""Unit tests for document operation catalog."""

from __future__ import annotations

import pytest

from forze.application.composition.document import (
    DocumentDTOs,
    document_capability_allows,
)
from forze.application.contracts.document import DocumentSpec
from forze_contrib.soft_deletion import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

pytestmark = pytest.mark.unit


class _SoftDoc(Document, SoftDeletionMixin):
    pass


def _spec(*, supports_update: bool = False, supports_soft_delete: bool = False) -> DocumentSpec:
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


def _dtos(*, supports_update: bool = False) -> DocumentDTOs:
    class UpdateCmd(BaseDTO):
        title: str | None = None

    empty = type("EmptyUpdate", (BaseDTO,), {})
    return DocumentDTOs(
        read=ReadDocument,
        create=CreateDocumentCmd,
        update=UpdateCmd if supports_update else empty,
    )


class TestDocumentCapabilityAllows:
    def test_create_requires_write_and_dto(self) -> None:
        spec = _spec()
        dtos = _dtos()
        assert document_capability_allows("create", spec, dtos)

    def test_update_requires_support(self) -> None:
        spec = _spec(supports_update=True)
        dtos = _dtos(supports_update=True)
        assert document_capability_allows("update", spec, dtos)
        assert not document_capability_allows("update", _spec(), _dtos())

    def test_soft_delete_requires_mixin(self) -> None:
        spec = _spec(supports_soft_delete=True)
        dtos = _dtos()
        assert document_capability_allows("delete", spec, dtos)
        assert not document_capability_allows("delete", _spec(), dtos)

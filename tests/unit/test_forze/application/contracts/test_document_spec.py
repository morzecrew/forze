"""Tests for :class:`~forze.application.contracts.document.DocumentSpec`."""

import msgspec
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument


class _Read(ReadDocument):
    name: str


class _Domain(Document):
    name: str


class _Create(CreateDocumentCmd):
    name: str


class _PydanticUpdate(BaseDTO):
    name: str | None = None


class _EmptyPydanticUpdate(BaseDTO):
    pass


class _MsgspecUpdate(msgspec.Struct, forbid_unknown_fields=True):
    name: str | None = None


class _EmptyMsgspecUpdate(msgspec.Struct, forbid_unknown_fields=True):
    pass


def test_supports_update_true_for_pydantic_with_fields() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_PydanticUpdate,
        ),
    )
    assert spec.supports_update() is True


def test_supports_update_false_for_empty_pydantic_update() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_EmptyPydanticUpdate,
        ),
    )
    assert spec.supports_update() is False


def test_supports_update_true_for_msgspec_with_fields() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_MsgspecUpdate,
        ),
    )
    assert spec.supports_update() is True


def test_supports_update_false_for_empty_msgspec_update() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_EmptyMsgspecUpdate,
        ),
    )
    assert spec.supports_update() is False

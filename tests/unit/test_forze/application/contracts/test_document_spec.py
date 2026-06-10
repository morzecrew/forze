"""Tests for :class:`~forze.application.contracts.document.DocumentSpec`."""

import msgspec
import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import QueryFieldPolicy
from forze.base.exceptions import CoreException
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


# ----------------------- #
# Query field policy


def test_query_policy_defaults_to_all_read_fields() -> None:
    spec = DocumentSpec(name="doc", read=_Read)

    # No policy → every read-model field is filterable and sortable.
    assert "name" in spec.filterable_fields()
    assert "id" in spec.sortable_fields()
    assert spec.filterable_fields() == spec.sortable_fields()


def test_query_policy_restricts_axes_independently() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        query_policy=QueryFieldPolicy(filterable={"name"}, sortable=["id"]),
    )

    assert spec.filterable_fields() == frozenset({"name"})
    assert spec.sortable_fields() == frozenset({"id"})


def test_query_policy_none_axis_means_all_fields() -> None:
    # Only filterable is constrained; sortable (None) stays all read fields.
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        query_policy=QueryFieldPolicy(filterable={"name"}),
    )

    assert spec.filterable_fields() == frozenset({"name"})
    assert "id" in spec.sortable_fields() and "name" in spec.sortable_fields()


def test_query_policy_unknown_field_rejected_at_construction() -> None:
    with pytest.raises(CoreException, match="not on the read model"):
        DocumentSpec(
            name="doc",
            read=_Read,
            query_policy=QueryFieldPolicy(filterable={"nonexistent"}),
        )


def test_sensitive_defaults_to_false() -> None:
    spec = DocumentSpec(name="doc", read=_Read)

    assert spec.sensitive is False


def test_sensitive_flag_round_trips() -> None:
    spec = DocumentSpec(name="doc", read=_Read, sensitive=True)

    assert spec.sensitive is True

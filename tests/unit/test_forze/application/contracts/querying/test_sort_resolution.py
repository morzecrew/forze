"""Tests for :mod:`forze.application.contracts.querying.sort_resolution`."""

from datetime import datetime

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.querying.sort_resolution import (
    normalize_sorts_for_keyset,
    read_fields_for_model,
    resolve_effective_sorts,
    validate_sort_fields,
)
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze.domain.constants import ID_FIELD


class _WithId(BaseModel):
    id: str
    name: str


class _ViewRow(BaseModel):
    generated_at: datetime
    tb_chat_id: int


def test_resolve_effective_sorts_caller_wins() -> None:
    fields = read_fields_for_model(_ViewRow)
    out = resolve_effective_sorts(
        sorts={"tb_chat_id": "desc"},
        default_sort={"generated_at": "asc"},
        read_fields=fields,
        spec_name="t",
    )
    assert out == {"tb_chat_id": "desc"}


def test_resolve_effective_sorts_uses_default_sort() -> None:
    fields = read_fields_for_model(_ViewRow)
    out = resolve_effective_sorts(
        sorts=None,
        default_sort={"generated_at": "desc", "tb_chat_id": "asc"},
        read_fields=fields,
        spec_name="t",
    )
    assert out == {"generated_at": "desc", "tb_chat_id": "asc"}


def test_resolve_effective_sorts_id_fallback() -> None:
    fields = read_fields_for_model(_WithId)
    out = resolve_effective_sorts(
        sorts=None,
        default_sort=None,
        read_fields=fields,
        spec_name="t",
    )
    assert out == {ID_FIELD: "asc"}


def test_resolve_effective_sorts_precondition_without_id_or_default() -> None:
    fields = read_fields_for_model(_ViewRow)
    with pytest.raises(CoreException, match="default_sort"):
        resolve_effective_sorts(
            sorts=None,
            default_sort=None,
            read_fields=fields,
            spec_name="view",
        )


def test_normalize_sorts_for_keyset_skips_id_tiebreaker_when_absent() -> None:
    fields = read_fields_for_model(_ViewRow)
    out = normalize_sorts_for_keyset(
        {"generated_at": "desc", "tb_chat_id": "desc"},
        read_fields=fields,
    )
    assert out == [("generated_at", "desc"), ("tb_chat_id", "desc")]


def test_normalize_sorts_for_keyset_appends_id_when_present() -> None:
    fields = read_fields_for_model(_WithId)
    out = normalize_sorts_for_keyset(
        {"name": "asc"},
        read_fields=fields,
    )
    assert out == [("name", "asc"), (ID_FIELD, "asc")]


def test_validate_sort_fields_unknown_field() -> None:
    fields = read_fields_for_model(_ViewRow)
    with pytest.raises(CoreException, match="not on read model"):
        validate_sort_fields(
            {"missing": "asc"},
            read_fields=fields,
            spec_name="x",
        )


def test_document_spec_validates_default_sort() -> None:
    DocumentSpec(
        name="v",
        read=_ViewRow,
        default_sort={"generated_at": "desc", "tb_chat_id": "asc"},
    )


def test_document_spec_rejects_invalid_default_sort_field() -> None:
    with pytest.raises(CoreException, match="not on read model"):
        DocumentSpec(
            name="v",
            read=_ViewRow,
            default_sort={"id": "asc"},
        )


def test_search_spec_validates_default_sort() -> None:
    SearchSpec(
        name="s",
        model_type=_ViewRow,
        fields=["generated_at"],
        default_sort={"generated_at": "desc", "tb_chat_id": "asc"},
    )

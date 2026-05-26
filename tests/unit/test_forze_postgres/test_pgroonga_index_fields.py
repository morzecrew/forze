"""Unit tests for PGroonga index field resolution and alignment."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_postgres.adapters.search._pgroonga_index_fields import (
    align_pgroonga_search_columns,
    heap_columns_to_logical,
    parse_pgroonga_index_heap_columns,
    resolve_pgroonga_index_alignment,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.introspect.types import PostgresIndexInfo


class _Doc(BaseModel):
    a: str
    b: str
    c: str


_IDX = PostgresQualifiedName("public", "idx_test")


def _info(
    *, expr: str | None = None, columns: tuple[str, ...] = ()
) -> PostgresIndexInfo:
    return PostgresIndexInfo(
        schema="public",
        name="idx_test",
        amname="pgroonga",
        engine="pgroonga",
        indexdef="CREATE INDEX idx_test ...",
        expr=expr,
        columns=columns,
    )


def test_parse_array_columns() -> None:
    assert parse_pgroonga_index_heap_columns(
        "(ARRAY[title, content])",
        (),
        index_qname=_IDX,
    ) == ("title", "content")


def test_parse_single_column_from_expr() -> None:
    assert parse_pgroonga_index_heap_columns(
        "(title)",
        (),
        index_qname=_IDX,
    ) == ("title",)


def test_parse_columns_fallback() -> None:
    assert parse_pgroonga_index_heap_columns(
        None,
        ("doc_title", "doc_body"),
        index_qname=_IDX,
    ) == ("doc_title", "doc_body")


def test_parse_unparseable_raises() -> None:
    with pytest.raises(exc.internal, match="Cannot resolve PGroonga index columns"):
        parse_pgroonga_index_heap_columns(
            "to_tsvector(title)",
            (),
            index_qname=_IDX,
        )


def test_heap_columns_to_logical_with_field_map() -> None:
    logical = heap_columns_to_logical(
        ("doc_title", "doc_body"),
        {"title": "doc_title", "content": "doc_body"},
    )
    assert logical == ("title", "content")


def test_heap_columns_to_logical_ambiguous_map_raises() -> None:
    with pytest.raises(exc.internal, match="Ambiguous field_map"):
        heap_columns_to_logical(
            ("col",),
            {"a": "col", "b": "col"},
        )


def test_align_uses_index_order_not_spec_order() -> None:
    spec = SearchSpec(name="t", model_type=_Doc, fields=["b", "a", "c"])
    heap, weights = align_pgroonga_search_columns(
        spec,
        ("a", "b"),
        {"a": "col_a", "b": "col_b"},
        {"a": 10, "b": 20, "c": 0},
        index_qname=_IDX,
    )
    assert heap == ["col_a", "col_b"]
    assert weights == [10, 20]


def test_align_missing_spec_field_raises() -> None:
    spec = SearchSpec(name="t", model_type=_Doc, fields=["a"])
    with pytest.raises(exc.internal, match="add it to SearchSpec.fields"):
        align_pgroonga_search_columns(
            spec,
            ("a", "b"),
            None,
            {"a": 1, "b": 2},
            index_qname=_IDX,
        )


def test_resolve_pgroonga_index_alignment_reversed_spec() -> None:
    spec = SearchSpec(name="t", model_type=_Doc, fields=["b", "a"])
    heap, weights, uses_array = resolve_pgroonga_index_alignment(
        spec,
        _info(expr="(ARRAY[col_a, col_b])"),
        {"a": "col_a", "b": "col_b"},
        {"a": 100, "b": 1},
        index_qname=_IDX,
    )
    assert uses_array is True
    assert heap == ["col_a", "col_b"]
    assert weights == [100, 1]

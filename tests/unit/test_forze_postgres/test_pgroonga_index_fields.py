"""Unit tests for PGroonga index field resolution and alignment."""

import pytest

from forze.base.exceptions import CoreException
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_postgres.adapters.search._pgroonga_index_fields import (
    align_pgroonga_search_columns,
    heap_columns_to_logical,
    parse_pgroonga_index_heap_columns,
    pgroonga_index_uses_array_expr,
    resolve_pgroonga_index_alignment,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.catalog.introspect.types import PostgresIndexInfo


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


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        # ARRAY of COALESCE-wrapped columns (the reported eis.idx_ktru case):
        # paren-aware split must keep each COALESCE(...) element intact.
        (
            "(ARRAY[COALESCE(name, ''::text), COALESCE(code, ''::text)])",
            ("name", "code"),
        ),
        # COALESCE without the ::text cast on the default.
        ("(ARRAY[COALESCE(name, ''), COALESCE(code, '')])", ("name", "code")),
        # Mixed: a bare column alongside a COALESCE-wrapped one.
        ("(ARRAY[name, COALESCE(code, ''::text)])", ("name", "code")),
        # Per-element ::type casts.
        ("(ARRAY[name::text, code::text])", ("name", "code")),
        # COALESCE call itself carrying a trailing cast.
        ("(ARRAY[COALESCE(name, '')::text, code])", ("name", "code")),
        # Irregular whitespace around elements and commas.
        ("(ARRAY[ COALESCE(name , ''::text) ,code ])", ("name", "code")),
    ],
)
def test_parse_array_coalesced_and_cast_columns(
    expr: str, expected: tuple[str, ...]
) -> None:
    assert (
        parse_pgroonga_index_heap_columns(expr, (), index_qname=_IDX) == expected
    )


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("(name::text)", ("name",)),
        ("(COALESCE(name, ''::text))", ("name",)),
        ("COALESCE(name, '')", ("name",)),
    ],
)
def test_parse_single_column_coalesced_or_cast(
    expr: str, expected: tuple[str, ...]
) -> None:
    assert (
        parse_pgroonga_index_heap_columns(expr, (), index_qname=_IDX) == expected
    )


@pytest.mark.parametrize(
    "expr",
    [
        # Transforms Forze cannot reproduce as coalesce(col::text, '') must stay
        # fail-closed rather than silently search the wrong expression.
        "(ARRAY[lower(name), code])",
        "(ARRAY[name || code])",
        "(ARRAY[COALESCE(lower(name), '')])",
        "(lower(title))",
    ],
)
def test_parse_unreproducible_transform_raises(expr: str) -> None:
    with pytest.raises(CoreException, match="Cannot resolve PGroonga index columns"):
        parse_pgroonga_index_heap_columns(expr, (), index_qname=_IDX)


def test_parse_array_element_with_empty_default_balanced() -> None:
    # A bare column alongside an empty-default COALESCE element resolves; the
    # balanced, literal-aware scan keeps each element intact.
    assert parse_pgroonga_index_heap_columns(
        "(ARRAY[name, COALESCE(code, ''::text)])",
        (),
        index_qname=_IDX,
    ) == ("name", "code")


@pytest.mark.parametrize(
    "expr",
    [
        # A non-empty/structural literal default is not the empty-string form
        # Forze rebuilds, so it must fail closed -- but the literal-aware scan
        # must still structure the element correctly (not corrupt depth) before
        # rejecting it, rather than crash or mis-split.
        "(ARRAY[COALESCE(name, 'a,b'), code])",
        "(ARRAY[COALESCE(name, '(x)'), code])",
        "(ARRAY[COALESCE(name, '],['::text), code])",
        "(ARRAY[COALESCE(name, ']'::text), code])",
        "(COALESCE(name, ',(['))",
    ],
)
def test_parse_nonempty_literal_default_fails_closed(expr: str) -> None:
    with pytest.raises(CoreException, match="Cannot resolve PGroonga index columns"):
        parse_pgroonga_index_heap_columns(expr, (), index_qname=_IDX)


@pytest.mark.parametrize(
    "expr",
    [
        # Forze rebuilds the match as ``coalesce(col::text, '')``; a COALESCE
        # with a non-empty literal default or a column fallback indexes values
        # Forze cannot reproduce, so accepting it would silently miss rows.
        "(ARRAY[COALESCE(title, 'missing'), body])",
        "(COALESCE(title, 'missing'::text))",
        "(ARRAY[COALESCE(title, fallback_col), body])",
        "(COALESCE(title, other_col))",
    ],
)
def test_parse_coalesce_non_empty_default_fails_closed(expr: str) -> None:
    with pytest.raises(CoreException, match="Cannot resolve PGroonga index columns"):
        parse_pgroonga_index_heap_columns(expr, (), index_qname=_IDX)


@pytest.mark.parametrize(
    "expr",
    [
        # ARRAY[...] must be the whole expression (after peeling parens). An
        # ARRAY nested in a transform, or with trailing text, is a different
        # expression than Forze would search, so it must be rejected.
        "(concat(ARRAY[title], body))",
        "(ARRAY[a, b] || extra)",
        "(ARRAY[a, b]::text[])",
    ],
)
def test_parse_non_top_level_array_fails_closed(expr: str) -> None:
    with pytest.raises(CoreException, match="Cannot resolve PGroonga index columns"):
        parse_pgroonga_index_heap_columns(expr, (), index_qname=_IDX)


@pytest.mark.parametrize(
    "expr",
    [
        # A ``::`` cast that applies to a sub-expression (operator/extra text
        # after the type) must not be peeled to the first operand, else Forze
        # would search a column instead of the indexed concatenation.
        "(name::text || code)",
        "(ARRAY[name::text || code, body])",
        "((a || b)::text)",
        "(name::text::varchar)",
    ],
)
def test_parse_cast_on_subexpression_fails_closed(expr: str) -> None:
    with pytest.raises(CoreException, match="Cannot resolve PGroonga index columns"):
        parse_pgroonga_index_heap_columns(expr, (), index_qname=_IDX)


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        # A cast that wraps the whole (preceding) expression is still peeled.
        ("(name::text)", ("name",)),
        ("(COALESCE(name, '')::text)", ("name",)),
        ("(id::int[])", ("id",)),
        ("(ts::timestamp with time zone)", ("ts",)),
        ("(ARRAY[name::text, code::varchar(255)])", ("name", "code")),
    ],
)
def test_parse_whole_expression_cast_resolves(
    expr: str, expected: tuple[str, ...]
) -> None:
    assert parse_pgroonga_index_heap_columns(expr, (), index_qname=_IDX) == expected


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("(ARRAY[a, b])", True),
        ("(ARRAY [a, b])", True),
        ("(title)", False),
        (None, False),
        # Must not misfire on a column whose name merely contains "array".
        ("COALESCE(array_field, ''::text)", False),
        ("(my_array_col)", False),
        # An ARRAY[ inside a quoted literal default is not the constructor
        # (including one carrying a comma that could fool a naive split).
        ("(COALESCE(col, 'ARRAY[x]'::text))", False),
        ("(COALESCE(title, 'ARRAY[a,b]'))", False),
        # ARRAY nested in a transform / with trailing text is not top-level.
        ("(concat(ARRAY[title], body))", False),
        ("(ARRAY[a, b] || extra)", False),
    ],
)
def test_pgroonga_index_uses_array_expr_detects_constructor(
    expr: str | None, expected: bool
) -> None:
    assert pgroonga_index_uses_array_expr(expr) is expected


def test_parse_single_column_named_like_array() -> None:
    # The "array"-in-name column resolves as a single column, not an ARRAY form.
    assert parse_pgroonga_index_heap_columns(
        "(COALESCE(array_field, ''::text))",
        (),
        index_qname=_IDX,
    ) == ("array_field",)


def test_parse_single_column_with_array_in_literal_default_fails_closed() -> None:
    # The ARRAY[ inside the quoted default is not the constructor (not array),
    # and the non-empty default is not reproducible, so this fails closed.
    with pytest.raises(CoreException, match="Cannot resolve PGroonga index columns"):
        parse_pgroonga_index_heap_columns(
            "(COALESCE(col, 'ARRAY[x]'::text))",
            (),
            index_qname=_IDX,
        )


def test_parse_unparseable_raises() -> None:
    with pytest.raises(CoreException, match="Cannot resolve PGroonga index columns"):
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
    with pytest.raises(CoreException, match="Ambiguous field_map"):
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
    with pytest.raises(CoreException, match="add it to SearchSpec.fields"):
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


def test_resolve_pgroonga_index_alignment_coalesced_matches_bare() -> None:
    # A COALESCE-declared index must resolve to the same heap columns/weights
    # as the equivalent bare-column index, since Forze re-wraps every column
    # as coalesce(col::text, '') on the query side regardless.
    spec = SearchSpec(name="t", model_type=_Doc, fields=["b", "a"])
    field_map = {"a": "col_a", "b": "col_b"}
    eff_weights = {"a": 100, "b": 1}

    bare = resolve_pgroonga_index_alignment(
        spec,
        _info(expr="(ARRAY[col_a, col_b])"),
        field_map,
        eff_weights,
        index_qname=_IDX,
    )
    coalesced = resolve_pgroonga_index_alignment(
        spec,
        _info(expr="(ARRAY[COALESCE(col_a, ''::text), COALESCE(col_b, ''::text)])"),
        field_map,
        eff_weights,
        index_qname=_IDX,
    )

    assert coalesced == bare
    assert coalesced == (["col_a", "col_b"], [100, 1], True)

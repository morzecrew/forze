"""Unit coverage for ``coerce_query_ord_operands`` across every scalar family and node.

The seam casts a JSON *string* ordering bound to the targeted field's scalar family once,
keyed by the read model. The end-to-end Decimal path is exercised through the mock adapter
elsewhere; here we drive the caster directly so the temporal, date, UUID, boolean-node
recursion and array-element-quantifier branches are each pinned.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryField,
    QueryNot,
    QueryOr,
    coerce_query_ord_operands,
)
from forze.base.exceptions import CoreException

pytestmark = pytest.mark.unit


class _Line(BaseModel):
    qty: Decimal = Decimal("0")
    prior: list[Decimal] = []


class _Model(BaseModel):
    name: str
    price: Decimal = Decimal("0")
    created_at: datetime = datetime(2020, 1, 1, tzinfo=timezone.utc)
    born_on: date = date(2020, 1, 1)
    ref: UUID = UUID(int=0)
    tags: list[Decimal] = []
    matrix: list[list[Decimal]] = []
    lines: list[_Line] = []


def _f(name: str, value: object, op: str = "$gt") -> QueryField:
    return QueryField(name=name, op=op, value=value)  # type: ignore[arg-type]


def test_none_model_passes_through_unchanged() -> None:
    node = _f("price", "10")
    assert coerce_query_ord_operands(node, None) is node


def test_decimal_field_casts_string_to_decimal() -> None:
    out = coerce_query_ord_operands(_f("price", "10.5"), _Model)
    assert isinstance(out, QueryField) and out.value == Decimal("10.5")


def test_datetime_field_casts_string_and_forces_tz() -> None:
    out = coerce_query_ord_operands(_f("created_at", "2021-06-01T00:00:00"), _Model)
    assert isinstance(out, QueryField)
    assert isinstance(out.value, datetime) and out.value.tzinfo is not None


def test_date_field_casts_string_to_date() -> None:
    out = coerce_query_ord_operands(_f("born_on", "2021-06-01"), _Model)
    assert isinstance(out, QueryField)
    assert isinstance(out.value, date) and not isinstance(out.value, datetime)


def test_uuid_field_casts_string_to_uuid() -> None:
    u = UUID(int=42)
    out = coerce_query_ord_operands(_f("ref", str(u)), _Model)
    assert isinstance(out, QueryField) and out.value == u


def test_non_ordering_op_is_left_untouched() -> None:
    node = _f("price", "10", op="$eq")
    assert coerce_query_ord_operands(node, _Model) is node


def test_non_string_operand_is_left_untouched() -> None:
    node = _f("price", 10)
    assert coerce_query_ord_operands(node, _Model) is node


def test_unresolvable_field_passes_through() -> None:
    node = _f("missing", "10")
    assert coerce_query_ord_operands(node, _Model) is node


def test_string_field_is_left_untouched() -> None:
    node = _f("name", "abc")
    assert coerce_query_ord_operands(node, _Model) is node


def test_unparseable_decimal_string_is_refused() -> None:
    with pytest.raises(CoreException):
        coerce_query_ord_operands(_f("price", "not-a-number"), _Model)


def test_boolean_nodes_recurse_into_children() -> None:
    node = QueryAnd(
        (
            QueryOr((_f("price", "1"),)),
            QueryNot(_f("price", "2")),
        )
    )
    out = coerce_query_ord_operands(node, _Model)
    assert isinstance(out, QueryAnd)
    or_branch, not_branch = out.items
    assert isinstance(or_branch, QueryOr)
    inner = or_branch.items[0]
    assert isinstance(inner, QueryField) and inner.value == Decimal("1")
    assert isinstance(not_branch, QueryNot)
    negated = not_branch.item
    assert isinstance(negated, QueryField) and negated.value == Decimal("2")


def test_scalar_element_quantifier_casts_against_element_type() -> None:
    # tags: list[Decimal] — the sentinel "$" targets the scalar element itself.
    node = QueryElem(path="tags", quantifier="$any", inner=_f(ELEM_SCALAR_FIELD, "3.14"))
    out = coerce_query_ord_operands(node, _Model)
    assert isinstance(out, QueryElem)
    inner = out.inner
    assert isinstance(inner, QueryField) and inner.value == Decimal("3.14")


def test_object_element_quantifier_casts_nested_model_field() -> None:
    # lines: list[_Line] — the inner predicate targets a field of the element model.
    node = QueryElem(
        path="lines",
        quantifier="$all",
        inner=QueryAnd((_f("qty", "7"),)),
    )
    out = coerce_query_ord_operands(node, _Model)
    assert isinstance(out, QueryElem)
    inner = out.inner
    assert isinstance(inner, QueryAnd)
    leaf = inner.items[0]
    assert isinstance(leaf, QueryField) and leaf.value == Decimal("7")


def test_element_inner_or_not_recurse() -> None:
    node = QueryElem(
        path="tags",
        quantifier="$any",
        inner=QueryOr(
            (
                _f(ELEM_SCALAR_FIELD, "1"),
                QueryNot(_f(ELEM_SCALAR_FIELD, "2")),
            )
        ),
    )
    out = coerce_query_ord_operands(node, _Model)
    assert isinstance(out, QueryElem)
    or_inner = out.inner
    assert isinstance(or_inner, QueryOr)
    first, negated = or_inner.items
    assert isinstance(first, QueryField) and first.value == Decimal("1")
    assert isinstance(negated, QueryNot)
    leaf = negated.item
    assert isinstance(leaf, QueryField) and leaf.value == Decimal("2")


def test_nested_element_quantifier_casts_deeper_scalar() -> None:
    # lines[*].prior is list[Decimal]: an element quantifier nested under an object element.
    node = QueryElem(
        path="lines",
        quantifier="$any",
        inner=QueryElem(
            path="prior",
            quantifier="$any",
            inner=_f(ELEM_SCALAR_FIELD, "9"),
        ),
    )
    out = coerce_query_ord_operands(node, _Model)
    assert isinstance(out, QueryElem)
    nested = out.inner
    assert isinstance(nested, QueryElem)
    leaf = nested.inner
    assert isinstance(leaf, QueryField) and leaf.value == Decimal("9")


def test_scalar_of_scalar_array_casts_deepest_element() -> None:
    # matrix: list[list[Decimal]] — a scalar element quantifier ("$") nested under a
    # scalar element quantifier, so the element annotation is peeled twice.
    node = QueryElem(
        path="matrix",
        quantifier="$any",
        inner=QueryElem(
            path=ELEM_SCALAR_FIELD,
            quantifier="$any",
            inner=_f(ELEM_SCALAR_FIELD, "5"),
        ),
    )
    out = coerce_query_ord_operands(node, _Model)
    assert isinstance(out, QueryElem)
    nested = out.inner
    assert isinstance(nested, QueryElem)
    leaf = nested.inner
    assert isinstance(leaf, QueryField) and leaf.value == Decimal("5")


def test_uncastable_node_type_passes_through() -> None:
    # A field-to-field compare has no string operand to cast — returned unchanged.
    node = QueryCompare(left="price", op="$gt", right="tags")
    assert coerce_query_ord_operands(node, _Model) is node


def test_compare_node_inside_element_passes_through() -> None:
    # A non-field/non-quantifier node inside an element predicate is returned untouched.
    compare = QueryCompare(left=ELEM_SCALAR_FIELD, op="$gt", right=ELEM_SCALAR_FIELD)
    node = QueryElem(path="tags", quantifier="$any", inner=compare)
    out = coerce_query_ord_operands(node, _Model)
    assert isinstance(out, QueryElem) and out.inner is compare

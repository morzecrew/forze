"""Operator/field-type compatibility: nonsensical operator–field pairings fail clean.

A complement to capability validation — capabilities ask *can this backend compile the
operator*, these ask *does the operator fit the field's type*. ``$like`` on an int,
``$gt`` on a bool, a set operator on a scalar, a quantifier on a non-array: each is a
caller mistake rejected with a ``precondition`` (code ``query_operator_type_mismatch``),
never a runtime backend type error. Unresolvable types are skipped (never a false
rejection).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

import pytest
from pydantic import BaseModel, Field, StringConstraints

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    OPERATOR_TYPE_MISMATCH_CODE,
    QueryElem,
    QueryField,
    QueryFilterExpressionParser,
    TreePath,
    classify_field_type,
    coerce_query_ord_operands,
    field_value_operators,
    is_quantifiable_field,
    validate_query_field_types,
)
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit


class _Item(BaseModel):
    sku: str
    qty: int
    tags: list[str]


class _Doc(BaseModel):
    name: str
    age: int
    score: float
    active: bool
    created: datetime
    ref: UUID
    tags: list[str]
    nums: list[int]
    items: list[_Item]
    matrix: list[list[str]]  # scalar array-of-arrays
    path: TreePath  # materialized hierarchy path
    note: str | None = None


def _check(expr: dict, model: type[BaseModel] | None = _Doc, **kw) -> None:
    ast = QueryFilterExpressionParser.parse(expr)
    validate_query_field_types(ast, model, **kw)


# ....................... #


class TestCompatiblePass:
    @pytest.mark.parametrize(
        "expr",
        [
            {"$values": {"name": {"$like": "a%"}}},
            {"$values": {"name": {"$ilike": "a%"}}},
            {"$values": {"name": {"$regex": "^a"}}},
            {"$values": {"name": {"$in": ["a", "b"]}}},
            {"$values": {"age": {"$gt": 5}}},
            {"$values": {"age": {"$gte": 5, "$lt": 9}}},
            {"$values": {"score": {"$lte": 1.5}}},
            {"$values": {"created": {"$gt": datetime(2020, 1, 1)}}},
            {"$values": {"age": {"$in": [1, 2, 3]}}},
            {"$values": {"active": {"$eq": True}}},
            {"$values": {"active": {"$null": True}}},
            {"$values": {"tags": {"$superset": ["a", "b"]}}},
            {"$values": {"tags": {"$overlaps": ["a"]}}},
            {"$values": {"tags": {"$in": ["a"]}}},  # array membership = overlap
            {"$values": {"tags": {"$nin": ["a"]}}},  # = disjoint
            {"$values": {"nums": {"$empty": True}}},
            {"$values": {"tags": {"$any": "hot"}}},
            {"$values": {"nums": {"$all": {"$gte": 2}}}},
            {"$values": {"items": {"$any": {"$values": {"qty": {"$gte": 1}}}}}},
            {"$values": {"items": {"$any": {"$values": {"sku": {"$like": "a%"}}}}}},
            {"$values": {"items": {"$any": {"$values": {"tags": {"$any": "x"}}}}}},
            {"$fields": {"age": {"$lt": "score"}}},
        ],
    )
    def test_passes(self, expr: dict) -> None:
        _check(expr)


class TestIncompatibleRaise:
    @pytest.mark.parametrize(
        ("expr", "needle"),
        [
            ({"$values": {"age": {"$like": "x"}}}, "$like"),
            ({"$values": {"score": {"$regex": "x"}}}, "$regex"),
            ({"$values": {"active": {"$gt": True}}}, "$gt"),
            ({"$values": {"name": {"$gt": 5}}}, "$gt"),
            ({"$values": {"name": {"$superset": ["a"]}}}, "$superset"),
            ({"$values": {"age": {"$overlaps": [1]}}}, "$overlaps"),
            ({"$values": {"age": {"$empty": True}}}, "$empty"),
            ({"$values": {"name": {"$empty": True}}}, "$empty"),
            ({"$values": {"tags": {"$like": "x"}}}, "$like"),
        ],
    )
    def test_raises(self, expr: dict, needle: str) -> None:
        with pytest.raises(CoreException, match=r"\$\w+") as ei:
            _check(expr)

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE
        assert needle in str(ei.value)

    def test_quantifier_on_non_array_raises(self) -> None:
        with pytest.raises(CoreException, match="array") as ei:
            _check({"$values": {"name": {"$any": "x"}}})

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE

    def test_nested_scalar_op_mismatch_raises(self) -> None:
        # ``tags`` elements are strings — ``$like`` is fine, ``$superset`` is not.
        _check({"$values": {"items": {"$any": {"$values": {"tags": {"$any": {"$like": "h%"}}}}}}})

        with pytest.raises(CoreException) as ei:
            _check(
                {"$values": {"items": {"$any": {"$values": {"qty": {"$like": "x"}}}}}},
            )

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE

    def test_rejection_found_under_combinators(self) -> None:
        expr = {
            "$or": [
                {"$values": {"age": {"$gt": 1}}},
                {"$and": [{"$not": {"$values": {"age": {"$like": "x"}}}}]},
            ],
        }

        with pytest.raises(CoreException) as ei:
            _check(expr)

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE


class TestBestEffortSkips:
    def test_none_model_is_noop(self) -> None:
        # Nothing to resolve against — never raises.
        _check({"$values": {"age": {"$like": "x"}}}, model=None)

    def test_unknown_field_skipped(self) -> None:
        # Field existence is enforced elsewhere; an unresolved path is not type-checked.
        _check({"$values": {"ghost": {"$like": "x"}}})

    def test_optional_is_unwrapped(self) -> None:
        # ``note: str | None`` resolves to ``str`` — text ops allowed, set ops not.
        _check({"$values": {"note": {"$like": "x%"}}})

        with pytest.raises(CoreException) as ei:
            _check({"$values": {"note": {"$superset": ["a"]}}})

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE

    def test_field_type_hint_override(self) -> None:
        # A hint pins a type the model leaves ambiguous; here it forces a numeric leaf,
        # so a text op on it is rejected.
        with pytest.raises(CoreException) as ei:
            _check(
                {"$values": {"name": {"$like": "x"}}},
                field_type_hints={"name": int},
            )

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE

    def test_genuine_union_hint_is_skipped(self) -> None:
        # A real union (not just Optional) can't be classified → never a false rejection.
        _check({"$values": {"name": {"$gt": 1}}}, field_type_hints={"name": int | str})

    def test_unmodeled_generic_hint_is_skipped(self) -> None:
        from collections.abc import Iterator

        _check(
            {"$values": {"name": {"$gt": 1}}},
            field_type_hints={"name": Iterator[int]},
        )

    def test_opaque_type_hint_is_skipped(self) -> None:
        # A concrete type that maps to no coarse class (here ``object``) → unknown → skip.
        _check({"$values": {"name": {"$gt": 1}}}, field_type_hints={"name": object})

    def test_path_through_non_model_is_skipped(self) -> None:
        # ``name`` is a str; descending into ``name.sub`` can't resolve → not checked.
        _check({"$values": {"name.sub": {"$eq": 1}}})


class TestHierarchyPath:
    @pytest.mark.parametrize(
        "expr",
        [
            {"$values": {"path": {"$descendant_of": "top.science"}}},
            {"$values": {"path": {"$ancestor_of": "top.science.math"}}},
            {"$values": {"path": {"$descendant_of": ["top.science", "top.arts"]}}},
            # A TreePath is still a string — text/membership/equality stay valid on it.
            {"$values": {"path": {"$like": "top.%"}}},
            {"$values": {"path": {"$in": ["a", "b"]}}},
            {"$values": {"path": {"$eq": "top"}}},
        ],
    )
    def test_hierarchy_ops_pass_on_tree_path(self, expr: dict) -> None:
        _check(expr)

    @pytest.mark.parametrize("op", ["$descendant_of", "$ancestor_of"])
    def test_hierarchy_op_on_plain_string_rejected(self, op: str) -> None:
        # ``name`` is a plain str, not a TreePath — the hierarchy operators don't apply.
        with pytest.raises(CoreException, match=r"\$\w+") as ei:
            _check({"$values": {"name": {op: "top.science"}}})

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE
        assert op in str(ei.value)

    @pytest.mark.parametrize("op", ["$descendant_of", "$ancestor_of"])
    def test_hierarchy_op_on_number_rejected(self, op: str) -> None:
        with pytest.raises(CoreException) as ei:
            _check({"$values": {"age": {op: "top"}}})

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE


class TestScalarArrayOfArrays:
    def test_nested_scalar_quantifier_on_array_of_arrays_passes(self) -> None:
        # matrix is list[list[str]]; the inner element is a str array → valid.
        _check({"$values": {"matrix": {"$any": {"$any": "hot"}}}})
        _check({"$values": {"matrix": {"$all": {"$any": {"$like": "h%"}}}}})

    def test_inner_quantifier_on_non_array_element_rejected(self) -> None:
        # tags is list[str]; its element is a str, not an array → the inner quantifier
        # has nothing to range over.
        with pytest.raises(CoreException, match="requires an array") as ei:
            _check({"$values": {"tags": {"$any": {"$any": "x"}}}})

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE

    def test_op_mismatch_inside_array_of_arrays_rejected(self) -> None:
        # the deepest element is a str → ordering is invalid on it.
        with pytest.raises(CoreException) as ei:
            _check({"$values": {"matrix": {"$any": {"$any": {"$gt": 5}}}}})

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE


# ....................... #

Code = Annotated[str, StringConstraints(max_length=32)]
Qty = Annotated[Decimal, Field(ge=0)]
When = Annotated[datetime, Field()]
type CodeAlias = Annotated[str, StringConstraints(max_length=32)]


class _Constrained(BaseModel):
    code: Code | None = None
    qty: Qty | None = None
    when: When | None = None
    codes: list[Code] = []
    qtys: list[Qty] = []
    aliased: CodeAlias | None = None


class TestAnnotatedFields:
    """An ``Annotated`` field classifies as the type it wraps.

    Pydantic drops the metadata only from a bare top-level ``Annotated``: inside a union or
    a container it stays, so ``Code | None`` must not fall through to "unknown" and skip
    every check that a plain ``str | None`` gets.
    """

    @pytest.mark.parametrize(
        ("ann", "expected"),
        [
            (Code, "string"),
            (Code | None, "string"),
            (Annotated[str | None, Field()], "string"),
            (Annotated[Code | None, Field()] | None, "string"),
            (Qty | None, "number"),
            (When | None, "temporal"),
            (list[Code], "collection"),
            (CodeAlias | None, "string"),
            # A genuine union stays unresolved, metadata or not.
            (Annotated[int | str, Field()] | None, "unknown"),
        ],
    )
    def test_classifies_the_wrapped_type(self, ann: Any, expected: str) -> None:
        assert classify_field_type(ann) == expected

    def test_advertises_the_wrapped_types_operators(self) -> None:
        assert field_value_operators(Code | None) == field_value_operators(str)
        assert field_value_operators(Qty | None) == field_value_operators(Decimal)
        assert "$superset" not in field_value_operators(Code | None)
        assert is_quantifiable_field(list[Code]) is True
        assert is_quantifiable_field(Code | None) is False

    @pytest.mark.parametrize(
        "expr",
        [
            {"$values": {"code": {"$gt": "a"}}},
            {"$values": {"aliased": {"$gt": "a"}}},
            {"$values": {"qty": {"$like": "%1%"}}},
            {"$values": {"codes": {"$any": {"$gt": "a"}}}},
            {"$values": {"codes": {"$like": "a%"}}},
        ],
    )
    def test_refuses_an_operator_the_wrapped_type_does_not_take(self, expr: dict) -> None:
        with pytest.raises(CoreException) as ei:
            _check(expr, model=_Constrained)

        assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE

    @pytest.mark.parametrize(
        "expr",
        [
            {"$values": {"code": {"$like": "a%"}}},
            {"$values": {"qty": {"$gt": 5}}},
            {"$values": {"codes": {"$superset": ["a"]}}},
            {"$values": {"codes": {"$any": {"$like": "a%"}}}},
        ],
    )
    def test_accepts_what_the_wrapped_type_takes(self, expr: dict) -> None:
        _check(expr, model=_Constrained)

    def test_casts_a_string_bound_to_the_wrapped_type(self) -> None:
        qty = coerce_query_ord_operands(QueryField(name="qty", op="$gt", value="5"), _Constrained)
        when = coerce_query_ord_operands(
            QueryField(name="when", op="$gt", value="2021-06-01T00:00:00"), _Constrained
        )
        elem = coerce_query_ord_operands(
            QueryElem(
                path="qtys",
                quantifier="$any",
                inner=QueryField(name=ELEM_SCALAR_FIELD, op="$gt", value="5"),
            ),
            _Constrained,
        )

        assert isinstance(qty, QueryField) and qty.value == Decimal("5")
        assert isinstance(when, QueryField) and when.value == datetime(2021, 6, 1, tzinfo=UTC)
        assert isinstance(elem, QueryElem) and isinstance(elem.inner, QueryField)
        assert elem.inner.value == Decimal("5")

"""Operator/field-type compatibility: nonsensical operator–field pairings fail clean.

A complement to capability validation — capabilities ask *can this backend compile the
operator*, these ask *does the operator fit the field's type*. ``$like`` on an int,
``$gt`` on a bool, a set operator on a scalar, a quantifier on a non-array: each is a
caller mistake rejected with a ``precondition`` (code ``query_operator_type_mismatch``),
never a runtime backend type error. Unresolvable types are skipped (never a false
rejection).
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    OPERATOR_TYPE_MISMATCH_CODE,
    QueryFilterExpressionParser,
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
            ({"$values": {"tags": {"$in": ["a"]}}}, "$in"),
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

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
    TreePath,
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

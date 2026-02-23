"""Unit tests for postgres filter builder (build_filters)."""

from __future__ import annotations

import pytest

from forze.base.errors import ValidationError
from forze.infra.providers.postgres.builder import build_filters
from forze.infra.providers.postgres.introspect import PostgresType


# ----------------------- #
# Minimal type maps for tests


def _scalar_types(*fields: str) -> dict[str, PostgresType]:
    return {f: PostgresType(base="int4", is_array=False, not_null=True) for f in fields}


def _text_type(field: str) -> dict[str, PostgresType]:
    return {field: PostgresType(base="text", is_array=False, not_null=False)}


def _array_type(field: str, base: str = "text") -> dict[str, PostgresType]:
    return {field: PostgresType(base=base, is_array=True, not_null=False)}


def _ltree_type(field: str) -> dict[str, PostgresType]:
    return {field: PostgresType(base="ltree", is_array=False, not_null=False)}


# ----------------------- #
# Baseline regression tests (canonical names only) — Phase 2 / T003


class TestBuildFiltersCanonicalNames:
    """Assert build_filters output for filters using only canonical operator names."""

    def test_eq_canonical_returns_one_part_and_param(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters({"x": {"eq": 1}}, types=types)
        assert len(parts) == 1
        assert params == [1]

    def test_neq_canonical_returns_one_part_and_param(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters({"x": {"neq": 2}}, types=types)
        assert len(parts) == 1
        assert params == [2]

    def test_gte_canonical_returns_one_part_and_param(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters({"x": {"gte": 0}}, types=types)
        assert len(parts) == 1
        assert params == [0]

    def test_in_canonical_returns_one_part_and_params(self) -> None:
        types = _scalar_types("x")
        # Public canonical name is "in"; with types, IN becomes col = ANY(%s) so one list param
        parts, params = build_filters({"x": {"in": [1, 2, 3]}}, types=types)
        assert len(parts) == 1
        assert params == [[1, 2, 3]]

    def test_is_null_canonical_true_returns_one_part_no_param(self) -> None:
        types = _text_type("x")
        parts, params = build_filters({"x": {"is_null": True}}, types=types)
        assert len(parts) == 1
        assert params == []

    def test_or_canonical_two_branches_returns_one_part_two_params(self) -> None:
        types = _scalar_types("x")
        # Public canonical name is "or"
        parts, params = build_filters(
            {"x": {"or": [{"eq": 1}, {"eq": 2}]}}, types=types
        )
        assert len(parts) == 1
        assert params == [1, 2]

    def test_and_combination_two_ops_on_one_field_returns_one_part(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters({"x": {"gte": 1, "lte": 10}}, types=types)
        assert len(parts) == 1
        assert params == [1, 10]

    def test_empty_filters_returns_empty_lists(self) -> None:
        parts, params = build_filters(None)
        assert parts == []
        assert params == []

    def test_scalar_shortcut_treated_as_eq(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters({"x": 42}, types=types)
        assert len(parts) == 1
        assert params == [42]


# ----------------------- #
# US1: Invalid operator and alias rejection (T004, T005)


class TestInvalidOperatorRejection:
    """Invalid operator key raises ValidationError; message references canonical names."""

    def test_unknown_operator_raises_validation_error(self) -> None:
        types = _scalar_types("x")
        with pytest.raises(ValidationError) as exc_info:
            build_filters({"x": {"unknown_op": 1}}, types=types)
        assert (
            "eq" in str(exc_info.value).lower()
            or "expected" in str(exc_info.value).lower()
            or "canonical" in str(exc_info.value).lower()
        )

    def test_alias_ge_raises_validation_error_after_refactor(self) -> None:
        types = _scalar_types("x")
        with pytest.raises(ValidationError):
            build_filters({"x": {"ge": 1}}, types=types)

    def test_alias_double_equals_raises_validation_error_after_refactor(self) -> None:
        types = _scalar_types("x")
        with pytest.raises(ValidationError):
            build_filters({"x": {"==": 1}}, types=types)

    def test_alias_not_in_raises_validation_error_after_refactor(self) -> None:
        types = _scalar_types("x")
        with pytest.raises(ValidationError):
            build_filters({"x": {"not in": [1, 2]}}, types=types)

    def test_internal_name_in_raises_validation_error_public_is_in(self) -> None:
        types = _scalar_types("x")
        with pytest.raises(ValidationError):
            build_filters({"x": {"in_": [1, 2]}}, types=types)

    def test_internal_name_or_raises_validation_error_public_is_or(self) -> None:
        types = _scalar_types("x")
        with pytest.raises(ValidationError):
            build_filters({"x": {"or_": [{"eq": 1}]}}, types=types)


# ----------------------- #
# US2: Combined operators and OR chains (T009); operator families (T010)


class TestCombinedOperatorsAndOrChains:
    """AND on one field and OR chains produce correct SQL/params."""

    def test_and_three_ops_on_one_field(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters(
            {"x": {"gte": 1, "lte": 10, "neq": 5}}, types=types
        )
        assert len(parts) == 1
        assert 1 in params and 10 in params and 5 in params

    def test_or_three_branches(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters(
            {"x": {"or": [{"eq": 1}, {"eq": 2}, {"eq": 3}]}}, types=types
        )
        assert len(parts) == 1
        assert params == [1, 2, 3]


class TestOperatorFamilies:
    """Each operator family (scalar, in/not_in, is_null, array, or) works with canonical names."""

    def test_gt_lt(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters({"x": {"gt": 0, "lt": 100}}, types=types)
        assert len(parts) == 1
        assert params == [0, 100]

    def test_not_in(self) -> None:
        types = _scalar_types("x")
        parts, params = build_filters({"x": {"not_in": [0, -1]}}, types=types)
        assert len(parts) == 1
        assert len(params) == 1 and isinstance(params[0], list)

    def test_is_null_false(self) -> None:
        types = _text_type("x")
        parts, params = build_filters({"x": {"is_null": False}}, types=types)
        assert len(parts) == 1
        assert params == []

    def test_contains_array(self) -> None:
        types = _array_type("tags")
        parts, params = build_filters({"tags": {"contains": ["a", "b"]}}, types=types)
        assert len(parts) == 1
        assert params == [["a", "b"]]

    def test_empty_array_true(self) -> None:
        types = _array_type("tags")
        parts, params = build_filters({"tags": {"empty": True}}, types=types)
        assert len(parts) == 1
        assert params == []

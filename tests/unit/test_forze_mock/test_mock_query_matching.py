"""Unit tests for :mod:`forze_mock.adapters` in-memory filter matching."""

from typing import Any, cast
from uuid import UUID

import pytest

from forze.application.contracts.querying import QueryField
from forze_mock.query import _MISSING, _match_field, _path_get, _path_text

# ----------------------- #


def test_path_get_nested_and_missing() -> None:
    assert _path_get({"a": {"b": 2}}, "a.b") == 2
    assert _path_get({"a": {}}, "a.z") is _MISSING


def test_path_text_joins_sequences() -> None:
    assert _path_text({"t": ["x", "y"]}, "t") == "x y"
    assert _path_text({"t": None}, "t") == ""


@pytest.mark.parametrize(
    ("doc", "field", "expected"),
    [
        ({}, QueryField("k", "$eq", 1), False),
        ({"k": 1}, QueryField("k", "$eq", 1), True),
        ({}, QueryField("k", "$neq", 1), True),
        ({"k": 2}, QueryField("k", "$neq", 1), True),
    ],
)
def test_match_eq_neq_and_missing(
    doc: dict[str, object],
    field: QueryField,
    expected: bool,
) -> None:
    assert _match_field(doc, field) is expected


def test_match_ordering_and_type_error_falls_false() -> None:
    assert _match_field({"n": 2}, QueryField("n", "$gt", 1)) is True
    assert _match_field({"n": "x"}, QueryField("n", "$gt", 1)) is False
    assert _match_field({"n": 1}, QueryField("n", "$gte", 1)) is True
    assert _match_field({"n": 1}, QueryField("n", "$lt", 2)) is True
    assert _match_field({"n": 1}, QueryField("n", "$lte", 1)) is True


def test_match_ordering_decimal_nan_falls_false_instead_of_raising() -> None:
    # Decimal NaN ordered comparisons raise InvalidOperation (an ArithmeticError,
    # not a TypeError) where float NaN merely returns False. Incomparable must read
    # as "no match" on every path, never escape as an uncaught 500. The coercion
    # seam refuses non-finite *operands*, so the live case is a stored row value.
    from decimal import Decimal

    nan = Decimal("NaN")

    for op in ("$gt", "$gte", "$lt", "$lte"):
        assert _match_field({"n": nan}, QueryField("n", op, Decimal("5"))) is False
        assert _match_field({"n": Decimal("5")}, QueryField("n", op, nan)) is False


def test_match_null_and_empty() -> None:
    assert _match_field({}, QueryField("x", "$null", True)) is True
    assert _match_field({"x": None}, QueryField("x", "$null", True)) is True
    assert _match_field({"x": 1}, QueryField("x", "$null", True)) is False
    assert _match_field({"x": 1}, QueryField("x", "$null", False)) is True
    assert _match_field({}, QueryField("x", "$empty", True)) is False
    assert _match_field({"x": []}, QueryField("x", "$empty", True)) is True
    assert _match_field({"x": [1]}, QueryField("x", "$empty", False)) is True


def test_match_in_nin_membership() -> None:
    assert _match_field({"t": "a"}, QueryField("t", "$in", ["a", "b"])) is True
    assert _match_field({"t": ["a", "c"]}, QueryField("t", "$in", ["a"])) is True
    assert _match_field({}, QueryField("t", "$in", ["a"])) is False
    assert _match_field({"t": "z"}, QueryField("t", "$nin", ["a"])) is True


def test_match_set_ops() -> None:
    assert _match_field({"s": [1, 2, 3]}, QueryField("s", "$superset", [1, 2])) is True
    assert _match_field({"s": [1]}, QueryField("s", "$subset", [1, 2])) is True
    assert _match_field({"s": [1, 2]}, QueryField("s", "$disjoint", [3, 4])) is True
    assert _match_field({"s": [1, 2]}, QueryField("s", "$overlaps", [2, 9])) is True
    assert _match_field({}, QueryField("s", "$disjoint", [1])) is True


def test_match_eq_uuid_coercion() -> None:
    u = UUID("33333333-3333-3333-3333-333333333333")
    assert _match_field({"id": u}, QueryField("id", "$eq", str(u))) is True


def test_match_text_patterns() -> None:
    assert _match_field({"t": "Roadmap"}, QueryField("t", "$ilike", "%road%")) is True
    assert _match_field({"t": "other"}, QueryField("t", "$ilike", "%road%")) is False
    assert _match_field({}, QueryField("t", "$ilike", "%road%")) is False
    assert _match_field({"t": "foo"}, QueryField("t", "$regex", "^f")) is True

    # A present ``None`` (not just a missing field) never matches a text pattern: ``str(None)`` would
    # otherwise become ``"None"`` and spuriously match — e.g. ``%on%`` or ``.*`` (a backend's NULL
    # matches no pattern).
    assert _match_field({"t": None}, QueryField("t", "$like", "%on%")) is False
    assert _match_field({"t": None}, QueryField("t", "$regex", ".*")) is False
    assert _match_field({"t": None}, QueryField("t", "$ilike", "%n%")) is False


def test_match_descendant_of_label_aware_inclusive() -> None:
    f = QueryField("p", "$descendant_of", "top.science")

    # at or below the node, on label boundaries
    assert _match_field({"p": "top.science"}, f) is True  # inclusive
    assert _match_field({"p": "top.science.math"}, f) is True
    assert _match_field({"p": "top.science.math.algebra"}, f) is True

    # not below it
    assert _match_field({"p": "top"}, f) is False  # an ancestor, not a descendant
    assert _match_field({"p": "top.arts"}, f) is False
    # label-boundary: "scientist" is not the "science" label
    assert _match_field({"p": "top.scientist"}, f) is False
    assert _match_field({}, f) is False


def test_match_ancestor_of_label_aware_inclusive() -> None:
    f = QueryField("p", "$ancestor_of", "top.science.math")

    # at or above the node
    assert _match_field({"p": "top.science.math"}, f) is True  # inclusive
    assert _match_field({"p": "top.science"}, f) is True
    assert _match_field({"p": "top"}, f) is True

    # not above it
    assert _match_field({"p": "top.science.math.algebra"}, f) is False  # below
    assert _match_field({"p": "top.arts"}, f) is False
    assert _match_field({"p": "top.scien"}, f) is False  # not a label prefix
    assert _match_field({}, f) is False


def test_unknown_operator_falls_through_without_match() -> None:
    """Unsupported ops are not handled by the mock matcher (no default case)."""
    assert _match_field({"a": 1}, QueryField("a", cast(Any, "$nope"), 1)) is None

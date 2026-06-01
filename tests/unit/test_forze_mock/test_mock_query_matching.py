"""Unit tests for :mod:`forze_mock.adapters` in-memory filter matching."""

from typing import Any, cast
from uuid import UUID

import pytest

from forze.application.contracts.querying import QueryField
from forze_mock.adapters import _MISSING, _match_field, _path_get, _path_text

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


def test_unknown_operator_falls_through_without_match() -> None:
    """Unsupported ops are not handled by the mock matcher (no default case)."""
    assert _match_field({"a": 1}, QueryField("a", cast(Any, "$nope"), 1)) is None

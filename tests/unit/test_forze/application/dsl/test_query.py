"""Unit tests for forze.application.dsl.query."""

import pytest

from forze.application.contracts.query.dsl import FilterExpressionParser, ValueCaster

# ----------------------- #


class TestValueCaster:
    """Tests for ValueCaster."""

    def test_as_bool_true(self) -> None:
        assert ValueCaster.as_bool(True) is True
        assert ValueCaster.as_bool(1) is True
        assert ValueCaster.as_bool("true") is True

    def test_as_bool_false(self) -> None:
        assert ValueCaster.as_bool(False) is False
        assert ValueCaster.as_bool(0) is False
        assert ValueCaster.as_bool("false") is False

    def test_as_bool_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid boolean"):
            ValueCaster.as_bool("invalid")

    def test_as_int(self) -> None:
        assert ValueCaster.as_int(42) == 42
        assert ValueCaster.as_int("42") == 42

    def test_as_uuid(self) -> None:
        from uuid import UUID

        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert ValueCaster.as_uuid(str(u)) == u


class TestFilterExpressionParser:
    """Tests for FilterExpressionParser."""

    def test_parse_simple_predicate(self) -> None:
        expr = {"$fields": {"name": "foo"}}
        result = FilterExpressionParser.parse(expr)
        assert result is not None

    def test_parse_conjunction(self) -> None:
        expr = {"$and": [{"$fields": {"a": 1}}, {"$fields": {"b": 2}}]}
        result = FilterExpressionParser.parse(expr)
        assert result is not None

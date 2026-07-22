"""Unit tests for forze.application.dsl.query."""

import pytest

from forze.application.contracts.querying import (
    QueryFilterExpressionParser,
    QueryValueCaster,
)
from forze.base.exceptions import CoreException

# ----------------------- #

class TestValueCaster:
    """Tests for ValueCaster."""

    def test_as_bool_true(self) -> None:
        assert QueryValueCaster.as_bool(True) is True
        assert QueryValueCaster.as_bool(1) is True
        assert QueryValueCaster.as_bool("true") is True

    def test_as_bool_false(self) -> None:
        assert QueryValueCaster.as_bool(False) is False
        assert QueryValueCaster.as_bool(0) is False
        assert QueryValueCaster.as_bool("false") is False

    def test_as_bool_invalid_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid boolean"):
            QueryValueCaster.as_bool("invalid")

    def test_as_int(self) -> None:
        assert QueryValueCaster.as_int(42) == 42
        assert QueryValueCaster.as_int("42") == 42

    def test_as_uuid(self) -> None:
        from uuid import UUID

        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert QueryValueCaster.as_uuid(str(u)) == u

    def test_as_decimal_rejects_non_finite(self) -> None:
        # "NaN"/"Infinity" parse as Decimal but are not filter operands: Postgres
        # sorts 'NaN'::numeric above every number (a $lt bound fails open), while
        # the in-memory Decimal comparison raises InvalidOperation.
        from decimal import Decimal

        for value in ("NaN", "nan", "sNaN", "Infinity", "-inf", Decimal("NaN"), float("nan")):
            with pytest.raises(CoreException, match="Non-finite"):
                QueryValueCaster.as_decimal(value)

    def test_as_float_rejects_non_finite(self) -> None:
        for value in ("nan", "inf", "-Infinity", float("nan"), float("inf")):
            with pytest.raises(CoreException, match="Non-finite"):
                QueryValueCaster.as_float(value)

class TestFilterExpressionParser:
    """Tests for FilterExpressionParser."""

    def test_parse_simple_predicate(self) -> None:
        expr = {"$values": {"name": "foo"}}
        result = QueryFilterExpressionParser.parse(expr)
        assert result is not None

    def test_parse_conjunction(self) -> None:
        expr = {"$and": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]}
        result = QueryFilterExpressionParser.parse(expr)
        assert result is not None

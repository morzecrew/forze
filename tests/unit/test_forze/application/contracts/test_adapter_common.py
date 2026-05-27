"""Tests for shared analytics adapter helpers."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.contracts.analytics._adapter_common import (
    dry_run_enabled,
    dry_run_offset_page,
    pagination_window,
    parse_count_row,
    shape_rows,
    timeout_seconds,
    validated_params,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict

# ----------------------- #


class _Params(BaseModel):
    day: str


class _Row(BaseModel):
    value: int


def _spec() -> AnalyticsSpec[_Row, _Row]:
    return AnalyticsSpec(
        name="metrics",
        read=_Row,
        queries={"daily": AnalyticsQueryDefinition(params=_Params)},
    )


class TestValidatedParams:
    def test_returns_same_instance_when_types_match(self) -> None:
        params = _Params(day="2026-01-01")
        out = validated_params(_spec(), "daily", params)
        assert out is params

    def test_unknown_query_key_raises(self) -> None:
        with pytest.raises(CoreException, match="Unknown analytics query"):
            validated_params(_spec(), "missing", _Params(day="x"))

    def test_other_model_dump_validated(self) -> None:
        class Wrapper(BaseModel):
            day: str

        out = validated_params(_spec(), "daily", Wrapper(day="2026-01-01"))
        assert isinstance(out, _Params)
        assert out.day == "2026-01-01"

    def test_non_model_params_raises(self) -> None:
        with pytest.raises(CoreException, match="Pydantic model"):
            validated_params(_spec(), "daily", 42)  # type: ignore[arg-type]


class TestRunOptionsHelpers:
    def test_dry_run_enabled(self) -> None:
        assert dry_run_enabled({"dry_run": True}) is True
        assert dry_run_enabled(None) is False

    def test_timeout_seconds(self) -> None:
        assert timeout_seconds(None) is None
        assert timeout_seconds({"timeout": timedelta(seconds=30)}) == 30

    def test_pagination_window(self) -> None:
        assert pagination_window({"limit": 10, "offset": 5}) == (10, 5)
        assert pagination_window(None) == (None, None)


class TestShapeRows:
    def test_return_fields_projection(self) -> None:
        rows: list[JsonDict] = [{"a": 1, "b": 2}]
        out = shape_rows(rows, read_type=_Row, return_type=None, return_fields=("a",))
        assert out == [{"a": 1}]

    def test_return_type_validation(self) -> None:
        rows: list[JsonDict] = [{"value": 3}]
        out = shape_rows(rows, read_type=_Row, return_type=_Row, return_fields=None)
        assert len(out) == 1
        assert out[0].value == 3


class TestDryRunAndCount:
    def test_dry_run_offset_page_with_count(self) -> None:
        page = dry_run_offset_page({"limit": 5, "offset": 0}, return_count=True)
        assert page.count == 0
        assert page.hits == []

    def test_parse_count_row(self) -> None:
        assert parse_count_row([]) == 0
        assert parse_count_row([{"forze_cnt": 42}]) == 42

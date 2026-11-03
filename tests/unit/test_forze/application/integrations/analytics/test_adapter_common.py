"""Tests for shared analytics adapter helpers."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.integrations.analytics.adapter_common import (
    dry_run_enabled,
    dry_run_offset_page,
    pagination_window,
    parse_count_row,
    route_timeout,
    shape_rows,
    validate_route_timeout,
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

    def test_dry_run_enabled_refuses_an_option_the_type_does_not_define(self) -> None:
        with pytest.raises(CoreException, match="Unknown keys in AnalyticsRunOptions: dryrun, max_row"):
            dry_run_enabled({"dryrun": True, "max_row": 1, "timeout": timedelta(seconds=1)})  # type: ignore[typeddict-unknown-key]

    def test_route_timeout_applies_the_route_ceiling_without_an_option(self) -> None:
        ceiling = timedelta(seconds=60)

        assert route_timeout(None, ceiling) == ceiling
        assert route_timeout({}, ceiling) == ceiling
        assert route_timeout({"max_rows": 5}, ceiling) == ceiling

    def test_route_timeout_clamps_down_never_up(self) -> None:
        ceiling = timedelta(seconds=60)

        assert route_timeout({"timeout": timedelta(seconds=5)}, ceiling) == timedelta(seconds=5)
        assert route_timeout({"timeout": timedelta(minutes=10)}, ceiling) == ceiling

    @pytest.mark.parametrize("bad", [timedelta(0), timedelta(seconds=-1), 30])
    def test_route_timeout_refuses_a_non_positive_option(self, bad: object) -> None:
        with pytest.raises(CoreException) as caught:
            route_timeout({"timeout": bad}, timedelta(seconds=60))  # type: ignore[typeddict-item]

        assert caught.value.code == "analytics_option_invalid"

    def test_a_timeout_must_be_whole_units_of_the_engine_resolution(self) -> None:
        second = timedelta(seconds=1)

        validate_route_timeout(timedelta(seconds=3), resolution=second)

        for bad in (timedelta(seconds=1.5), timedelta(milliseconds=500)):
            with pytest.raises(CoreException, match="analytics_timeout_invalid"):
                validate_route_timeout(bad, resolution=second)

            with pytest.raises(CoreException, match="analytics_option_invalid"):
                route_timeout({"timeout": bad}, timedelta(seconds=60), resolution=second)

    def test_a_timeout_above_the_engine_maximum_is_refused(self) -> None:
        with pytest.raises(CoreException, match="analytics_timeout_invalid"):
            validate_route_timeout(timedelta(days=30), maximum=timedelta(days=24))

    @pytest.mark.parametrize("bad", [timedelta(0), timedelta(seconds=-1), None, 60])
    def test_validate_route_timeout_refuses_anything_but_a_positive_timedelta(
        self, bad: object
    ) -> None:
        with pytest.raises(CoreException) as caught:
            validate_route_timeout(bad)

        assert caught.value.code == "analytics_timeout_invalid"

    def test_pagination_window(self) -> None:
        assert pagination_window({"limit": 10, "offset": 5}) == (10, 5)
        assert pagination_window(None) == (None, None)


class TestShapeRows:
    def test_return_fields_projection(self) -> None:
        rows: list[JsonDict] = [{"a": 1, "b": 2}]
        spec = _spec()
        out = shape_rows(
            rows,
            read_codec=spec.read_codec,
            read_type=_Row,
            return_type=None,
            return_fields=("a",),
        )
        assert out == [{"a": 1}]

    def test_return_type_validation(self) -> None:
        rows: list[JsonDict] = [{"value": 3}]
        spec = _spec()
        out = shape_rows(
            rows,
            read_codec=spec.read_codec,
            read_type=_Row,
            return_type=_Row,
            return_fields=None,
        )
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

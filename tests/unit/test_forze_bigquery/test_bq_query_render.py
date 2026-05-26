"""Tests for BigQuery query request builders."""

from __future__ import annotations

from pydantic import BaseModel

from forze_bigquery.kernel.platform.query import (
    build_count_sql,
    build_sync_query_request,
    params_to_query_parameters,
)


class _Params(BaseModel):
    day: str
    n: int = 1


def test_params_to_query_parameters() -> None:
    params = _Params(day="2026-01-01", n=2)
    qps = params_to_query_parameters(params)
    assert len(qps) == 2
    assert qps[0]["name"] == "day"
    assert qps[0]["parameterType"] == {"type": "STRING"}


def test_build_sync_query_request_named_params() -> None:
    body = build_sync_query_request(
        "SELECT @day",
        query_parameters=params_to_query_parameters(_Params(day="x")),
        dry_run=True,
        maximum_bytes_billed=1_000_000,
    )
    assert body["dryRun"] is True
    assert body["parameterMode"] == "NAMED"
    assert body["maximumBytesBilled"] == "1000000"


def test_build_count_sql_wraps_inner() -> None:
    sql = build_count_sql("SELECT 1 AS value WHERE day = @day")
    assert "COUNT(*)" in sql
    assert "forze_analytics_subq" in sql

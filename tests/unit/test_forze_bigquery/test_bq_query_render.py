"""Tests for BigQuery query request builders."""

from __future__ import annotations

from pydantic import BaseModel

from forze_bigquery.kernel.client.query import (
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


class _ArrayParams(BaseModel):
    ids: list[int] = []
    tags: list[str] = []
    opt: int | None = None


def test_empty_array_param_carries_typed_array_from_annotation() -> None:
    # Regression: an empty list must still emit ``arrayType`` (BigQuery 400s
    # on an ARRAY parameter without it), derived from the field annotation.
    qps = {q["name"]: q for q in params_to_query_parameters(_ArrayParams())}

    assert qps["ids"]["parameterType"] == {
        "type": "ARRAY",
        "arrayType": {"type": "INT64"},
    }
    assert qps["ids"]["parameterValue"] == {"arrayValues": []}
    assert qps["tags"]["parameterType"]["arrayType"] == {"type": "STRING"}


def test_none_param_typed_from_annotation_not_string() -> None:
    # Regression: ``None`` for an ``int | None`` field is typed INT64, not STRING.
    qps = {q["name"]: q for q in params_to_query_parameters(_ArrayParams())}
    assert qps["opt"]["parameterType"] == {"type": "INT64"}
    assert qps["opt"]["parameterValue"] == {"value": None}


def test_non_empty_array_param_emits_values() -> None:
    qps = {
        q["name"]: q
        for q in params_to_query_parameters(_ArrayParams(ids=[1, 2], tags=["a"]))
    }
    assert qps["ids"]["parameterType"]["arrayType"] == {"type": "INT64"}
    assert qps["ids"]["parameterValue"] == {
        "arrayValues": [{"value": "1"}, {"value": "2"}],
    }


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


def test_params_supports_common_scalar_types() -> None:
    from datetime import date, datetime
    from decimal import Decimal
    from uuid import UUID

    class _All(BaseModel):
        flag: bool
        count: int
        ratio: float
        amount: Decimal
        when: datetime
        day: date
        uid: UUID
        label: str
        items: list[int]

    qps = params_to_query_parameters(
        _All(
            flag=True,
            count=1,
            ratio=1.5,
            amount=Decimal("1.0"),
            when=datetime(2026, 1, 1, tzinfo=__import__("datetime").timezone.utc),
            day=date(2026, 1, 1),
            uid=UUID("00000000-0000-0000-0000-000000000001"),
            label="x",
            items=[1, 2],
        ),
    )
    types = {p["parameterType"]["type"] for p in qps}
    assert types >= {"BOOL", "INT64", "FLOAT64", "NUMERIC", "TIMESTAMP", "DATE", "STRING", "ARRAY"}


def test_build_sync_query_request_pagination_fields() -> None:
    body = build_sync_query_request(
        "SELECT 1",
        max_results=10,
        start_index=5,
        page_token="tok",
        timeout_ms=30_000,
    )
    assert body["maxResults"] == 10
    assert body["startIndex"] == "5"
    assert body["pageToken"] == "tok"
    assert body["timeoutMs"] == 30_000

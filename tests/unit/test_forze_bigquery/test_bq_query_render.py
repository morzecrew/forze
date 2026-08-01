"""Tests for BigQuery query request builders."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException, ExceptionKind
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


class _BytesParams(BaseModel):
    blob: bytes
    blobs: list[bytes] = []


def test_bytes_param_typed_and_base64_encoded() -> None:
    qps = {q["name"]: q for q in params_to_query_parameters(_BytesParams(blob=b"hi"))}
    assert qps["blob"]["parameterType"] == {"type": "BYTES"}
    assert qps["blob"]["parameterValue"] == {"value": "aGk="}  # base64("hi")


def test_bytes_array_param_typed_and_encoded() -> None:
    qps = {
        q["name"]: q
        for q in params_to_query_parameters(_BytesParams(blob=b"x", blobs=[b"hi"]))
    }
    assert qps["blobs"]["parameterType"] == {
        "type": "ARRAY",
        "arrayType": {"type": "BYTES"},
    }
    assert qps["blobs"]["parameterValue"] == {"arrayValues": [{"value": "aGk="}]}


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


class TestDecimalParameterType:
    """NUMERIC vs BIGNUMERIC is picked from the Decimal's own exponent/digits.

    NUMERIC holds 9 fractional and 29 integer digits; a finer or wider value sent as
    NUMERIC would be rounded server-side — a silently changed money value. The choice
    is by numeric value (normalized), non-finite values are refused at the seam like
    every other backend, and a value even BIGNUMERIC cannot hold exactly is refused
    rather than rounded.
    """

    @staticmethod
    def _one(value: Decimal) -> dict:  # type: ignore[type-arg]
        class _P(BaseModel):
            amount: Decimal

        (qp,) = params_to_query_parameters(_P(amount=value))
        return qp

    def test_in_range_decimal_stays_numeric(self) -> None:
        qp = self._one(Decimal("123.45"))
        assert qp["parameterType"] == {"type": "NUMERIC"}

        # 29 integer digits and scale 9 — the NUMERIC corner, still NUMERIC.
        corner = Decimal("9" * 29 + "." + "9" * 9)
        assert self._one(corner)["parameterType"] == {"type": "NUMERIC"}

    def test_fine_scale_picks_bignumeric(self) -> None:
        qp = self._one(Decimal("0.1234567890123"))  # scale 13 > 9
        assert qp["parameterType"] == {"type": "BIGNUMERIC"}
        assert qp["parameterValue"] == {"value": "0.1234567890123"}

    def test_wide_magnitude_picks_bignumeric(self) -> None:
        qp = self._one(Decimal("1e30"))  # 31 integer digits > 29
        assert qp["parameterType"] == {"type": "BIGNUMERIC"}

    def test_bignumeric_bound_is_the_exact_maximum_not_a_digit_count(self) -> None:
        # The largest BIGNUMERIC values have 39 integer digits (leading 5.78…): a
        # digit-count cap would wrongly refuse them. The exact maximum is accepted;
        # one smallest step above it is refused.
        from forze_bigquery.kernel.client.query import _BIGNUMERIC_MAX_ABS

        assert self._one(Decimal("4e38"))["parameterType"] == {"type": "BIGNUMERIC"}
        assert self._one(_BIGNUMERIC_MAX_ABS)["parameterType"] == {"type": "BIGNUMERIC"}
        assert self._one(-_BIGNUMERIC_MAX_ABS)["parameterType"] == {"type": "BIGNUMERIC"}

        # One smallest representable step above the maximum — computed under a wide
        # local context, because default-context arithmetic (prec=28) would round the
        # 77-digit sum straight back below the bound (the same context trap the
        # implementation avoids by never using ``normalize()``).
        from decimal import localcontext

        with localcontext() as decimal_ctx:
            decimal_ctx.prec = 100
            above_max = _BIGNUMERIC_MAX_ABS + Decimal("1E-38")

        with pytest.raises(CoreException) as ei:
            self._one(above_max)
        assert ei.value.kind is ExceptionKind.PRECONDITION

    def test_decimal_value_serializes_fixed_point_never_scientific(self) -> None:
        qp = self._one(Decimal("5E+3"))
        assert qp["parameterValue"] == {"value": "5000"}
        assert self._one(Decimal("0.00100"))["parameterValue"] == {"value": "0.00100"}

    def test_choice_is_by_value_not_spelling(self) -> None:
        # Twelve trailing zeros are still the value 1 — scale 0, NUMERIC.
        assert self._one(Decimal("1.000000000000"))["parameterType"] == {"type": "NUMERIC"}

    def test_zero_is_numeric_regardless_of_spelling(self) -> None:
        # Zero with any exponent is representable exactly — the fast path, so a
        # wide-exponent spelling never trips the scale check.
        assert self._one(Decimal("0"))["parameterType"] == {"type": "NUMERIC"}
        assert self._one(Decimal("0E-100"))["parameterType"] == {"type": "NUMERIC"}

    def test_unannotated_decimal_list_infers_from_sample_and_widens(self) -> None:
        # Raw-dict path: no annotation, so the first element infers the array type —
        # and the widening scan must still cover every element past the sample.
        (qp,) = params_to_query_parameters({"amounts": [Decimal("1.5"), Decimal("1e30")]})
        assert qp["parameterType"] == {"type": "ARRAY", "arrayType": {"type": "BIGNUMERIC"}}

    def test_non_finite_decimal_refused(self) -> None:
        # Reachable through the raw-dict path only — pydantic already refuses a
        # non-finite Decimal on a typed model field; the dict form has no such
        # guard, so the seam must hold the property itself.
        for bad in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
            with pytest.raises(CoreException) as ei:
                params_to_query_parameters({"amount": bad})
            assert ei.value.kind is ExceptionKind.PRECONDITION

    def test_beyond_bignumeric_refused_not_rounded(self) -> None:
        for bad in (Decimal("1e40"), Decimal("1e-40")):
            with pytest.raises(CoreException) as ei:
                self._one(bad)
            assert ei.value.kind is ExceptionKind.PRECONDITION

    def test_array_widens_to_the_neediest_element(self) -> None:
        class _P(BaseModel):
            amounts: list[Decimal]

        # First element fits NUMERIC; the second needs BIGNUMERIC — the array
        # type must hold every element, whether inferred from the annotation
        # or from the first sample.
        (qp,) = params_to_query_parameters(
            _P(amounts=[Decimal("1.5"), Decimal("0.1234567890123")])
        )
        assert qp["parameterType"] == {
            "type": "ARRAY",
            "arrayType": {"type": "BIGNUMERIC"},
        }

    def test_empty_decimal_array_keeps_annotation_numeric(self) -> None:
        class _P(BaseModel):
            amounts: list[Decimal] = []

        (qp,) = params_to_query_parameters(_P())
        assert qp["parameterType"] == {"type": "ARRAY", "arrayType": {"type": "NUMERIC"}}


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

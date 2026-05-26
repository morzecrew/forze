"""Unit tests for analytics contract specs."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    validate_analytics_spec,
)

# ----------------------- #


class _Row(BaseModel):
    value: int = 0


class _Params(BaseModel):
    day: str = "2026-01-01"


class _IngestRow(BaseModel):
    event: str = "click"


def _minimal_spec() -> AnalyticsSpec[_Row, _IngestRow]:
    return AnalyticsSpec(
        name="metrics",
        read=_Row,
        queries={
            "daily": AnalyticsQueryDefinition(params=_Params),
        },
        ingest=_IngestRow,
    )


class TestAnalyticsSpec:
    def test_minimal_spec(self) -> None:
        spec = _minimal_spec()
        assert spec.read is _Row
        assert "daily" in spec.queries

    def test_empty_queries_raise(self) -> None:
        with pytest.raises(exc.internal, match="at least one"):
            AnalyticsSpec(
                name="m",
                read=_Row,
                queries={},
            )

    def test_duplicate_query_keys_raise(self) -> None:
        q = AnalyticsQueryDefinition(params=_Params)

        class _DupKeys(Mapping[str, AnalyticsQueryDefinition]):
            def __getitem__(self, key: str) -> AnalyticsQueryDefinition:
                return q

            def __iter__(self) -> Iterator[str]:
                yield from ("a", "a")

            def __len__(self) -> int:
                return 2

        with pytest.raises(exc.internal, match="Duplicate"):
            AnalyticsSpec(name="m", read=_Row, queries=_DupKeys())

    def test_invalid_params_type_raises(self) -> None:
        with pytest.raises(exc.internal, match="BaseModel"):
            AnalyticsSpec(
                name="m",
                read=_Row,
                queries={"q": AnalyticsQueryDefinition(params=str)},  # type: ignore[arg-type]
            )

    def test_invalid_ingest_type_raises(self) -> None:
        with pytest.raises(exc.internal, match="ingest"):
            AnalyticsSpec(
                name="m",
                read=_Row,
                queries={"q": AnalyticsQueryDefinition(params=_Params)},
                ingest=str,  # type: ignore[arg-type]
            )


class TestValidateAnalyticsSpec:
    def test_validate_accepts_minimal(self) -> None:
        validate_analytics_spec(_minimal_spec())

"""Pytest configuration and fixtures for forze_duckdb unit tests.

DuckDB is in-process and Docker-free, so these tests exercise a *real* engine over
a small local Parquet fixture rather than a mock.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncIterator, Iterator

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

import duckdb
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze_duckdb import (
    DuckDbAnalyticsConfig,
    DuckDbClient,
    DuckDbConfig,
    DuckDbQueryConfig,
)
from forze_duckdb.adapters import DuckDbAnalyticsAdapter

# ----------------------- #


class Row(BaseModel):
    """Read model for the events fixture."""

    day: str
    total: int


class Params(BaseModel):
    """Query params for the events fixture."""

    min_total: int = 0


# ....................... #


@pytest.fixture
def events_parquet(tmp_path: Path) -> str:
    """Write a tiny deterministic events Parquet file and return its path."""

    path = tmp_path / "events.parquet"
    duckdb.connect().execute(
        "COPY (SELECT * FROM (VALUES ('a', 10), ('b', 20), ('c', 30), ('d', 40)) "
        "t(day, total)) "
        f"TO '{path}' (FORMAT parquet)"
    )

    return str(path)


# ....................... #


@pytest.fixture
def events_spec() -> AnalyticsSpec[Row, Any]:
    """An analytics spec with a single ``by_day`` query."""

    return AnalyticsSpec(
        name="events",
        read=Row,
        queries={"by_day": AnalyticsQueryDefinition(params=Params)},
    )


# ....................... #


@pytest.fixture
async def client() -> AsyncIterator[DuckDbClient]:
    """An initialized in-memory DuckDB client (no network extensions)."""

    c = DuckDbClient()
    await c.initialize(":memory:", config=DuckDbConfig(), extensions=())

    try:
        yield c

    finally:
        await c.close()


# ....................... #


@pytest.fixture
def make_adapter(
    client: DuckDbClient,
    events_spec: AnalyticsSpec[Row, Any],
) -> Iterator[Any]:
    """Factory building a query-bound adapter from a per-query SQL string."""

    def _make(sql: str, *, skip_total: bool = False) -> DuckDbAnalyticsAdapter[Row]:
        config = DuckDbAnalyticsConfig(
            queries={"by_day": DuckDbQueryConfig(sql=sql, skip_total=skip_total)},
        )
        config.validate_against_spec(events_spec)

        return DuckDbAnalyticsAdapter(
            client=client,
            spec=events_spec,
            config=config,
        )

    yield _make

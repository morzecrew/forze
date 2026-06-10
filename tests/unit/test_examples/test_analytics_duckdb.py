"""Analytics-over-a-data-lake recipe — DuckDB queries a local Parquet file (no Docker)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

from examples.recipes.analytics_duckdb.app import (
    SALES_SPEC,
    _write_sample_lake,
    build_runtime,
    top_regions,
)


async def test_named_query_aggregates_the_lake() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        parquet = _write_sample_lake(Path(tmp))
        runtime = build_runtime(parquet)

        async with runtime.scope():
            ctx = runtime.get_context()

            # min_total=20 keeps us (30+20=50) and drops eu (15) and apac (8).
            rows = await top_regions(ctx, min_total=20)
            assert [(r.region, r.total) for r in rows] == [("us", 50)]

            # No floor → every region, sorted; proves the read model + ordering.
            allrows = await top_regions(ctx, min_total=0)
            assert [(r.region, r.total) for r in allrows] == [
                ("apac", 8),
                ("eu", 15),
                ("us", 50),
            ]


def test_spec_exposes_only_query_and_output_type() -> None:
    # The handler-facing surface is the query key + read model — no infra leaks.
    assert set(SALES_SPEC.queries) == {"by_region"}
    assert SALES_SPEC.read.__name__ == "RegionTotal"

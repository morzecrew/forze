"""Recipe: analytics over a data lake with DuckDB — named queries over Parquet, in-process.

DuckDB is an embedded engine: point it at Parquet/CSV/Iceberg/Delta files (here a
local Parquet the demo writes) and it runs the scan in your process — no warehouse,
no Docker. Handlers only ever name a ``query_key`` + output type; the lake source
binds below the line in the config and lifecycle step.

Run it:  uv run python -m examples.recipes.analytics_duckdb.app
Exercised by tests/unit/test_examples/test_analytics_duckdb.py.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import duckdb
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
    LifecyclePlan,
)
from forze_duckdb import (
    DuckDbAnalyticsConfig,
    DuckDbClient,
    DuckDbDepsModule,
    DuckDbQueryConfig,
    ParquetSource,
    duckdb_lifecycle_step,
)


# --8<-- [start:spec]
class RegionTotal(BaseModel):
    region: str
    total: int


class SalesQuery(BaseModel):
    min_total: int = 0


# The spec is the whole handler-facing surface: a named query + its params + read model.
# It says nothing about DuckDB, Parquet, or where the data lives.
SALES_SPEC = AnalyticsSpec[RegionTotal, None](  # type: ignore[reportUnknownReturnType]
    name="sales",
    read=RegionTotal,
    queries={"by_region": AnalyticsQueryDefinition(params=SalesQuery)},
)
# --8<-- [end:spec]


# --8<-- [start:config]
# The physical mapping lives below the line: DuckDB SQL per query_key, against a
# source view registered at startup. `$min_total` binds from the params model.
SALES_CONFIG = DuckDbAnalyticsConfig(
    queries={
        "by_region": DuckDbQueryConfig(
            sql=(
                "SELECT region, sum(total) AS total FROM sales "
                "GROUP BY region HAVING sum(total) >= $min_total ORDER BY region"
            ),
        ),
    },
)
# --8<-- [end:config]


def _write_sample_lake(directory: Path) -> Path:
    """Write a tiny Parquet 'lake' the demo can query (stands in for S3/GCS)."""

    path = directory / "sales.parquet"
    with duckdb.connect() as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES "
            "('eu', 10), ('eu', 5), ('us', 30), ('us', 20), ('apac', 8)"
            ") t(region, total)) TO ? (FORMAT parquet)",
            [str(path)],
        )
    return path


# --8<-- [start:wire]
def build_runtime(parquet: Path) -> ExecutionRuntime:
    client = DuckDbClient()

    # The lifecycle step opens the engine and registers the lake source as a view;
    # in production `ParquetSource("s3://bucket/sales/*.parquet")` + S3 credentials.
    return ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            DuckDbDepsModule(client=client, analytics={"sales": SALES_CONFIG})
        ).freeze(),
        lifecycle=LifecyclePlan.from_steps(
            duckdb_lifecycle_step(
                extensions=(),  # local file: no httpfs needed
                sources={"sales": ParquetSource(str(parquet))},
            ),
        ).freeze(),
    )


# --8<-- [end:wire]


# --8<-- [start:query]
async def top_regions(ctx: ExecutionContext, min_total: int) -> list[RegionTotal]:
    # The handler names the query and gets typed rows back — engine-agnostic.
    page = await ctx.analytics.query(SALES_SPEC).run(
        "by_region", SalesQuery(min_total=min_total)
    )
    return list(page.hits)


# --8<-- [end:query]


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        parquet = _write_sample_lake(Path(tmp))
        runtime = build_runtime(parquet)

        async with runtime.scope():
            rows = await top_regions(runtime.get_context(), min_total=20)

        for row in rows:
            print(f"{row.region}: {row.total}")


if __name__ == "__main__":
    asyncio.run(main())

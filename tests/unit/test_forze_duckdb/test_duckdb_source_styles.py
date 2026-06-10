"""Both lake-source styles must produce identical results: inline ``read_parquet``
vs a view registered at client startup. This is the acceptance gate for shipping
both styles."""

from __future__ import annotations

from typing import Any

from forze.application.contracts.analytics import AnalyticsSpec
from forze_duckdb import (
    DuckDbAnalyticsConfig,
    DuckDbClient,
    DuckDbQueryConfig,
)
from forze_duckdb.adapters import DuckDbAnalyticsAdapter

from tests.unit.test_forze_duckdb.conftest import Params, Row

# ----------------------- #


async def test_inline_and_view_sources_match(
    events_parquet: str,
    events_spec: AnalyticsSpec[Row, Any],
) -> None:
    # Style A: inline read_parquet in the query SQL.
    inline_client = DuckDbClient()
    await inline_client.initialize(":memory:", extensions=())
    inline_cfg = DuckDbAnalyticsConfig(
        queries={
            "by_day": DuckDbQueryConfig(
                sql=(
                    f"SELECT day, total FROM read_parquet('{events_parquet}') "
                    "WHERE total >= $min_total ORDER BY day"
                )
            )
        }
    )
    inline = DuckDbAnalyticsAdapter(
        client=inline_client, spec=events_spec, config=inline_cfg
    )

    # Style B: a view registered at startup; the query references it.
    view_client = DuckDbClient()
    await view_client.initialize(
        ":memory:",
        extensions=(),
        sources={"events": f"read_parquet('{events_parquet}')"},
    )
    view_cfg = DuckDbAnalyticsConfig(
        queries={
            "by_day": DuckDbQueryConfig(
                sql="SELECT day, total FROM events WHERE total >= $min_total ORDER BY day"
            )
        }
    )
    view = DuckDbAnalyticsAdapter(
        client=view_client, spec=events_spec, config=view_cfg
    )

    try:
        inline_rows = [
            (r.day, r.total)
            for r in (await inline.run("by_day", Params(min_total=15))).hits
        ]
        view_rows = [
            (r.day, r.total)
            for r in (await view.run("by_day", Params(min_total=15))).hits
        ]

        assert inline_rows == view_rows == [("b", 20), ("c", 30), ("d", 40)]

    finally:
        await inline_client.close()
        await view_client.close()


# ....................... #


async def test_bootstrap_sql_runs_at_startup(
    events_parquet: str,
    events_spec: AnalyticsSpec[Row, Any],
) -> None:
    client = DuckDbClient()
    await client.initialize(
        ":memory:",
        extensions=(),
        bootstrap_sql=(
            f"CREATE VIEW events AS SELECT * FROM read_parquet('{events_parquet}')",
        ),
    )
    cfg = DuckDbAnalyticsConfig(
        queries={
            "by_day": DuckDbQueryConfig(
                sql="SELECT day, total FROM events WHERE total >= $min_total ORDER BY day"
            )
        }
    )
    adapter = DuckDbAnalyticsAdapter(client=client, spec=events_spec, config=cfg)

    try:
        rows = [(r.day, r.total) for r in (await adapter.run("by_day", Params())).hits]
        assert rows == [("a", 10), ("b", 20), ("c", 30), ("d", 40)]

    finally:
        await client.close()

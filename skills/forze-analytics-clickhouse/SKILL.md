---
name: forze-analytics-clickhouse
description: >-
  Wires and consumes Forze analytics with AnalyticsSpec, AnalyticsQueryPort,
  AnalyticsIngestPort, ClickHouseDepsModule, SQL templates with {name:Type},
  ClickHouseConfig lifecycle, and tests with MockAnalyticsAdapter or Docker
  ClickHouse. Use when adding ClickHouse warehouse queries or insert ingest.
---

# Forze analytics and ClickHouse

Use when querying ClickHouse tables/views or appending typed batches via `forze_clickhouse` (`clickhouse-connect` async client). For in-memory tests, use `MockAnalyticsAdapter` in `forze_mock`.

## Spec and deps route

`AnalyticsSpec.name` is the logical route. Register the same key in `ClickHouseDepsModule.analytics` with `database`, `queries`, and optional `ingest_table`.

```python
from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze_clickhouse import (
    ClickHouseClient,
    ClickHouseConfig,
    ClickHouseDepsModule,
    clickhouse_lifecycle_step,
)

spec = AnalyticsSpec(
    name="events",
    read=MetricRow,
    queries={"daily": AnalyticsQueryDefinition(params=DailyParams)},
    ingest=EventRow,
)

connection = ClickHouseConfig(host="localhost", port=8123, database="analytics")
module = ClickHouseDepsModule(
    client=ClickHouseClient(),
    analytics={
        "events": {
            "database": "analytics",
            "queries": {"daily": {"sql": "SELECT ... WHERE day = {day:Date}"}},
            "ingest_table": "events_raw",
        },
    },
)
```

## Handler usage

```python
async with runtime.session() as ctx:
    q = ctx.analytics.query(spec)
    page = await q.run_page("daily", DailyParams(day="2026-01-01"))
    await ctx.analytics.ingest(spec).append([EventRow(event="signup")])
```

`dry_run` on ClickHouse skips execution (no cost estimate). Use `cursor_column` + `{forze_after:Type}` in SQL for keyset cursors.

## Local Docker

Pass `ClickHouseConfig(host=..., port=8123, username=..., password=...)` to `clickhouse_lifecycle_step(connection=...)`. See [ClickHouse integration](https://morzecrew.github.io/forze/integrations/clickhouse/) for Docker and local setup.

## Reference

- [ClickHouse integration](https://morzecrew.github.io/forze/integrations/clickhouse/)
- [Analytics contracts](https://morzecrew.github.io/forze/reference/contracts/)
- [`forze-framework-usage`](../forze-framework-usage/SKILL.md)

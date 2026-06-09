---
title: ClickHouse
icon: lucide/bar-chart-3
summary: Named, parameterized warehouse queries on ClickHouse
---

`forze[clickhouse]` implements the analytics contracts on ClickHouse — named,
parameterized SQL against pre-provisioned tables, plus optional append. Like
[BigQuery](bigquery.md), this is warehouse reads, not document storage.

## Install

```bash
uv add 'forze[clickhouse]'
```

Needs a ClickHouse server.

## The client

```python
from forze_clickhouse import ClickHouseClient

ch = ClickHouseClient()
```

`RoutedClickHouseClient` resolves per-tenant host/database/credentials.

## Wire it

Each analytics route maps `query_key`s to SQL, keyed by `AnalyticsSpec.name`:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_clickhouse import (
    ClickHouseAnalyticsConfig,
    ClickHouseClient,
    ClickHouseConfig,
    ClickHouseDepsModule,
    ClickHouseQueryConfig,
    clickhouse_lifecycle_step,
)

events = ClickHouseAnalyticsConfig(
    database="analytics",
    queries={"daily": ClickHouseQueryConfig(sql="SELECT day, value FROM analytics.metrics WHERE day = {day:Date}")},
)

deps = DepsRegistry.from_modules(ClickHouseDepsModule(client=ch, analytics={"events": events}))
lifecycle = LifecyclePlan.from_steps(
    clickhouse_lifecycle_step(connection=ClickHouseConfig(host="localhost", port=8123)),
)
```

## What it provides

| Contract | Keyed by |
|----------|----------|
| Analytics query (`run` / `run_page` / `run_cursor` / `run_chunked`) | `AnalyticsSpec.name` |
| Analytics ingest (`append`) | `AnalyticsSpec.name` (`ingest_relation`) |

## Notes

- **Tables are pre-provisioned**; queries are named SQL with server-side
  `{field:Type}` params bound from the params model. Read SQL isn't rewritten.
- The lifecycle step takes `connection=ClickHouseConfig(...)` (host, port,
  credentials, database).
- For stable deep pagination, set `cursor_column` on the query config (keyset)
  instead of offset cursors.

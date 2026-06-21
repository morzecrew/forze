---
name: forze-analytics
description: >-
  Wires and consumes Forze analytics with AnalyticsSpec, AnalyticsQueryPort
  (run / run_page / run_cursor) and AnalyticsIngestPort (append), named
  parameterized SQL templates, and the BigQuery (BigQueryDepsModule, @param SQL)
  and ClickHouse (ClickHouseDepsModule, {name:Type} SQL) backends, plus
  MockAnalyticsAdapter tests. Use when adding warehouse queries or streaming
  append.
---

# Forze analytics (BigQuery & ClickHouse)

Use when querying pre-provisioned warehouse tables/views or appending typed batches. The contract is one `AnalyticsSpec` and one query/ingest port surface; the deps module, the SQL parameter syntax, and the lifecycle differ per backend. Analytics is a **named-query port**, not a CRUD aggregate — there is no facade; resolve `ctx.analytics.query(spec)` / `ctx.analytics.ingest(spec)` directly (in driving code or a handler). For in-memory tests, use `MockAnalyticsAdapter` in `forze_mock`. For general handler patterns, see [`forze-framework-usage`](../forze-framework-usage/SKILL.md).

## Spec and deps route

`AnalyticsSpec.name` is the logical route. Register the same key in the backend module's `analytics` map with the dataset/database, the named `queries`, and an optional `ingest` relation (`(namespace, table)`). SQL lives in the deps config (never in handlers); each query is referenced by key.

```python
from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec

spec = AnalyticsSpec(
    name="events",
    read=MetricRow,
    queries={"daily": AnalyticsQueryDefinition(params=DailyParams)},
    ingest=EventRow,
)
```

### BigQuery (`@param` SQL)

```python
from forze.application.execution import LifecyclePlan
from forze_bigquery import BigQueryClient, BigQueryDepsModule, bigquery_lifecycle_step

module = BigQueryDepsModule(
    client=BigQueryClient(),
    analytics={
        "events": {
            "dataset": "analytics",
            "queries": {"daily": {"sql": "SELECT day, count(*) AS n FROM events WHERE day = @day GROUP BY day"}},
            "ingest": ("analytics", "events_raw"),
        },
    },
)
lifecycle = LifecyclePlan.from_steps(
    bigquery_lifecycle_step(project_id="my-gcp-project"),  # initializes BigQueryClient
)
```

Local emulator: set `BIGQUERY_EMULATOR_HOST=http://localhost:9050` before startup ([goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator)); the lifecycle step does not take an emulator URL.

### ClickHouse (`{name:Type}` SQL)

```python
from forze.application.execution import LifecyclePlan
from forze_clickhouse import (
    ClickHouseClient,
    ClickHouseConfig,
    ClickHouseDepsModule,
    clickhouse_lifecycle_step,
)

module = ClickHouseDepsModule(
    client=ClickHouseClient(),
    analytics={
        "events": {
            "database": "analytics",
            "queries": {"daily": {"sql": "SELECT day, count(*) AS n FROM events WHERE day = {day:Date} GROUP BY day"}},
            "ingest": ("analytics", "events_raw"),
        },
    },
)
lifecycle = LifecyclePlan.from_steps(
    clickhouse_lifecycle_step(
        connection=ClickHouseConfig(host="localhost", port=8123, username="default", password=""),
    ),
)
```

Cursor reads go through `run_cursor` with an opaque cursor token (`CursorPaginationExpression`) — there is no `cursor_column` SQL convention, and a backend that can't do keyset cursors raises unsupported. `dry_run` skips execution.

## Consuming analytics

Open a runtime scope, resolve the query/ingest port, and call by query key:

```python
async with runtime.scope():
    ctx = runtime.get_context()
    q = ctx.analytics.query(spec)

    page = await q.run_page("daily", DailyParams(day="2026-01-01"))        # Page: .hits + total count (unless skip_total)
    countless = await q.run("daily", DailyParams(day="2026-01-01"))        # CountlessPage: .hits only — prefer for large scans
    rows = countless.hits
    # run_cursor → one keyset CursorPage; run_chunked → async batches, for streamed large exports

    result = await ctx.analytics.ingest(spec).append([EventRow(event="signup")])
    # result.accepted, result.rejected, result.errors (partial streaming-insert failures)
```

## Anti-patterns

1. **Putting SQL or dataset/database names in `AnalyticsSpec`** — the spec carries the logical name and param/read types; the deps config carries SQL and physical names.
2. **Building SQL strings in handlers** — reference a named query key; parameterize with the backend's placeholder syntax (`@param` / `{name:Type}`).
3. **Using `run_page` for large scans you don't need a total for** — it runs a COUNT; prefer `run` / `run_cursor`.
4. **Treating analytics as a write store** — it is append/query only; durable state belongs in document/storage ports.
5. **Hard-coding warehouse credentials** — use a secrets/env layer, ADC, or workload identity.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [BigQuery integration](https://morzecrew.github.io/forze/latest/integrations/bigquery/)
- [ClickHouse integration](https://morzecrew.github.io/forze/latest/integrations/clickhouse/)
- [Analytics contracts](https://morzecrew.github.io/forze/latest/reference/contracts/)
- [`forze-framework-usage`](../forze-framework-usage/SKILL.md)

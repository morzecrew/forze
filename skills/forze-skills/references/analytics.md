# Analytics

Warehouse queries and streaming appends over an `AnalyticsSpec`: the spec names a query, the deps config carries the SQL and the dataset. Covers BigQuery and ClickHouse, whose placeholder syntaxes differ.

## Spec and deps route

`AnalyticsSpec.name` is the logical route. Register the same key in the backend module's `analytics` map as a backend `*AnalyticsConfig` object (not a plain dict — the module freezes the mapping but does not coerce nested dicts) carrying the dataset/database, the named `queries` (each a `*QueryConfig`), and an optional `ingest` relation (`(namespace, table)`). SQL lives in the deps config (never in handlers); each query is referenced by key.

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
from forze_bigquery import (
    BigQueryAnalyticsConfig,
    BigQueryClient,
    BigQueryDepsModule,
    BigQueryQueryConfig,
    bigquery_lifecycle_step,
)

module = BigQueryDepsModule(
    client=BigQueryClient(),
    analytics={
        "events": BigQueryAnalyticsConfig(  # a config object, not a plain dict
            dataset="analytics",
            queries={"daily": BigQueryQueryConfig(sql="SELECT day, count(*) AS n FROM events WHERE day = @day GROUP BY day")},
            ingest=("analytics", "events_raw"),
        ),
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
    ClickHouseAnalyticsConfig,
    ClickHouseClient,
    ClickHouseConfig,
    ClickHouseDepsModule,
    ClickHouseQueryConfig,
    clickhouse_lifecycle_step,
)

module = ClickHouseDepsModule(
    client=ClickHouseClient(),
    analytics={
        "events": ClickHouseAnalyticsConfig(  # a config object, not a plain dict
            database="analytics",
            queries={"daily": ClickHouseQueryConfig(sql="SELECT day, count(*) AS n FROM events WHERE day = {day:Date} GROUP BY day")},
            ingest=("analytics", "events_raw"),
        ),
    },
)
lifecycle = LifecyclePlan.from_steps(
    clickhouse_lifecycle_step(
        connection=ClickHouseConfig(host="localhost", port=8123, username="default", password=""),
    ),
)
```

ClickHouse keyset cursors need `cursor_column` on the query config plus a matching `{forze_after:Type}` placeholder in the SQL; `run_cursor` then pages by that key (you round-trip the opaque cursor token each call). Omit them and `run_cursor` falls back to offset-style cursors — unstable for large or changing result sets. `dry_run` skips execution.

### DuckDB (in-process, no `dataset`/`database`)

Same plane, same `AnalyticsSpec`, no server. The config is the smaller one — there is no warehouse namespace to name — and the lifecycle step is where a DuckDB deployment actually differs, because that is where extensions, object-store credentials and attached sources are declared:

```python
from forze_duckdb import (
    DuckDbAnalyticsConfig,
    DuckDbClient,
    DuckDbDepsModule,
    DuckDbQueryConfig,
    duckdb_lifecycle_step,
)

module = DuckDbDepsModule(
    client=DuckDbClient(),
    analytics={
        "events": DuckDbAnalyticsConfig(
            queries={"daily": DuckDbQueryConfig(sql="SELECT day, count(*) AS n FROM events GROUP BY day")},
        ),
    },
)
lifecycle = duckdb_lifecycle_step(database=":memory:", sources={"events": "s3://bucket/events/*.parquet"})
```

## Consuming analytics

Open a runtime scope, resolve the query/ingest port, and call by query key (`DailyParams` is your query-params model and `EventRow` your ingest-row model — the same ones the `AnalyticsSpec` references):

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

- **Putting SQL or dataset/database names in `AnalyticsSpec`** — the spec carries the logical name and param/read types; the deps config carries SQL and physical names.
- **Building SQL strings in handlers** — reference a named query key; parameterize with the backend's placeholder syntax (`@param` / `{name:Type}`).
- **Using `run_page` for large scans you don't need a total for** — it runs a COUNT; prefer `run` / `run_cursor`.
- **Treating analytics as a write store** — it is append/query only; durable state belongs in document/storage ports.
- **Hard-coding warehouse credentials** — use a secrets/env layer, ADC, or workload identity.

## Reference

- [BigQuery integration](https://morzecrew.github.io/forze/latest/integrations/bigquery/)
- [ClickHouse integration](https://morzecrew.github.io/forze/latest/integrations/clickhouse/)
- [Analytics contracts](https://morzecrew.github.io/forze/latest/reference/contracts/)

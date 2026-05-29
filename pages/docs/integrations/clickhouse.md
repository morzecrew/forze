# ClickHouse Integration

## What this integration provides

Run named SQL queries and optional row appends behind Forze analytics contracts without coupling handlers to the ClickHouse Python driver.

## When to use it

Use this when you operate a ClickHouse cluster (or a local Docker instance for tests) and want native async access via [`clickhouse-connect`](https://pypi.org/project/clickhouse-connect/) (`get_async_client`, aiohttp).

## Standard setup checklist

1. Install the `clickhouse` optional extra.
2. Declare `AnalyticsSpec` routes and named queries in application code.
3. Map each route to database, SQL templates, and optional ingest table in `ClickHouseDepsModule`.
4. Add `clickhouse_lifecycle_step` or `routed_clickhouse_lifecycle_step` when the client opens network connections.
5. Resolve ports from `ExecutionContext`; do not import adapters in handlers.

Use `RoutedClickHouseClient` when tenant identity selects ClickHouse host, credentials, or database.

`forze_clickhouse` implements `AnalyticsQueryPort` and, when configured, `AnalyticsIngestPort` on the same adapter.

## Installation

    :::bash
    uv add 'forze[clickhouse]'

## Runtime wiring

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_clickhouse import (
        ClickHouseClient,
        ClickHouseConfig,
        ClickHouseDepsModule,
        clickhouse_lifecycle_step,
    )

    client = ClickHouseClient()
    module = ClickHouseDepsModule(
        client=client,
        analytics={
            "events": {
                "database": "analytics",
                "queries": {
                    "daily": {
                        "sql": (
                            "SELECT event, value FROM analytics.metrics "
                            "WHERE day = {day:Date}"
                        ),
                    },
                },
                "ingest_table": "events_raw",
            },
        },
    )

    connection = ClickHouseConfig(
        host="clickhouse.example.com",
        port=8123,
        username="default",
        password="secret",
        database="analytics",
    )

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
        lifecycle=LifecyclePlan.from_steps(
            clickhouse_lifecycle_step(connection=connection),
        ),
    )

### Local Docker (integration tests)

Start ClickHouse (for example `clickhouse/clickhouse-server` on port 8123 with credentials), then pass host/port in `ClickHouseConfig`. There is no emulator environment variable on the public API—connection settings are explicit.

### Routed client (multi-tenant credentials)

Register `RoutedClickHouseClient` under `ClickHouseClientDepKey` and use `routed_clickhouse_lifecycle_step(client=routed_ch)`. Per-tenant secrets resolve to `ClickHouseRoutingCredentials` (`host`, `port`, `username`, `password`, `database`, `secure`, …).

## Configuration

Physical mapping lives on `ClickHouseDepsModule.analytics`, keyed by `AnalyticsSpec.name`:

| Field | Purpose |
|-------|---------|
| `queries` | Map of `query_key` → `sql` (+ optional `skip_total`, `cursor_column`) |
| `ingest_relation` | Ingest target as static `(database, table)` or tenant `ValueResolver`. Preferred when `AnalyticsSpec.ingest` is set. |
| `database` | Legacy database id for `ingest_table` when `ingest_relation` is omitted. |
| `ingest_table` | Legacy table name for `append`; use `ingest_relation` for relation-level isolation. |
| `max_append_rows` | Optional cap per `append` batch (default 10_000) |

Query keys in config **must** match `AnalyticsSpec.queries`. The module validates this at build time.

## SQL templates

- Use ClickHouse **server-side** parameters: `{field:Type}` (for example `{day:Date}`, `{uid:UInt64}`).
- Values come from the spec’s Pydantic `params` model via `model_dump()` passed as `parameters`.
- Offset pagination uses `LIMIT` / `OFFSET` appended by the adapter.
- `run_page` runs a `count()` wrapper unless `skip_total: true` is set on the query config.
- `run_cursor` defaults to **offset cursors** (`{"o": offset}`); unstable under concurrent writes. For keyset pagination, set `cursor_column` on the query config and include `{forze_after:Type}` in SQL (the adapter injects `forze_after` from the cursor).
- `dry_run` skips execution (empty pages); ClickHouse does not estimate query cost via this option.
- The shared async client selects the database per request via query settings (safe under concurrent handlers).

### Analytics queries and tenancy

**Ingest** uses `ingest_relation` (resolved per request). **Query** SQL is **not** rewritten—use `{field:Type}` placeholders for params only.

| Concern | Framework | Application |
|---------|-----------|-------------|
| Append target | `ingest_relation` / `resolved_ingest_relation()` | Static `(database, table)` or `ValueResolver` |
| Read SQL | Not rewritten | Qualify tables in SQL (`database.table` or `FROM db.table`) |

Recommended patterns:

1. **Cluster/database per tenant** — `RoutedClickHouseClient` with per-tenant `database` in `ClickHouseRoutingCredentials`; static table names in SQL.
2. **Database-per-tenant on shared cluster** — `ingest_relation` resolver for append; tenant-specific `database.table` in query strings.
3. **Row filters** — `WHERE tenant_id = {tenant_id:UUID}` (or similar) bound from the params model when sharing tables.

See [Multi-tenancy — relation-level isolation](../concepts/multi-tenancy.md#relation-level-isolation-all-integrations).

## Using analytics ports

    :::python
    from forze.application.contracts.analytics import AnalyticsSpec

    async with runtime.session() as ctx:
        q = ctx.analytics.query(spec)
        page = await q.run_page("daily", DailyParams(day="2026-01-01"))
        await ctx.analytics.ingest(spec).append([EventRow(event="signup", value=1)])

Pass `AnalyticsRunOptions` (`dry_run`, `max_rows`, `timeout`) per request; `dry_run` returns empty pages without executing SQL.

## Operation reference

| Port method | Behavior |
|-------------|----------|
| `run` / `project_run` / `select_run` | Execute named query; return `CountlessPage` |
| `run_page` | COUNT wrapper + data query → `Page` with total |
| `run_cursor` | Offset token in cursor `after` |
| `run_chunked` | Repeated queries with increasing `OFFSET` |
| `append` | `insert` rows into `ingest_table` |

## Multi-tenant databases

- **Connection routing:** `RoutedClickHouseClient` resolves per-tenant connection settings from `SecretsPort` (including default `database` in credentials).
- **Relation-level ingest:** use `ingest_relation` when the physical `(database, table)` varies per tenant on a shared cluster.
- **Analytics reads:** query SQL remains author-defined; see [Analytics queries and tenancy](#analytics-queries-and-tenancy) above.

## Client health

Call `await client.health()` after lifecycle startup (`SELECT 1` probe).

## Out of scope (v1)

Load jobs, `MERGE`, DDL, per-route database resolvers, and bulk ETL. Prefer queue/stream handoff plus external loaders for large pipelines; see [Analytics contracts](../core-package/contracts/analytics.md).

## Related pages

- [Analytics contracts](../core-package/contracts/analytics.md)
- [Mock integration](mock.md) — in-memory adapter for unit tests
- [BigQuery integration](bigquery.md) — GCP warehouse adapter

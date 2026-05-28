# Google BigQuery Integration

## What this integration provides

Run named Standard SQL queries and optional streaming row appends behind Forze analytics contracts without coupling handlers to the BigQuery SDK.

## When to use it

Use this when you run on GCP (or the [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) for local tests) and want async HTTP access via [`gcloud-aio-bigquery`](https://pypi.org/project/gcloud-aio-bigquery/), aligned with [`forze_gcs`](gcs.md).

## Standard setup checklist

1. Install the `bigquery` optional extra.
2. Declare `AnalyticsSpec` routes and named queries in application code.
3. Map each route to dataset, SQL templates, and optional ingest table in `BigQueryDepsModule`.
4. Add `bigquery_lifecycle_step` when the client opens network connections.
5. Resolve ports from `ExecutionContext`; do not import adapters in handlers.

`forze_bigquery` implements `AnalyticsQueryPort` and, when configured, `AnalyticsIngestPort` on the same adapter.

## Installation

    :::bash
    uv add 'forze[bigquery]'

## Runtime wiring

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_bigquery import BigQueryClient, BigQueryDepsModule, bigquery_lifecycle_step

    client = BigQueryClient()
    module = BigQueryDepsModule(
        client=client,
        analytics={
            "events": {
                "dataset": "analytics",
                "queries": {
                    "daily": {
                        "sql": (
                            "SELECT event, value FROM analytics.metrics "
                            "WHERE day = @day"
                        ),
                    },
                },
                "ingest_table": "events_raw",
                "insert_id_field": "event_id",
            },
        },
    )

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
        lifecycle=LifecyclePlan.from_steps(
            bigquery_lifecycle_step(project_id="my-gcp-project"),
        ),
    )

### Emulator (goccy/bigquery-emulator)

For local development and integration tests, set `BIGQUERY_EMULATOR_HOST` to the emulator base URL **before** starting the runtime (for example `http://localhost:9050`). The client reads this environment variable at initialization; lifecycle and application code do not take an emulator URL parameter.

Start the emulator (for example `ghcr.io/goccy/bigquery-emulator:latest` on port 9050), then wire lifecycle as usual:

    :::python
    bigquery_lifecycle_step(project_id="test-project")

### Service account credentials

By default the client uses Application Default Credentials. To use an explicit key file:

    :::python
    bigquery_lifecycle_step(
        project_id="my-gcp-project",
        service_file="/path/to/service-account.json",
    )

### What gets registered

| Key | Capability |
|-----|-----------|
| `BigQueryClientDepKey` | Raw BigQuery client (`Job` / `Table` via shared `aiohttp` session) |
| `AnalyticsQueryDepKey` | Query port adapter factory |
| `AnalyticsIngestDepKey` | Ingest port adapter factory (when `ingest_table` is set) |

## Configuration

Physical mapping lives on `BigQueryDepsModule.analytics`, keyed by `AnalyticsSpec.name`:

| Field | Purpose |
|-------|---------|
| `dataset` | BigQuery dataset id for the route |
| `queries` | Map of `query_key` → `sql` (+ optional `maximum_bytes_billed`, `skip_total`) |
| `ingest_table` | Table id for `append`; required when `spec.ingest` is set |
| `insert_id_field` | Optional row field for streaming insert deduplication |
| `max_append_rows` | Optional cap per `append` batch (default 10_000) |

Query keys in config **must** match `AnalyticsSpec.queries`. The module validates this at build time.

## SQL templates

- Use **Standard SQL** (`use_legacy_sql=False` is enforced in the adapter).
- Name parameters with `@field` placeholders; values come from the spec’s Pydantic `params` model via BigQuery `queryParameters`.
- Offset pagination is applied in SQL (`LIMIT` / `OFFSET`) or by slicing small result sets.
- `run_page` runs a `COUNT(*)` wrapper around your query SQL, then the data query, so totals are available on `Page.total`. Set `skip_total: true` on a query to skip the COUNT (``Page.total`` is ``None``).
- Streaming inserts may partially fail; `AnalyticsAppendResult.rejected` and `.errors` surface row-level `insertErrors` when present.

## Using analytics ports

    :::python
    from forze.application.contracts.analytics import AnalyticsSpec

    async with runtime.session() as ctx:
        q = ctx.analytics.query(spec)
        page = await q.run_page("daily", DailyParams(day="2026-01-01"))
        await ctx.analytics.ingest(spec).append([EventRow(event="signup", value=1)])

Pass `AnalyticsRunOptions` (`dry_run`, `max_rows`, `timeout`) per request; the adapter maps `dry_run` to BigQuery dry-run queries and respects byte limits via `BigQueryConfig.maximum_bytes_billed`.

## Operation reference

| Port method | Behavior |
|-------------|----------|
| `run` / `project_run` / `select_run` | Execute named query; return `CountlessPage` |
| `run_page` | COUNT wrapper + data query → `Page` with total |
| `run_cursor` | Encode/decode BigQuery `pageToken` in cursor `after` |
| `run_chunked` | Page through `get_query_results` until exhausted |
| `append` | Streaming `Table.insert` with optional `insert_id_field` |

## Multi-tenant datasets

v1 uses a single `project_id` per `BigQueryClient`. For multiple tenants, either:

- Register separate `AnalyticsSpec` routes per tenant with distinct `dataset` values in `BigQueryDepsModule.analytics`, or
- Resolve the dataset in application code and pass tenant-specific specs/config at deploy time.

A future `RoutedBigQueryClient` may resolve dataset from `ctx.inv_ctx` tenant identity; not included in v1.

## Client health

Call `await client.health()` after lifecycle startup for readiness checks (lightweight dry-run query).

## Out of scope (v1)

Load jobs, `MERGE`, DDL, automatic tenancy routing, and bulk ETL. Prefer queue/stream handoff plus external loaders for large pipelines; see [Analytics contracts](../core-package/contracts/analytics.md).

## Related pages

- [Analytics contracts](../core-package/contracts/analytics.md)
- [Mock integration](mock.md) — in-memory adapter for unit tests
- [Google Cloud Storage](gcs.md) — shared `gcloud-aio` stack

---
title: Analytics
icon: lucide/chart-bar
summary: The analytics contract — registered named queries over a warehouse, plus optional ingest
---

The analytics contract runs **registered, parametrized queries** against a warehouse table
or view and returns typed rows, with an optional append-only **ingest** side. Unlike the
[document](document.md) port it has no per-row CRUD and no live query DSL — you register
named queries up front and pass typed params; unlike [procedures](procedure.md) it only
reads. The concept is [Analytics](../../data-events/analytics.md); the worked flow is the
[analytics-over-a-data-lake](../../recipes/analytics-over-a-data-lake.md) recipe.

```python
rows = await ctx.analytics.query(spec).run("by_region", params)   # registered query → typed rows
await ctx.analytics.ingest(spec).append(new_rows)                 # append-only bulk load
```

## Spec

`AnalyticsSpec[R, Ing]` — the read-row model, the named queries, and an optional ingest
model:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | logical name / warehouse route |
| `read` | `type[R]` | required | default read model for result rows |
| `queries` | `Mapping[str, AnalyticsQueryDefinition]` | required | named queries (≥1); each declares a typed `params` model |
| `ingest` | `type[Ing] \| None` | `None` | append-row model; `None` disables ingest |
| `encryption` | `FieldEncryption \| None` | `None` | seal columns at rest (confidential, **not** aggregatable; `binds_record_id` unsupported — rows have no id) |
| `read_codec` / `ingest_codec` | `ModelCodec \| None` | `None` | codec overrides (auto-derived otherwise) |

Each `AnalyticsQueryDefinition` carries a Pydantic `params` model — the typed arguments a
`run*` call passes — and an optional `description`.

## Query port  (`ctx.analytics.query(spec)`)

Run a registered query by key with typed params; the shape × pagination naming mirrors the
[document query port](document.md):

| Method | Result |
|--------|--------|
| `run(query_key, params, pagination=None, *, options=None)` | `CountlessPage[R]` |
| `run_page(...)` | `Page[R]` (with `.count` when the backend supports it) |
| `run_cursor(...)` | `CursorPage[R]` (keyset) |
| `run_chunked(..., fetch_batch_size=2000)` | async generator of row batches |
| `project_run*` / `select_run*` | `JsonDict` / caller-typed-model variants |

`options: AnalyticsRunOptions` carries the per-run knobs — `dry_run`, `max_rows`, and
`timeout`.

## Ingest port  (`ctx.analytics.ingest(spec)`)

| Method | Signature | Notes |
|--------|-----------|-------|
| `append` | `append(rows)` | append-only bulk insert; returns an `AnalyticsAppendResult` (`accepted` / `rejected` / `errors`), or `None` when the backend reports nothing |

Ingest is append-only — no update or delete. To recompute a rollup over an ingested batch,
reach for the [procedures](procedure.md) port rather than per-row writes.

## Implemented by

| Backend | Notes | Integration |
|---------|-------|-------------|
| Postgres | tables / views | [Postgres](../../integrations/postgres.md) |
| ClickHouse | columnar warehouse | [ClickHouse](../../integrations/clickhouse.md) |
| BigQuery | serverless warehouse | [BigQuery](../../integrations/bigquery.md) |
| DuckDB | in-process / data-lake (query-only; tenancy ceiling `tagged`) | [DuckDB](../../integrations/duckdb.md) |

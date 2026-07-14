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
| `provenance` | `AnalyticsProvenance` | `UNDECLARED` | where these rows come from — see below |
| `encryption` | `FieldEncryption \| None` | `None` | seal columns at rest (confidential, **not** aggregatable; `binds_record_id` unsupported — rows have no id) |
| `read_codec` / `ingest_codec` | `ModelCodec \| None` | `None` | codec overrides (auto-derived otherwise) |

Each `AnalyticsQueryDefinition` carries a Pydantic `params` model — the typed arguments a
`run*` call passes — and an optional `description`.

### Provenance

`provenance` says whether this warehouse table *derives* from data the application already
owns, or *is* the only place those rows exist. The framework cannot work that out for itself —
both cases have the same spec, the same ports and the same rows — and guessing is unsafe in
either direction, so the author declares it:

| Value | Meaning |
|-------|---------|
| `PROJECTED` | recomputed from a plane that is itself exported (documents, say). A portable export does not carry it; the application recomputes it on the target. |
| `SYSTEM_OF_RECORD` | the warehouse holds the only copy — the usual shape when events are ingested straight into ClickHouse or BigQuery. A portable export **refuses**: the query port exposes only your named queries, so there is no full-scan read to carry it with and nothing to rebuild it from. Use your warehouse's own tooling. |
| `UNDECLARED` *(default)* | nobody has said. **Legal at runtime** — it changes nothing about how the port behaves — but a portable export refuses rather than guess. |

It costs nothing until you try to [export](../../running-in-prod/index.md) the application:
assume *projected* wrongly and the export silently drops the only copy of the data; assume
*system of record* wrongly and it refuses to carry a table that was never more than a cache.
"We didn't think about it" must not look like "there was nothing here."

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

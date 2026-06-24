---
title: Analytics
icon: lucide/chart-bar
summary: Registered, typed queries over a warehouse or data lake — the group-bys and scans the document port isn't built for
---

A document read fetches records by key or filter. **Analytical** reads are a different
shape: group-bys, aggregates, scans over millions of rows, usually against a warehouse or a
pile of Parquet rather than your operational store. The analytics contract gives those a
typed, governed home — without putting warehouse SQL in your handlers.

## A named query, not a live DSL

Where the [document](reading-data.md) port composes a query DSL at call time, an analytics
surface registers its queries **up front**. A handler names a `query_key` and passes typed
params; it never writes SQL or learns the backend:

```python
--8<-- "recipes/analytics_duckdb/app.py:spec"
```

Running one returns typed rows, with the same shape × pagination naming as the document
port — `run` / `run_page` / `run_cursor`, plus `project_run` / `select_run`:

```python
--8<-- "recipes/analytics_duckdb/app.py:query"
```

The physical mapping — the SQL, the warehouse table, the lake source — lives in the wiring,
below the line. Swap DuckDB-over-Parquet for ClickHouse or BigQuery and the handler doesn't
change.

## Ingest

A surface can also be **append-only writable**: declare an `ingest` model and
`ctx.analytics.ingest(spec).append(rows)` bulk-loads them. There is no update or delete —
analytical data is immutable facts. To recompute a rollup over an ingested batch, run one
[procedure](procedures.md) rather than per-row writes.

## When to reach for it

| You need | Use |
| --- | --- |
| Group-bys, aggregates, or scans over many rows | **analytics** |
| Fetch or list operational records by key / filter | [document](reading-data.md) |
| Feed a value to logic *inside* a read source | [query parameters](query-parameters.md) |
| Recompute or run a command over the warehouse | [procedures](procedures.md) |

!!! warning "Encrypted columns can't be analyzed"

    A field-encrypted column is confidential but **not** aggregatable, groupable, or
    range-filterable — randomized ciphertext has no numeric or linguistic structure. Encrypt
    only the PII you store-and-return, never the dimensions and measures you query by.

The full spec and method surface is the
[analytics reference](../reference/contracts/analytics.md); the worked data-lake flow is the
[analytics-over-a-data-lake](../recipes/analytics-over-a-data-lake.md) recipe.

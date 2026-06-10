---
title: Analytics over a data lake
icon: lucide/database-zap
summary: Run named, typed analytics queries over Parquet on object storage with in-process DuckDB — no warehouse
---

Sometimes you need real analytical queries — group-bys, aggregates, scans over
millions of rows — but the data already lives as files in a bucket and the
latency budget is generous. Standing up a warehouse for that is overkill.
[DuckDB](../integrations/duckdb.md) runs the query *in your process*, straight
over the Parquet, so there's nothing to provision.

The handler never learns any of this. It asks for a named query and gets typed
rows back; whether they came from DuckDB-over-S3 or a warehouse is wiring.

The runnable version lives at `examples/recipes/analytics_duckdb/` and runs fully
in-process over a local Parquet file — no Docker.

## Declare the query surface

An `AnalyticsSpec` is the whole handler-facing contract: a named query, its
params model, and the read model rows come back as. It says nothing about DuckDB,
Parquet, or where the data lives.

```python
--8<-- "recipes/analytics_duckdb/app.py:spec"
```

## Map it to DuckDB

The physical mapping lives below the line — the SQL for each `query_key`, against
a source registered at startup. Params bind by name (`$min_total`):

```python
--8<-- "recipes/analytics_duckdb/app.py:config"
```

## Point it at the lake

The lifecycle step opens the engine and registers the lake source as a view. Here
the source is a local Parquet file; in production it's
`ParquetSource("s3://bucket/sales/*.parquet")` with
[object-storage credentials](../integrations/duckdb.md#the-lake-source) resolved
from your secrets backend:

```python
--8<-- "recipes/analytics_duckdb/app.py:wire"
```

## Run the query

From a handler, resolve the port off the context and run the named query — typed
rows out, engine-agnostic:

```python
--8<-- "recipes/analytics_duckdb/app.py:query"
```

## Notes

- **Query-only.** DuckDB here reads the lake; it doesn't write or maintain tables.
  Producing the Parquet (and Iceberg/Delta compaction) is a separate pipeline.
- **Both source styles work.** Register named views (`sources=`) and reference
  them in SQL, or inline `read_parquet('s3://…')` in a query.
- **It's a great test double.** Because the engine is in-process, the same spec
  can run against a tiny local Parquet fixture in a Docker-free unit test.
- **Pagination** is available via `run_page` (with a total) and `run_cursor`; for
  large scans `run_chunked` streams batches.

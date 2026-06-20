---
title: DuckDB
icon: lucide/database-zap
summary: In-process analytics over a data lake — query Parquet, Iceberg and Delta on object storage, no warehouse
---

`forze[duckdb]` implements the analytics query contract with an **embedded**
DuckDB engine. Where [ClickHouse](clickhouse.md) and [BigQuery](bigquery.md) are
standing warehouses, DuckDB is *compute without a server*: point it at Parquet,
CSV, Iceberg or Delta files on S3/GCS (or local disk) and it runs the scan
in-process. It suits heavy analytics that don't need ultra-low-latency
serving — and, being Docker-free, it doubles as an analytics backend in tests.

It is **query-only** (no ingest, no table management): named, parameterized
queries returning typed rows, exactly like the other analytics adapters.

## Install

```bash
uv add 'forze[duckdb]'
```

No server to run — DuckDB is a library. Reading object storage pulls the
`httpfs` extension (and `iceberg` / `delta` for those formats) on first use.

## The client

```python
from forze_duckdb import DuckDbClient

duck = DuckDbClient()
```

The engine is synchronous and in-process; the client runs each query on a
dedicated bounded executor (DuckDB releases the GIL, so the event loop stays
responsive) with its own cursor, and honors per-query timeouts via interrupt.

## The lake source

Sources are declared as typed descriptors that compile to scan expressions and
auto-load the extensions they need — no hand-written SQL strings:

```python
from forze_duckdb import ParquetSource, IcebergSource, DeltaSource

events = ParquetSource("s3://bucket/events/*.parquet")   # also CsvSource, JsonSource
```

Object-storage credentials render to DuckDB secrets, resolved at startup from the
wired [secrets backend](vault.md) via a `secret_ref` (or supplied inline for
local runs):

```python
from forze.application.contracts.secrets import SecretRef
from forze_duckdb import S3Credentials

creds = S3Credentials(name="lake", secret_ref=SecretRef(path="lake/s3"))
```

## Wire it

Each analytics route maps `query_key`s to DuckDB SQL (referencing a registered
source view or an inline `read_parquet(...)`), keyed by `AnalyticsSpec.name`. The
lifecycle step opens the connection, loads extensions, registers credentials and
source views:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_duckdb import (
    DuckDbAnalyticsConfig,
    DuckDbClient,
    DuckDbDepsModule,
    DuckDbQueryConfig,
    ParquetSource,
    S3Credentials,
    duckdb_lifecycle_step,
)
from forze.application.contracts.secrets import SecretRef

duck = DuckDbClient()

events = DuckDbAnalyticsConfig(
    queries={"by_day": DuckDbQueryConfig(
        sql="SELECT day, sum(total) AS total FROM events WHERE day >= $since GROUP BY day",
    )},
)

deps = DepsRegistry.from_modules(DuckDbDepsModule(client=duck, analytics={"events": events}))
lifecycle = LifecyclePlan.from_steps(
    duckdb_lifecycle_step(
        object_stores=(S3Credentials(name="lake", secret_ref=SecretRef(path="lake/s3")),),
        sources={"events": ParquetSource("s3://bucket/events/*.parquet")},
    ),
)
```

For a runnable, Docker-free walkthrough, see the
[analytics recipe](../recipes/analytics-over-a-data-lake.md).

## What it provides

| Contract | Keyed by |
|----------|----------|
| Analytics query (`run` / `run_page` / `run_cursor` / `run_chunked`) | `AnalyticsSpec.name` |

## Notes

- **Query-only.** Writing/maintaining tables (Iceberg/Delta compaction, ingest)
  is an ETL/ops concern, not a domain port — keep it out of the adapter.
- **Source styles compose.** Register named views (`sources=`) and reference them
  in SQL, *or* inline `read_parquet('s3://…')` in a query — both work.
- **Iceberg/Delta:** point `IcebergSource` at the table's current
  `*.metadata.json` (the table-directory form needs a `version-hint.text` many
  writers omit); for in-place tables read with `allow_moved_paths=False`.
- **Concurrency:** each query gets its own cursor, so parallel reads don't
  serialize. Cap parallelism with `DuckDbConfig(max_concurrent_queries=...)`.
- Cursor pagination is offset-based; a full terminal page reports `has_more`
  until the next (short) page confirms the end.

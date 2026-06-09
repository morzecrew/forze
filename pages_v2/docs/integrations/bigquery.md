---
title: BigQuery
icon: lucide/bar-chart-3
summary: Named, parameterized warehouse queries on Google BigQuery
---

`forze[bigquery]` implements the analytics contracts on Google BigQuery — named,
parameterized SQL against pre-provisioned tables, plus optional append. This is
warehouse reads, not OLTP document storage.

## Install

```bash
uv add 'forze[bigquery]'
```

Needs Google BigQuery (or the `goccy/bigquery-emulator` via
`BIGQUERY_EMULATOR_HOST`).

## The client

```python
from forze_bigquery import BigQueryClient

bq = BigQueryClient()
```

`RoutedBigQueryClient` resolves a per-tenant project/credentials.

## Wire it

Each analytics route maps `query_key`s to SQL, keyed by `AnalyticsSpec.name`:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_bigquery import BigQueryAnalyticsConfig, BigQueryClient, BigQueryDepsModule, BigQueryQueryConfig, bigquery_lifecycle_step

events = BigQueryAnalyticsConfig(
    dataset="analytics",
    queries={"daily": BigQueryQueryConfig(sql="SELECT day, value FROM analytics.metrics WHERE day = @day")},
)

deps = DepsRegistry.from_modules(BigQueryDepsModule(client=bq, analytics={"events": events}))
lifecycle = LifecyclePlan.from_steps(bigquery_lifecycle_step(project_id="my-project"))
```

## What it provides

| Contract | Keyed by |
|----------|----------|
| Analytics query (`run` / `run_page` / `run_cursor` / `run_chunked`) | `AnalyticsSpec.name` |
| Analytics ingest (`append`) | `AnalyticsSpec.name` (`ingest_relation`) |

## Notes

- **Tables are pre-provisioned**; queries are named Standard SQL with `@param`
  placeholders bound from each query's params model. Forze does **not** rewrite
  read SQL — qualify tables yourself.
- Ingest targets a relation `(dataset, table)`, static or a per-tenant resolver.
- Out of scope: load jobs, `MERGE`, and DDL — manage those externally.

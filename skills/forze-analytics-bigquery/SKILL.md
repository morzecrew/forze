---
name: forze-analytics-bigquery
description: >-
  Wires and consumes Forze analytics with AnalyticsSpec, AnalyticsQueryPort,
  AnalyticsIngestPort, BigQueryDepsModule, SQL templates with @params, lifecycle,
  and tests with MockAnalyticsAdapter or bigquery-emulator. Use when adding
  BigQuery warehouse queries or streaming append.
---

# Forze analytics and BigQuery

Use when querying pre-provisioned BigQuery tables/views or appending small typed batches via `forze_bigquery` (`gcloud-aio-bigquery`). For in-memory tests, use `MockAnalyticsAdapter` in `forze_mock`. For general handler patterns, see [`forze-framework-usage`](../forze-framework-usage/SKILL.md).

## Spec and deps route

`AnalyticsSpec.name` is the logical route. Register the same key in `BigQueryDepsModule.analytics` with `dataset`, `queries`, and optional `ingest_table`.

```python
from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze_bigquery import BigQueryClient, BigQueryDepsModule, bigquery_lifecycle_step

spec = AnalyticsSpec(
    name="events",
    read=MetricRow,
    queries={"daily": AnalyticsQueryDefinition(params=DailyParams)},
    ingest=EventRow,
)

module = BigQueryDepsModule(
    client=BigQueryClient(),
    analytics={
        "events": {
            "dataset": "analytics",
            "queries": {"daily": {"sql": "SELECT ... WHERE day = @day"}},
            "ingest_table": "events_raw",
        },
    },
)
```

## Handler usage

```python
async with runtime.session() as ctx:
    q = ctx.analytics.query(spec)
    # Prefer run/run_cursor for large scans; run_page runs COUNT unless skip_total: true
    page = await q.run_page("daily", DailyParams(day="2026-01-01"))
    result = await ctx.analytics.ingest(spec).append([EventRow(event="signup")])
    # result.accepted, result.rejected, result.errors (partial streaming insert failures)
```

## Local emulator

Set `BIGQUERY_EMULATOR_HOST=http://localhost:9050` before startup. Run [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) (see integration tests); lifecycle does not accept an emulator URL.

## Full reference

- Integration doc: `pages/docs/integrations/bigquery.md`
- Contracts: `pages/docs/core-package/contracts/analytics.md`

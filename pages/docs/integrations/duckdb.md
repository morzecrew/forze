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
queries returning typed rows, exactly like the other analytics adapters. With a
per-tenant routed client it also serves trusted
[dynamic reads](../data-events/dynamic-read.md) over an Iceberg lake.

## Install

```bash
uv add 'forze[duckdb]'
```

No server to run — DuckDB is a library. Reading object storage pulls the
`httpfs` extension (and `iceberg` / `delta` for those formats) on first use.

The extra installs DuckDB 1.x (`duckdb>=1.1.0,<2`). DuckDB 2.0 removed
`fetch_arrow_table`, which the client reads results with, so it is not supported yet.

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

## Settings

`DuckDbSettings` carries the database path and the resource limits — `memory_limit` in
particular, since DuckDB otherwise sizes itself from the host's memory rather than the
cgroup's, which is how a query gets the container OOM-killed. Extensions, object stores and
named sources stay wiring arguments. See
[connection settings](index.md#connection-settings).

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

## Per-tenant lake reads (`dedicated`)

One `DuckDbClient` is one engine for every tenant: whatever credentials it holds,
every statement can use. `RoutedDuckDbClient` instead builds an unnamed
`:memory:` instance per tenant, attached to that tenant's own warehouse in an
Iceberg REST catalog (Lakekeeper) with the tenant's own OAuth2 principal. The
instance holds only the credentials the catalog vends to that principal, so a
statement cannot reach another tenant's data. That is the `dedicated` tier, and
it is what lets a [dynamic-read](../data-events/dynamic-read.md) route run
compiled statements here:

```python
from datetime import timedelta

from forze.application.contracts.secrets import SecretRef
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze_duckdb import (
    DuckDbDepsModule,
    DuckDbDynamicReadConfig,
    LakeTenantCatalog,
    RoutedDuckDbClient,
)

lake = RoutedDuckDbClient(
    secrets=secrets,                       # SecretsPort; holds LakeCatalogCredentials
    secret_ref_for_tenant=lambda t: SecretRef(path=f"tenants/{t}/lake"),
    tenant_provider=current_tenant,
    catalog_for_tenant=lambda t: LakeTenantCatalog(
        uri="http://lakekeeper:8181/catalog",
        warehouse=f"wh-{t}",
        oauth2_server_uri="http://idp/realms/lake/protocol/openid-connect/token",
        oauth2_scope="lakekeeper",
        storage_prefix=f"s3://lake/{t}/",
    ),
    extension_directory="/opt/duckdb/extensions",   # httpfs + iceberg, baked into the image
)

widgets = DuckDbDynamicReadConfig(
    provenance="trusted",                  # the only tier admitted
    namespace="gold",                      # USE lake.gold before every statement
    statement_timeout=timedelta(seconds=5),
)

deps = DepsRegistry.from_modules(DuckDbDepsModule(client=lake, dynamic_reads={"widgets": widgets}))
lifecycle = LifecyclePlan.from_steps(routed_client_lifecycle_step("duckdb_lake", client=lake))
```

The secret holds `LakeCatalogCredentials`: `client_id` and `client_secret`, never
a static token, because a token is never refreshed. An empty `client_id` is
refused on first use, since the bundled `iceberg` build never refreshes a token
issued for one.

Each instance is built by one fixed sequence and serves nothing until it has
read the result back:

```text
connect(":memory:", persistent secrets off, extension autoload/autoinstall off,
        temp_directory '', extension_directory)
LOAD httpfs; LOAD iceberg                       -- never INSTALL at runtime
SET GLOBAL enable_global_s3_configuration=false; s3 keys cleared
SET GLOBAL allowed_directories=[storage_prefix]; enable_external_access=false
CREATE SECRET (TYPE iceberg, client credentials, scope)
ATTACH warehouse AS lake (vended_credentials, READ_ONLY)
SET GLOBAL disabled_filesystems='LocalFileSystem,HTTPFileSystem'
SET GLOBAL http_timeout; HTTP metadata and external file caches off
SET GLOBAL lock_configuration=true
attest                                          -- settings, secrets, databases, filesystems
```

A session that does not attest raises `duckdb_session_attestation_failed`
(`configuration`) and is closed. Each statement then runs on a fresh cursor:
`USE lake.<namespace>`, DuckDB's own parser checks that the text is exactly one
`SELECT`, the classified statement runs unmodified, and the per-table S3 secret
the catalog vended for it is dropped afterwards.

What the route promises, and who keeps each promise:

| Property | Kept by |
|----------|---------|
| Read-only | the catalog's `select`-only grant and the store; the `READ_ONLY` attach and parser classification are belts |
| One statement | DuckDB's parser, before anything runs |
| Timeout | the client interrupts the cursor. A CPU-bound statement stops at once. A catalog or store request that hangs is not cut short: its worker holds its slot for about 120 s, whatever `http_timeout` says |
| Row cap | the shell fetches cap + 1 rows from a streaming result and refuses the overflow |
| Container | the tenant's instance, principal and vended credentials (`dedicated`) |
| Untrusted statements | refused at wiring (`duckdb_dynamic_read_untrusted_refused`) |

Wiring refuses a dynamic route on a plain `DuckDbClient` (its origin floor is
`namespace`), and an `http_timeout` above the smallest route timeout. Named
analytics routes work on the routed client too and derive `dedicated`; their
SQL names tables as `lake.<namespace>.<table>`.

Operating it:

- **Size `max_concurrent_queries_total` for hung requests.** It bounds
  statements across all tenants (default 8), and peak memory is about that
  many times `engine.memory_limit`. A tenant whose catalog or store stops
  answering holds a slot for about 120 s per stuck request.
- **Call `evict_tenant` after a grant is restored or a namespace is created.**
  An instance that once found a namespace missing keeps it missing for its
  life. Rotate credentials the same way, or set `fingerprint_ttl`.
- **A revocation lands at the next statement**, or after the authorizer's
  check cache expires: OpenFGA caches checks for 10 s when its cache is on.
- **The deployment owns the outer controls.** The worker must hold no ambient
  cloud credentials, write nowhere it should not, and egress only to the
  catalog, the IdP and the store. An `s3://` path inside the tenant's prefix
  with no vended secret behind it goes, unsigned, to the public AWS endpoint.
- **Keep `sts-token-validity-seconds` well above the longest statement.**
  The engine does not re-vend a credential that expires mid-statement.

## What it provides

| Contract | Keyed by |
|----------|----------|
| Analytics query (`run` / `run_page` / `run_cursor` / `run_chunked`) | `AnalyticsSpec.name` |
| Dynamic read (`run` / `select`), on `RoutedDuckDbClient` only | `DynamicReadSpec.name` |

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

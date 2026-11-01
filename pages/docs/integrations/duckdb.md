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

The extra installs DuckDB 1.5 or 2 (`duckdb>=1.5.6,<3`). Both clients run on
both majors with the same code. The repository's lock tests on 2.0.0, and CI
runs the DuckDB suites on 1.5.6 as well. Where the per-tenant lake client
behaves differently on the two, the section on it says so.

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
instance holds that principal's client secret for its whole life, and the S3
credentials the catalog vends for a statement until the statement ends. Neither
reaches another tenant's data. That is the `dedicated` tier, and
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
        uri="https://lakekeeper:8181/catalog",
        warehouse=f"wh-{t}",
        oauth2_server_uri="https://idp/realms/lake/protocol/openid-connect/token",
        oauth2_scope="lakekeeper",
        storage_prefix=f"s3://lake/{t}/",
    ),
    extension_directory="/opt/duckdb/extensions",   # httpfs, iceberg, avro: in the image
    ca_cert_file="/etc/ssl/certs/ca-certificates.crt",   # HTTPS on DuckDB 2 reads it
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
refused on first use, since the `iceberg` build for DuckDB 1.5 never refreshes a
token issued for one.

Each instance is built by one fixed sequence and serves nothing until it has
read the result back:

```text
connect(":memory:", persistent secrets off, extension autoload/autoinstall off,
        temp_directory '', extension_directory)
LOAD httpfs; LOAD iceberg                       -- never INSTALL; iceberg also loads avro
SET GLOBAL enable_global_s3_configuration=false; s3 keys cleared
SET GLOBAL ca_cert_file                         -- if configured
SET GLOBAL allowed_directories=[storage_prefix, catalog uri/]
SET GLOBAL allowed_paths=[token endpoint, ca_cert_file]
SET GLOBAL enable_external_access=false
CREATE SECRET (TYPE iceberg, client credentials, scope)
ATTACH warehouse AS lake (vended_credentials, READ_ONLY)
SET GLOBAL http_timeout; HTTP metadata and external file caches off
SET GLOBAL lock_configuration=true
attest                                          -- settings, secrets, databases, refusals
```

The allowlist is the one bound inside the engine on what can be opened, local
or remote. Anything outside it is refused before any request: another
tenant's prefix, a local path, any other URL. It names the catalog and the
IdP because DuckDB 2 checks the `iceberg` extension's own requests against
it. Analytics SQL on the instance can therefore also send GETs into the
catalog's tree, which every tenant shares, and to the token endpoint. They
carry no token, and the catalog and the IdP refuse them. The allowlist
matches a path by its spelling: it resolves no `..`, and it does not re-check
where a redirect leads. No filesystem is disabled: DuckDB 2 reaches neither the
catalog with `HTTPFileSystem` off nor its CA bundle with `LocalFileSystem` off.
For HTTPS to the catalog, the IdP or the store, set `ca_cert_file` to a PEM
bundle: DuckDB 2 reads it through the allowlist, and without it the session does
not build. Any statement on the instance can read that file. The same sequence
runs on DuckDB 1.5.

What keeps credentials in is not the allowlist but their being unreadable:
`duckdb_secrets()` stays redacted, the `s3_*` options are cleared, no secret
persists, and the lock keeps it so. The attestation reads each of these back,
and checks by effect that a local path, an unlisted URL, the token endpoint
with a path or query added, a sibling of the prefix or the catalog tree, a file
beside the CA bundle, and a write to `lake` are all refused.

A session that does not attest raises `duckdb_session_attestation_failed`, and one
whose sequence fails (an extension missing from the image, a principal the catalog
refuses) raises `duckdb_session_setup_failed`. Both are `configuration` errors, and
the instance is closed. A setup error carries the engine's message with the
tenant's client secret and DuckDB's quote of the statement removed, and does not
chain the engine's error. Each statement then runs on a fresh cursor:
`USE lake.<namespace>`, DuckDB's own parser checks that the text is exactly one
`SELECT`, the classified statement runs unmodified, and a result with two columns
of the same name is refused. A dynamic read may read only tables it names
(`dynamic_read_statement_invalid` otherwise):

- no table function but `range`, `generate_series` and `unnest`. The others take
  a path, a URL, a table name or SQL text (`read_parquet`, `iceberg_scan`,
  `query`, `query_table`), and `PRAGMA` is refused on 1.5 as on 2;
- no path or URL as a table name, so nothing a statement writes reaches the
  allowlist's spelling match. Parsing binds nothing, so any name holding `/`,
  `:` or `.` counts as one: a table whose own name has a dot cannot be read
  dynamically;
- no table named through a catalog the instance has not attached, or nested
  deeper than `catalog.schema.table`: DuckDB 2 would read it as a nested
  namespace and resolve only its last level, a table the statement did not name.

After every read, dynamic or analytics, the
per-table S3 secrets the catalog vended for it are dropped. On DuckDB 1.5 a
kept secret outranks the next statement's fresh one, so once it expired every
later read of that table would fail. DuckDB 2 discards it when the statement
ends, and the drop finds nothing. If a drop fails, the read's own rows or error still come
back, and the instance is retired: it refuses further statements
(`duckdb_instance_retired`), and the next call builds a fresh one.

What the route promises, and who keeps each promise:

| Property | Kept by |
|----------|---------|
| Read-only | the catalog's `select`-only grant and the store; the `READ_ONLY` attach and parser classification are belts |
| One statement | DuckDB's parser, before anything runs |
| Timeout | the client interrupts the cursor, also when the caller is cancelled. A CPU-bound statement stops at once. A catalog or store request that hangs is not cut short: the caller still gets `dynamic_read_timeout` on time, but the worker holds its slot until the request ends. On DuckDB 2 that is `http_timeout` × (1 + `http_retries`, default 3); on 1.5 about 120 s, whatever `http_timeout` says |
| Row cap | the shell fetches cap + 1 rows from a streaming result and refuses the overflow |
| Container | the tenant's instance, principal and vended credentials (`dedicated`) |
| Untrusted statements | refused at wiring (`duckdb_dynamic_read_untrusted_refused`); a `provenance` other than `"trusted"` or `"untrusted"` is `duckdb_dynamic_read_provenance_invalid` |

Wiring refuses a dynamic route on a plain `DuckDbClient` (its origin floor is
`namespace`), and an `http_timeout` above the smallest route timeout. Named
analytics routes work on the routed client too and derive `dedicated`; their
SQL names tables as `lake.<namespace>.<table>`. A nested namespace is written
dotted, `namespace="gold.sub"`. DuckDB lists it as one schema, so SQL names its
tables bare or as `lake."gold.sub".<table>`.

Operating it:

- **Size `max_concurrent_queries_total` for hung requests.** It bounds
  statements across all tenants (default 8), and peak memory is about that
  many times `engine.memory_limit`. A tenant whose catalog or store stops
  answering holds a slot per stuck request: `http_timeout` × (1 +
  `http_retries`) on DuckDB 2, about 120 s on 1.5. Waiting for a slot counts
  against a statement's timeout.
- **Call `evict_tenant` after a grant is restored or a namespace is created.**
  An instance that once found a namespace missing keeps it missing for its
  life. Rotate credentials or remap a tenant's catalog the same way, or set
  `fingerprint_ttl`. An evicted instance closes in the background, and
  `close()` waits for those teardowns.
- **A revocation lands at the next statement**, or after the authorizer's
  check cache expires: OpenFGA caches checks for 10 s when its cache is on.
- **The deployment owns the outer controls.** The worker must hold no ambient
  cloud credentials, write nowhere it should not, and egress only to the
  catalog, the IdP and the store. An `s3://` path inside the tenant's prefix
  with no vended secret behind it goes, unsigned, to the public AWS endpoint.
  The engine follows a redirect from any of the three without re-checking it,
  and the store, not the allowlist, refuses a `..` key outside the vended
  credential's table.
- **Keep `sts-token-validity-seconds` well above the longest statement.**
  The engine does not re-vend a credential that expires mid-statement: the
  statement fails with an HTTP 400 from the store. A statement that starts
  after expiry loads the table again and gets a fresh credential.

## A DuckLake per tenant

A tenant's lake can also be a [DuckLake](https://ducklake.select/): no REST
catalog and no IdP, just a metadata schema in a Postgres database, read with the
tenant's own role, and Parquet files under a data path. Return a
`DuckLakeTenantCatalog` from `catalog_for_tenant` and keep everything else
above, routes and wiring included:

```python
from forze_duckdb import DuckLakeTenantCatalog, RoutedDuckDbClient

lake = RoutedDuckDbClient(
    secrets=secrets,                       # SecretsPort; holds DuckLakeCatalogCredentials
    secret_ref_for_tenant=lambda t: SecretRef(path=f"tenants/{t}/ducklake"),
    tenant_provider=current_tenant,
    catalog_for_tenant=lambda t: DuckLakeTenantCatalog(
        host="postgres",
        port=5432,
        database="app",
        metadata_schema=f"t{t.hex}_lake",
        data_path=f"/var/lib/lake/{t}/",   # or s3://lake/<tenant>/
    ),
    extension_directory="/opt/duckdb/extensions",   # ducklake, postgres (+ httpfs): in the image
)
```

The secret holds `DuckLakeCatalogCredentials`: `user` and `password` for the
tenant's metadata role and, for an `s3://` data path, `store`, an S3 key in the
`S3SecretPayload` shape. An empty role or password is refused
(`duckdb_lake_role_credentials_empty`), since libpq would fall back to the OS
user or the process's own credentials. A store key on a local path, or none on
an `s3://` one, is refused (`duckdb_lake_store_credentials_mismatch`). One client can serve
Iceberg tenants and DuckLake tenants side by side: each tenant's catalog picks
the shape of its secret.

Each instance is built by this sequence, on DuckDB 1.5 and 2:

```text
connect(":memory:", persistent secrets off, extension autoload/autoinstall off,
        temp_directory '', extension_directory)
LOAD ducklake; LOAD postgres [; LOAD httpfs]    -- never INSTALL; httpfs for s3://
[SET GLOBAL enable_global_s3_configuration=false; s3 keys cleared]
SET GLOBAL ca_cert_file                         -- if configured, s3:// only
SET GLOBAL allowed_directories=[data_path]
SET GLOBAL allowed_paths=[ca_cert_file]         -- empty on a local data path
CREATE SECRET (TYPE postgres, host, port, database, user, password)
[CREATE SECRET (TYPE s3, the store key, SCOPE data_path)]
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH, METADATA_SCHEMA, READ_ONLY)
SET GLOBAL enable_external_access=false
[SET GLOBAL http_timeout; HTTP metadata cache off]
SET GLOBAL enable_external_file_cache=false
SET GLOBAL lock_configuration=true
attest                                          -- settings, secrets, databases,
                                                -- refusals, the role's grants
```

It differs from the Iceberg sequence where DuckLake needs it to:

- **It attaches before external access goes off.** The `postgres` extension
  refuses to attach once it is off. A held instance still sees what the build
  commits later, new tables included, with no refresh.
- **The password lives in an unnamed `postgres` secret, never in the attach
  string.** `duckdb_databases()` hands the attach string to any statement, and
  the attestation checks that it is the bare `postgres:`. A statement sees the
  secret only redacted.
- **The role's grants bound the metadata database.** A statement can query it
  as the role through `postgres_query('__ducklake_metadata_lake', …)` and read
  `__ducklake_metadata_lake.<schema>.*`, both in a read-only transaction. The
  attestation refuses the session when the role:
    - can write any table, view or sequence in its schema, or create in it;
    - can read another DuckLake's metadata, which can hold that lake's inlined
      rows;
    - can read anything else outside its schema that `PUBLIC` cannot, by a
      column grant too;
    - finds a view in its schema that reads outside it, directly, through
      another view or through a function, since a view runs with its owner's
      rights;
    - is a superuser.

  Grant the role `USAGE` on its schema and `SELECT` on its tables, and nothing
  else. A `SECURITY DEFINER` function it may execute reaches past all of this,
  so keep none callable by it.
- **`ATTACH` adds four paths named `postgres:…` in the working directory to
  `allowed_paths`.** A later `SET` does not remove them, so a statement can read
  and write those four files. The attestation expects exactly them.

A dynamic read refuses all of this already: `postgres_query`, `postgres_scan`
and the `ducklake_*` functions are table functions outside its list, and the
metadata database is a catalog the instance does not list. Analytics SQL, which
the application writes, can call them.

Operating it, beyond the Iceberg list:

- **A local data path must be its own real path,** or the build is refused: the
  allowlist matches the spelling, and the filesystem follows a symlink. Keep any
  symlink out from under it too, since the attestation does not walk the tree.
- **On `s3://`, the store key is what confines reads to the prefix.** The
  engine refuses a `..` in a local path, and the attestation checks that, but
  it sends `s3://<prefix>/../<other>/…` to the store as it is. Scope each
  tenant's key to its own prefix.
- **The engine admits writes under the data path.** The allowlist is not
  read-only: `COPY … TO` a file there succeeds, and on DuckDB 2
  `ducklake_delete_orphaned_files` deletes files there although `lake` is
  attached `READ_ONLY`. Only the outside closes it: a read-only key for an
  `s3://` path, a read-only mount for a local one. A builder writing the same
  directory from the same process cannot have the mount.
- **On DuckDB 1.5, `postgres_scan` and `postgres_attach` take a connection
  string and connect to any Postgres, whatever `enable_external_access` says.**
  libpq then also reads the process's `PGPASSWORD`, `PGPASSFILE`,
  `PGSERVICEFILE` and `~/.pgpass`. DuckDB 2's extension has neither function.
  Run the worker with none of those variables set and no readable `~/.pgpass`,
  and let its egress policy allow only the metadata database and the store.
- **Run the worker in a read-only working directory**, so the four
  `postgres:…` paths cannot be written.
- **Call `evict_tenant` after rotating the role's password or the store key,**
  or set `fingerprint_ttl`, as for an Iceberg principal.

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

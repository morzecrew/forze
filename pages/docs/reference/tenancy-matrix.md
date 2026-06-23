---
title: Tenancy matrix
icon: lucide/users
summary: The tier each integration reaches, the mechanism behind it, and the read-side catch for messaging you consume
---

The narrative is in [Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md); this is
the exhaustive surface — every integration, the tiers it reaches, and how. Tiers are
`tagged` < `namespace` < `dedicated`, classified by the *isolation you get*:

- **`tagged`** — one shared container, a tenant marker you filter on (leakable by a
  forgotten predicate).
- **`namespace`** — a separate key / path / schema / collection / dataset / index per
  tenant; a name boundary a query can't cross.
- **`dedicated`** — a separate instance/connection per tenant (a routed client).

`tenant_aware=True` reaches `tagged` on a column store and `namespace` on a key/path
store (the prefix *is* a separate key). A per-tenant resolver reaches `namespace`; a
routed client reaches `dedicated`.

## Stores you query

A store read under the bound tenant scopes itself — adapters call
`ctx.inv_ctx.get_tenant()` on their own, no handler argument.

| Integration | Port(s) | `tenant_aware` | Resolver (`namespace`) | Routed client (`dedicated`) | Ceiling |
|-------------|---------|----------------|------------------------|-----------------------------|---------|
| Postgres | document, search, analytics | `tagged` (`tenant_id` column) | schema | `RoutedPostgresClient` | `dedicated` |
| Mongo | document, search | `tagged` (column) | collection | `RoutedMongoClient` | `dedicated` |
| Firestore | document | `tagged` (column) | collection | `RoutedFirestoreClient` | `dedicated` |
| DuckDB | analytics (query-only) | `tagged` (column) | — | — | **`tagged`** (in-process) |
| ClickHouse | analytics | `tagged` (column) | database | `RoutedClickHouseClient` | `dedicated` |
| BigQuery | analytics | `tagged` (column) | dataset | `RoutedBigQueryClient` | `dedicated` |
| Meilisearch | search | `tagged` (tenant filter) | per-tenant index | `RoutedMeilisearchClient` | `dedicated` |
| Neo4j | graph | `tagged` (tenant property) | per-tenant database | `RoutedNeo4jClient` † | `dedicated` † |
| Redis | cache, counter, idempotency, lock | `namespace` (key prefix) | per-tenant namespace | `RoutedRedisClient` | `dedicated` |
| S3 / GCS | object storage | `namespace` (path prefix) | per-tenant bucket | routed client | `dedicated` |
| HTTP outbound | http service | — | — | per-tenant credentials (routed) | `dedicated` |

† Neo4j reaches `namespace` with a per-tenant database on one driver (the usual
multi-tenant Neo4j shape); `dedicated` needs a genuinely per-tenant driver/instance,
which routing supports but is the less common deployment.

## Messaging you consume — the read-side catch

A **stream or queue drained in the background** has no ambient tenant — the consumer
isn't inside a tenanted request. So `tenant_aware` on the *resource* (a per-tenant key)
isolates the **write** side automatically, but the **read** side must bind the tenant
itself. How depends on the consumer:

| Resource | Write side | Read side (how to isolate per tenant) |
|----------|-----------|----------------------------------------|
| Stream / Pub/Sub (Redis) | `namespace` via `tenant_aware` key prefix | A **sharded gateway** — `TenantShardedSignalSource` + a `RealtimeShard` per instance binds each assigned tenant and reads its key ([RFC 0007](../integrations/socketio.md#tenant-aware-namespace-tier-gateway)). |
| Queue (RabbitMQ, SQS) | `namespace` via name prefix | Keep the queue **tenant-global** and bind the tenant from the message envelope in the consumer; or run a **per-tenant worker** per queue. A built-in sharded queue worker is not yet shipped. |
| Outbox | **tenant-global** (`tagged` — rows carry `tenant_id`) | The relay drains all rows tenant-less and **binds each row's tenant** as it forwards (`_under_claim_tenant`), so a tenant-aware *destination* still routes per-tenant. A *partitioned* (tenant-aware) outbox needs the sharded relay (`realtime_tenant_relay_lifecycle_step`); it fails closed (`outbox_relay_tenant_unbound`) on the plain relay. |
| Inbox | tenant-global (dedup keyed by id + tenant) | No isolation needed — dedup is by globally-unique id; runs under whatever tenant the gateway binds. |
| Realtime end-to-end | outbox `tagged` → stream `namespace` | The whole path is on the tier ladder: stage (tenant-tagged outbox) → relay (binds per row) → per-tenant stream key → sharded gateway (binds from the key) → tenant-scoped room. Ceiling `namespace`. |
| Durable — Temporal | `namespace` via per-tenant task queue | A worker drains one task queue; per-tenant isolation means a per-tenant queue with a worker assigned to it (operator-managed), or `tagged` with the tenant in the workflow context. |
| Durable — Inngest | — | No route-level marker; isolation only via a routed client (`dedicated`). |

The rule across all of these: **the reader binds the tenant.** A handler-read store
binds it from the request; a background consumer binds it from the resource it was
assigned (a sharded stream key) or from each item it processes (the relay per row).

## Declaring and enforcing a floor

`required_tenant_isolation` on a deps module refuses to wire anything weaker than the
declared tier — checked once at startup, never per request. Each module derives the tier
it reaches from its config (routed client → `dedicated`, resolver → `namespace`,
`tenant_aware` → `tagged`/`namespace` by backend). A floor a backend can **never** reach
fails as a **capability mismatch**, not a silent misconfiguration:

| Backend | Ceiling | A floor above it… |
|---------|---------|-------------------|
| DuckDB (in-process analytics) | `tagged` | fails to wire — no per-tenant routing or container |
| `forze_mock` | `namespace` | fails at `dedicated` — no routed-client equivalent |
| Everything else above | `dedicated` | wireable |

See [Multi-tenancy → Declaring a minimum](../identity-tenancy-enc/multi-tenancy.md#declaring-a-minimum)
for the wiring, and [→ Provisioning](../identity-tenancy-enc/multi-tenancy.md#provisioning-per-tenant-infrastructure)
for creating the per-tenant containers the stronger tiers assume.

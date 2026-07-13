---
title: Tenancy matrix
icon: lucide/users
summary: The tier each integration reaches, the mechanism behind it, and the read-side catch for messaging you consume
---

The narrative is in [Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md); this is
the exhaustive surface — every integration, the tiers it reaches, and how. Tiers
(`TenantIsolationMode`) are `none` < `tagged` < `namespace` < `dedicated`, classified by
the *isolation you get*:

- **`none`** — a deliberately shared resource with no tenant boundary at all.
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

A **stream, queue, or table drained in the background** has no ambient tenant — the
consumer isn't inside a tenanted request. So isolating the *resource* doesn't isolate
the *read*: the consumer must bind the tenant itself.

Here **tenant-global** means the resource is deliberately **shared** across tenants
(≈ tier `none`) — the sensible default for anything a tenant-less worker drains, since
the worker can't bind a tenant to read an isolated one. It is **not** the `tagged` tier:
rows may carry a `tenant_id`, but that's for the worker to **route** each item, not a
filter that isolates the store. Isolating the resource itself moves the worker to a
sharded, per-tenant read.

| Resource | Default & reachable isolation | Read side (how to isolate per tenant) |
|----------|------------------------------|----------------------------------------|
| Stream / Pub/Sub (Redis) | `namespace` via `tenant_aware` key prefix | A **sharded gateway** — `TenantShardedSignalSource` + a `RealtimeShard` per instance binds each assigned tenant and reads its key ([tenant-aware Socket.IO gateway](../integrations/socketio.md#tenancy-and-addressing)). |
| Queue (RabbitMQ, SQS) | `namespace` via name prefix | Keep the queue **tenant-global** and bind the tenant from the message envelope in the consumer; or run a **per-tenant worker** per queue. A built-in sharded queue worker is not yet shipped. |
| Commit-stream (Kafka) | `namespace` via per-tenant topic prefix; `dedicated` via routed client (no `tagged` — a topic has no row filter) | Keep the topic **tenant-global** and bind the tenant from the envelope in the consumer (`bind_tenant_from_headers=True` on the commit-stream consumer — opt-in, headers are untrusted); or run a consumer per namespaced topic. |
| Outbox | **tenant-global** by default; as a Postgres/Mongo table it can reach `tagged`→`dedicated` (see read side) | The plain relay drains a tenant-global outbox cross-tenant and **binds each row's `tenant_id`** as it forwards (`_under_claim_tenant`), so a tenant-aware *destination* routes per-tenant. To isolate the **outbox table itself** (`tenant_aware` and up), use the sharded relay (`realtime_tenant_relay_lifecycle_step`); the plain relay fails closed (`outbox_relay_tenant_unbound`). |
| Inbox | **tenant-global** by default; can be `tenant_aware` | Dedup is keyed by the globally-unique event id, so per-tenant isolation is **optional** (ids can't collide across tenants). Runs under whatever tenant the gateway binds — the shard tenant in namespace mode — so a tenant-aware inbox works there too. |
| Realtime end-to-end | outbox **tenant-global** → stream `namespace` | The whole path stays on the ladder: stage (tenant-global outbox, relay binds per row) → per-tenant stream key → sharded gateway (binds from the key) → tenant-scoped room. The outbox stays shared; only the stream is isolated. Ceiling `namespace`. |
| Durable — Temporal | `namespace` via per-tenant task queue | A worker drains one task queue; per-tenant isolation means a per-tenant queue with a worker assigned to it (operator-managed), or `tagged` with the tenant in the workflow context. |
| Durable — Inngest | `dedicated` (routed) only | No route-level marker; isolation only via a routed client. |

The rule across all of these: **the reader binds the tenant.** A handler-read store
binds it from the request; a background consumer binds it from the resource it was
assigned (a sharded stream key) or from each item it processes (the relay per row).
That's why the **default** for relay/worker-drained resources is tenant-global, and
isolating them is opt-in — it costs a sharded reader.

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

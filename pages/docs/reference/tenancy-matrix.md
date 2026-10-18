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
- **`namespace`** — a separate schema / collection / dataset / bucket / index / namespace
  per tenant, picked by a resolver; a name boundary a query can't cross.
- **`dedicated`** — a separate instance/connection per tenant (a routed client).

`tenant_aware=True` reaches `tagged`, whatever the marker: a column, a property, a key
prefix or a path prefix. A per-tenant resolver on every relation a route names reaches
`namespace`; a routed client reaches `dedicated`. A static relation counts even when the
spec doesn't use it, such as a history table with history off, or an ingest target the
spec never declares. A few routes work differently:

- A Postgres search counts its index as well as its read and heap relations. A Mongo
  search counts only its collection: an Atlas or vector index name is scoped to it.
- A Postgres inbox, idempotency store or outbox, and a Mongo outbox, pass their resolver
  the tenant only when they are `tenant_aware`, so only then does a per-tenant table count.
- A Postgres hub or federated search never reaches `namespace`. A federated search is
  `tagged` only when it is `tenant_aware` and every leg is `tenant_aware` or per-tenant;
  otherwise it is `none`.
- Wiring rates the switch, not what it builds: a Redis per-tenant `namespace` rates
  `namespace`, though its keys share one keyspace with every other tenant's, and a
  `tenant_aware` queue rates `tagged`, though each tenant gets a queue of its own.
- A Redis stream or pub/sub route has no per-route namespace, so it never reaches
  `namespace` either; a Kafka route has no `tagged` tier.

## Stores you query

A store read under the bound tenant scopes itself — adapters call
`ctx.inv_ctx.get_tenant()` on their own, no handler argument.

| Integration | Port(s) | `tenant_aware` | Resolver (`namespace`) | Routed client (`dedicated`) | Ceiling |
|-------------|---------|----------------|------------------------|-----------------------------|---------|
| Postgres | document, search, analytics, counter | `tagged` (`tenant_id` column) | schema | `RoutedPostgresClient` | `dedicated` |
| Postgres | idempotency | `tagged` (key prefix) | schema, on a tenant-aware store | `RoutedPostgresClient` | `dedicated` |
| Postgres | [dynamic read](../data-events/dynamic-read.md) | **refused** ‡ | schema (`query_schema`) | `RoutedPostgresClient` | `dedicated` |
| Mongo | document, search, counter | `tagged` (column) | collection | `RoutedMongoClient` | `dedicated` |
| Firestore | document, counter | `tagged` (column) | collection | `RoutedFirestoreClient` | `dedicated` |
| DuckDB | analytics (query-only) | `tagged` (column) | — | — | **`tagged`** (in-process) |
| ClickHouse | analytics | `tagged` (column) | database | `RoutedClickHouseClient` | `dedicated` |
| BigQuery | analytics | `tagged` (column) | dataset | `RoutedBigQueryClient` | `dedicated` |
| Meilisearch | search | `tagged` (tenant filter) | per-tenant index | `RoutedMeilisearchClient` | `dedicated` |
| Neo4j | graph | `tagged` (tenant property) | per-tenant database | `RoutedNeo4jClient` † | `dedicated` † |
| Redis | cache, counter, idempotency, lock | `tagged` (key prefix) | per-tenant namespace | `RoutedRedisClient` | `dedicated` |
| Redis | [fleet membership](../running-in-prod/fleet-membership.md) | **refused** § | **refused** § | — | outside the floor § |
| S3 / GCS | object storage | `tagged` (path prefix) | per-tenant bucket | routed client | `dedicated` |
| HTTP outbound | http service | — | — | per-tenant credentials (routed) | `dedicated` |
| Served models (HTTP) | inference | `tagged` (bound tenant required) | per-tenant `model_name` | `RoutedInferenceHttpClient` | `dedicated` |
| SageMaker | inference | `tagged` (bound tenant required) | per-tenant `endpoint_name` | `RoutedSageMakerRuntimeClient` | `dedicated` |

‡ The dynamic-read plane declares `origin="compiled"`, so its floor is `namespace`
and both `tagged` and `none` are refused as `statement_origin_isolation_floor`. Its
statements are written at runtime, so an isolating predicate cannot be reviewed,
cannot be checked at wiring, and when it is missing the read *succeeds* with another
tenant's rows. A per-tenant schema is the weakest container that does the scoping the
statement cannot be trusted to do.

§ A fleet is one service's replicas, whatever tenants they serve, and it beats from lifecycle
hooks with no tenant bound. A fleet route that is `tenant_aware` or resolves its namespace per
tenant is refused when configured (`fleet.tenant_scoped_route`), and fleet routes are left
out of the module's isolation floor. So are lock and pub/sub routes marked
`tenant_global=True`, for a singleton or leadership lock and an outbox wake; such a route is
refused at wiring when it is `tenant_aware`, resolves its namespace per tenant, or sits on a
routed client. The flag is the deployment's own word, not checked against what the route
holds: put only deployment-wide state on it, such as a lock or a wake, never a tenant's
data. A deployment whose Redis client is routed per tenant registers the lock or pub/sub
dep keys itself, over a `RedisDistributedLockAdapter` or `RedisPubSubAdapter` on a plain
`RedisClient`, as it does for a fleet.

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
| Stream / Pub/Sub (Redis) | `tagged` via `tenant_aware` key prefix | A **sharded gateway** — `TenantShardedSignalSource` + a `RealtimeShard` per instance binds each assigned tenant and reads its key ([tenant-aware Socket.IO gateway](../integrations/socketio.md#tenancy-and-addressing)). |
| Queue (RabbitMQ, SQS) | `tagged` via `tenant_aware` (a queue of its own per tenant, rated `tagged`); `namespace` via a per-tenant `namespace` | Keep the queue **tenant-global** and bind the tenant from the message envelope in the consumer; or run a **per-tenant worker** per queue. A built-in sharded queue worker is not yet shipped. |
| Commit-stream (Kafka) | `namespace` via per-tenant topic prefix; `dedicated` via routed client (no `tagged` — a topic has no row filter, so a fixed prefix counts as `none`) | Keep the topic **tenant-global** and bind the tenant from the envelope in the consumer (`bind_tenant_from_headers=True` on the commit-stream consumer — opt-in, headers are untrusted); or run your own consumer per namespaced topic under that tenant. The shipped commit-stream consumer reads unbound, so it refuses a `tenant_aware` group at startup. |
| Outbox | **tenant-global** by default; as a Postgres/Mongo table it can reach `tagged`→`dedicated` (see read side) | The plain relay drains a tenant-global outbox cross-tenant and **binds each row's `tenant_id`** as it forwards (`_under_claim_tenant`), so a tenant-aware *destination* routes per-tenant. To isolate the **outbox table itself** (`tenant_aware` and up), use the sharded relay (`realtime_tenant_relay_lifecycle_step`); the plain relay refuses it at startup (`tenant_global_required`). |
| Inbox | **tenant-global** by default; can be `tenant_aware` | Dedup is keyed by the globally-unique event id, so per-tenant isolation is **optional** (ids can't collide across tenants). Runs under whatever tenant the gateway binds — the shard tenant on a tenant-aware stream — so a tenant-aware inbox works there too. |
| Realtime end-to-end | outbox **tenant-global** → stream `tagged` (per-tenant key) | The whole path stays on the ladder: stage (tenant-global outbox, relay binds per row) → per-tenant stream key → sharded gateway (binds from the key) → tenant-scoped room. The outbox stays shared; only the stream is isolated. Ceiling `tagged`. |
| Durable — Temporal | `namespace` via per-tenant task queue | A worker drains one task queue; per-tenant isolation means a per-tenant queue with a worker assigned to it (operator-managed), or `tagged` with the tenant in the workflow context. |
| Durable — Inngest | `dedicated` (routed) only | No route-level marker; isolation only via a routed client. |

The rule across all of these: **the reader binds the tenant.** A handler-read store
binds it from the request; a background consumer binds it from the resource it was
assigned (a sharded stream key) or from each item it processes (the relay per row).
That's why the **default** for relay/worker-drained resources is tenant-global, and
isolating them is opt-in — it costs a sharded reader. The shipped relays, sweeps and
consumers check this when they start: run unbound over a `tenant_aware` resource, or over
a client routed per tenant with no tenant to route by, they refuse to start with a
`configuration` error coded `tenant_global_required`. Those are the outbox and realtime
relays, durable recovery and the scheduler, stream trim, mailbox retention,
`job_staleness`, the queue, notification and commit-stream consumers, the plain SSE tail,
and the socket.io gateway over its stream-group or pub/sub source. The singleton guard,
leadership, realtime group ensure, and the Temporal, S3, Mongo index and permission
provider steps check their lock or client the same way. A step of your own is not checked.

## Declaring and enforcing a floor

`required_tenant_isolation` on a deps module refuses to wire anything weaker than the
declared tier — checked once at startup, never per request. Each module derives the tier
it reaches from its config (routed client → `dedicated`, a resolver on every relation a
route names → `namespace`, `tenant_aware` → `tagged`; the exceptions are
listed at the top of this page). Routes that belong to the deployment rather than a tenant
(Redis fleets, and the tenant-global routes in § above) are left out, so a module of only
those meets the floor. A floor a backend can **never** reach fails as a **capability
mismatch**, not a silent misconfiguration:

| Backend | Ceiling | A floor above it… |
|---------|---------|-------------------|
| DuckDB (in-process analytics) | `tagged` | fails to wire — no per-tenant routing or container |
| `forze_mock` | `namespace` | fails at `dedicated` — no routed-client equivalent |
| Everything else above | `dedicated` | wireable |

See [Multi-tenancy → Declaring a minimum](../identity-tenancy-enc/multi-tenancy.md#declaring-a-minimum)
for the wiring, and [→ Provisioning](../identity-tenancy-enc/multi-tenancy.md#provisioning-per-tenant-infrastructure)
for creating the per-tenant containers the stronger tiers assume.

## The floor a route can't lower: statement origin

The tier tells you how strong a route's container is. It doesn't tell you what kind of
text runs inside it, and that's the other half of whether the route is safe. A route
declares its `StatementOrigin`, and its tier has to reach that origin's floor:

| Origin | Who wrote the statement | Floor | The shape it describes |
|--------|-------------------------|-------|------------------------|
| `structured` (default) | the framework, from typed spec elements — the adapter places every predicate | `none` | every plane in the table above |
| `compiled` | a trusted compiler, per request, declaring what it reads | `namespace` | catalog / semantic-layer SQL |
| `raw` | an engine-specific string nothing can rewrite or verify | `dedicated` | a whole-query hatch like `ctx.graph.raw` |

**No shipped route declares an origin yet**, so today this changes nothing you wire —
including the Neo4j raw hatch, which is still governed by `allow_raw_query` and your own
`required_tenant_isolation`, not by the `raw` floor above. The floors bind a route the
moment it declares one; until then the table tells you which tier a route *would* need.

`compiled` floors at `namespace`, not `tagged`, and the reason is worth internalizing
before you argue for the weaker tier: **verification raises confidence in a claim; it
doesn't create a boundary.** A read-set check runs over generated text, so a compiler bug
or a construct the checker renders imprecisely yields a statement that passes the check
and reads what it shouldn't. At `tagged` that's a silent cross-tenant read in a
report that renders perfectly. At `namespace` the same defect names a relation that
doesn't exist, or stays inside the tenant's own container. The floor follows from what
happens *when the check is wrong*, not from how good the check is.

The consequence is architectural: `compiled` at `namespace` means per-tenant containers,
so whatever names those containers is a security-relevant component, not a readability
one. A compiler that bakes namespace names into its artifacts is baking in an isolation
boundary.

Origin composes with `required_tenant_isolation` — a route clears both, and the stronger
requirement is the one that governs. They fail differently on purpose:

| | Comes from | Error code | How you fix it |
|---|---|---|---|
| Origin floor | the kind of text the route runs | `statement_origin_isolation_floor` | strengthen the container, or stop running that text here |
| Declared floor | `required_tenant_isolation` on the module | `<backend>_tenancy_validation_failed` | strengthen the container, or lower what you declared |

When both are unmet you get the origin one, because it's the floor you can't negotiate
with — lowering your declaration would leave the route failing anyway.

Origin is a **wiring** fact, checked once at startup. No statement is ever parsed to infer
where it came from.

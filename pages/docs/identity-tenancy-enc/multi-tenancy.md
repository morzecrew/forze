---
title: Multi-tenancy
icon: lucide/users
summary: Three separate questions — who is calling, which tenant they belong to, and how their data stays apart
---

When one deployment serves many customers, three questions that sound alike are
actually distinct — and Forze keeps them apart:

- **Who** is calling — the authenticated principal (`AuthnIdentity`).
- **Which tenant** the request belongs to — `TenantIdentity`.
- **How** that tenant's data stays isolated from everyone else's.

Keeping these separate is what lets the *same* handler serve every tenant
without a line of tenant-handling code in it.

## Binding the tenant to a request

At the boundary — HTTP middleware, typically — a request is authenticated, its
tenant resolved, and both bound to the execution context:

1. **Authenticate** into an `AuthnIdentity` (plus an optional hint, such as a JWT
   `tid` claim).
2. **Resolve** the `TenantIdentity`. A resolver validates any hints against the
   principal's actual memberships — it is authoritative, so a hint can never
   grant access the principal doesn't have.
3. **Bind** both with `ctx.inv_ctx.bind(...)`, so every adapter can call
   `ctx.inv_ctx.get_tenant()` on its own.

From there, adapters read the tenant themselves. A handler never threads a
`tenant_id` through its arguments — it just asks for the `orders` port and gets
*this* tenant's orders.

!!! warning "Don't authenticate against tenant-scoped data"

    Credential checks run *before* the tenant is resolved. If an authentication
    route reads a tenant-scoped store, bootstrap can deadlock. Keep the
    document routes used during authentication tenant-unaware.

## Where to draw the boundary

Isolation is **layered** — you choose where one tenant's data ends and the
next's begins. The same `TenantIdentity` can decide a marker on each row, the
namespace it lives in, or the whole connection. Forze names these three tiers,
weakest to strongest, and every integration reports which one its wiring
actually reaches:

![A bound TenantIdentity can isolate with a tenant marker, a per-tenant namespace, or a dedicated instance](../_diagrams/light/tenancy-isolation.svg#only-light){ data-src="../_diagrams/light/tenancy-isolation.svg#only-light" }
![A bound TenantIdentity can isolate with a tenant marker, a per-tenant namespace, or a dedicated instance](../_diagrams/dark/tenancy-isolation.svg#only-dark){ data-src="../_diagrams/dark/tenancy-isolation.svg#only-dark" }

| Tier | What tenants share | Reached by |
|------|--------------------|-----------|
| `tagged` | one container — every record carries its tenant, a *filter* a forgotten predicate can leak past | `tenant_aware=True` on a **column** store (a SQL `tenant_id` column, a graph property) |
| `namespace` | a separate container each on one instance — a *name boundary* a query can't cross | `tenant_aware=True` on a **key/path** store (a Redis key prefix, an object-store path prefix), **or** a per-tenant resolver (a Postgres schema, a Mongo collection, a dataset, a bucket, an index) |
| `dedicated` | nothing — a separate instance per tenant | a routed client resolving per-tenant credentials |

The names are deliberately storage-agnostic — they classify by the *isolation
you get*, not the switch you flip. The jump that matters is `tagged` →
`namespace`: a `tagged` marker is a *filter* a forgotten predicate can leak past
(a SQL `tenant_id` column, a graph property — table partitioning included, since
pruning still relies on the marker), whereas a `namespace` is a separate
container — a *name-resolution boundary* a query cannot cross.

So `tenant_aware=True` lands on a **different tier depending on the backend**: a
column store gains a filter (`tagged`), but a key/path store gains a separate key
or path (`namespace`) — the prefix *is* the boundary. A per-tenant resolver, and
routed clients, reach `namespace` and `dedicated` the same way everywhere.

### The tenant marker (`tagged`)

The lightest cut: one connection, one shared container, a tenant marker.
`tenant_aware=True` on a **column** store makes the adapter filter every read and
stamp every write with the bound tenant — a `tenant_id` column on Postgres or
Mongo, a property on a graph node. Correctness rests on the marker: a forgotten
predicate leaks. Combining it with a stronger cut is redundant — acceptable as
defense-in-depth, and startup warns when it spots the overlap.

### Per-tenant containers (`namespace`)

A container per tenant — a name boundary a query can't cross. Two ways to reach
it. On a **key/path** store, the *same* `tenant_aware=True` switch is already a
namespace: the adapter prefixes each tenant's key or path (`tenant:{id}:…` on
Redis, a path prefix on object storage, a per-tenant queue/stream key), so a read
addresses a separate key, not a shared one with a filter. On a store with named
containers, point a route's relation — or its named resource, a bucket / dataset
/ index — at a **resolver** instead of a static value, evaluated per request
against the bound tenant:

```python
PostgresDocumentConfig(
    read=lambda tid: (f"tenant_{tid.hex[:8]}", "orders"),
    write=lambda tid: (f"tenant_{tid.hex[:8]}", "orders"),
    bookkeeping_strategy="application",
)
```

Because the name is only known per request, startup schema validation (which
needs fixed names) skips these routes.

### Routed clients (`dedicated`)

A **routed client** resolves credentials per `TenantIdentity` and pools
connections by fingerprint, so tenants that share an endpoint reuse pools. You
swap it in at [wiring](../writing-operation/wiring.md) time — `RoutedPostgresClient` for
`PostgresClient` — and the specs and handlers don't change.

!!! note "Postgres routed clients"

    Set `introspector_cache_partition_key` on the deps module so the schema
    catalog cache partitions by tenant — required when the client is routed.

### Which integration reaches which tier

Each integration derives the tier its wiring actually reaches. The ceiling is set
by the backend — an in-process store can't route a per-tenant connection:

| Integration | Reaches | Via |
|-------------|---------|-----|
| Document — Postgres, Mongo, Firestore | `tagged` → `namespace` → `dedicated` | column · schema/collection resolver · routed client |
| Analytics — Postgres, ClickHouse, BigQuery | `tagged` → `namespace` → `dedicated` | column · dataset/database resolver · routed client |
| Analytics — DuckDB | `tagged` | column (in-process — its ceiling) |
| Search — Meilisearch | `tagged` → `namespace` → `dedicated` | tenant filter · per-tenant index · routed client |
| Graph — Neo4j | `tagged` → `namespace` → `dedicated` | tenant property · per-tenant database · routed client |
| Cache · Stream · Pub/Sub · Lock · Idempotency — Redis | `namespace` → `dedicated` | key prefix · routed client |
| Queue — RabbitMQ, SQS | `namespace` → `dedicated` | name prefix · routed client |
| Object storage — S3, GCS | `namespace` → `dedicated` | path prefix · per-tenant bucket · routed client |
| Outbound HTTP | `dedicated` | per-tenant credentials (routed client) |
| Durable — Temporal | `tagged` → `namespace` → `dedicated` | context marker · per-tenant task queue · routed client |
| Durable — Inngest | `dedicated` | routed client only |
| Realtime (stream-backed) | `namespace` | per-tenant stream key + sharded gateway/relay |

Key/path stores start at `namespace`: there is no shared container to tag, so
`tenant_aware` *is* the per-tenant key. The catch is on the **read** side of
anything you *consume* — see below. The full breakdown, per port, is the
[tenancy reference](../reference/tenancy-matrix.md).

!!! warning "Isolating messaging you consume"

    A store you *query* under the bound tenant scopes itself. A **stream or queue
    you drain in the background** does not: the consumer runs with no ambient
    tenant. Isolating it per tenant means binding the tenant on the read side —
    a **sharded gateway/relay** for realtime (one instance owns a disjoint tenant
    shard, [tenant-aware namespace-tier gateway](../integrations/socketio.md#tenant-aware-namespace-tier-gateway)),
    or a per-tenant worker for a queue. By default the outbox stays **tenant-global**
    (a shared table; rows carry their tenant for *routing*, not isolation) and its
    relay binds each row's tenant as it forwards — so the *outbox* needs no sharding
    even when its destination is per-tenant. Isolating the outbox table itself is
    opt-in, and then the relay must be sharded too.

## Declaring a minimum

Deriving a tier is descriptive. You can also make it *prescriptive*: set
`required_tenant_isolation` on any deps module and wiring refuses to assemble
anything weaker — a fail-closed floor checked once, at startup, never per
request.

```python
PostgresDepsModule(
    client=RoutedPostgresClient(...),
    required_tenant_isolation="dedicated",  # nothing short of a per-tenant connection
)
```

Each module derives the tier it actually reaches from the config it already
carries — a routed client → `dedicated`, a per-tenant resolver → `namespace`,
`tenant_aware` → `tagged` — and raises a clear configuration error when that's
below the floor. A floor a backend can *never* reach (`dedicated` on in-process
DuckDB, or on single-client Neo4j) fails as a **capability mismatch** rather
than a silent misconfiguration, because each integration's ceiling is known.
Leave it unset (the default) and nothing is enforced.

!!! tip "Where the floor earns its keep"

    Untrusted or self-scoping query paths — a raw SQL hatch, an analytics query
    trusted to filter itself — are only as safe as the store underneath them.
    Declaring `required_tenant_isolation="dedicated"` refuses to wire them
    anywhere a shared store could leak.

## Provisioning per-tenant infrastructure

The stronger tiers assume the per-tenant container already exists — a schema, a
dataset, a bucket. Onboarding a tenant should create it; offboarding should tear
it down. `TenantProvisionerPort` is that seam, wired through the tenancy module:

```python
from forze.application.integrations.storage import ObjectStorageTenantProvisioner
from forze_identity.tenancy.execution import TenancyDepsModule

TenancyDepsModule(
    tenant_management={"main"},
    tenant_provisioner=ObjectStorageTenantProvisioner(
        client=s3_client,
        bucket=lambda tid: f"tenant-{tid}",
    ),
)
```

`TenantManagementPort.provision_tenant(...)` records the tenant first, then runs
the provisioner — a failure leaves the record for an idempotent retry —
and `deprovision_tenant(...)` runs the inverse. Provisioners are idempotent and
receive the onboarded `TenantIdentity` **explicitly**: it is generally not the
ambient bound tenant, since an admin onboards tenant X without acting as X.
Compose one per integration with `CompositeTenantProvisioner`, wrap a callable
with `FunctionTenantProvisioner`, or ship nothing (`NoopTenantProvisioner`, the
default) and provision out of band. Forze includes
`ObjectStorageTenantProvisioner` (ensures a bucket) and, from `forze_postgres`,
`PostgresSchemaTenantProvisioner` (`CREATE SCHEMA IF NOT EXISTS`) — teardown is
opt-in wherever it would destroy data.

Isolating tenants by data boundary pairs naturally with isolating them by key —
sealing each tenant's data under its own keys is [Encryption](encryption.md).

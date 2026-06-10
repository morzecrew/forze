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
next's begins. The same `TenantIdentity` can decide the connection, the schema,
or just a row filter:

![A bound TenantIdentity can isolate at the database, schema, or row level](../_diagrams/light/tenancy-isolation.svg#only-light){ loading=lazy }
![A bound TenantIdentity can isolate at the database, schema, or row level](../_diagrams/dark/tenancy-isolation.svg#only-dark){ loading=lazy }

| Strategy | Mechanism | Tenants share… |
|----------|-----------|----------------|
| **Database per tenant** | a routed client (`RoutedPostgresClient`) resolves a per-tenant DSN from secrets | nothing — separate connections |
| **Schema per tenant** | a relation resolver on the route | one connection, separate schemas |
| **Shared, row-filtered** | `tenant_aware=True` adds a `tenant_id` column filter | one schema; rows tagged per tenant |

### Routed clients

A **routed client** resolves credentials per `TenantIdentity` and pools
connections by fingerprint, so tenants that share an endpoint reuse pools. You
swap it in at [wiring](wiring.md) time — `RoutedPostgresClient` for
`PostgresClient` — and the specs and handlers don't change.

!!! note "Postgres routed clients"

    Set `introspector_cache_partition_key` on the deps module so the schema
    catalog cache partitions by tenant — required when the client is routed.

### Relation resolvers

For schema-per-tenant, point a route's relation at a resolver instead of a
static `(schema, table)` tuple. It's evaluated per request against the bound
tenant:

```python
PostgresDocumentConfig(
    read=lambda tid: (f"tenant_{tid.hex[:8]}", "orders"),
    write=lambda tid: (f"tenant_{tid.hex[:8]}", "orders"),
    bookkeeping_strategy="application",
)
```

Because the name is only known per request, startup schema validation (which
needs fixed names) skips these routes.

### Row-level

The lightest cut: one connection, one schema, a `tenant_id` column.
`tenant_aware=True` makes the gateway filter every read and stamp every write
with the bound tenant. Combining it with a routed or relation cut is redundant —
acceptable as defense-in-depth, and startup warns when it spots the overlap.

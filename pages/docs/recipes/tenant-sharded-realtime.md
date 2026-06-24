---
title: Tenant-sharded realtime
icon: lucide/split
summary: Trusted per-tenant realtime isolation — each tenant on its own stream, no header trust
---

The default realtime stream is **tenant-global**: one stream carries every tenant's
signals and the tenant rides an (untrusted) header. For **trusted** per-tenant isolation,
put the stream on the [tenancy tier ladder](../identity-tenancy-enc/multi-tenancy.md) —
wire the stream `tenant_aware` so each tenant gets its own key, and consume one loop per
tenant so a signal's tenant is the stream it came from, never a forgeable header. The
[Socket.IO gateway](../integrations/socketio.md#tenancy-and-addressing) automates the
consume side with `TenantShardedSignalSource`; this recipe shows what makes it work end to
end for **durable** signals.

## Wire the stream tenant-aware, keep the outbox global

Wire only the realtime **stream** route `tenant_aware` (each tenant gets its own key). The
**outbox stays tenant-global** — a shared table whose rows are tagged with their tenant —
so the tenant-less relay can drain every tenant from one place:

```python
--8<-- "recipes/realtime_sharded/app.py:setup"
```

## Stage a durable signal under its tenant

A handler stages the signal with no realtime or tenant plumbing — the staging tenant is
ambient, and the outbox row is tagged with it:

```python
--8<-- "recipes/realtime_sharded/app.py:publish"
```

## The relay routes each row to its tenant's stream

The relay runs with **no** tenant bound, yet routes correctly: it binds each row's staged
tenant before appending, so a durable signal lands on that tenant's stream key.

```python
--8<-- "recipes/realtime_sharded/app.py:relay"
```

## Consume one tenant's stream

Binding a tenant resolves the stream adapter to *that tenant's* key, so a per-tenant
consumer only ever sees its own signals — the isolation is the stream's, not a header
check. This is what `TenantShardedSignalSource` runs once per assigned tenant:

```python
--8<-- "recipes/realtime_sharded/app.py:consume"
```

## Notes

- **The outbox stays tenant-global by design.** Only the stream is `tenant_aware`; the
  shared outbox is drained by the standard relay with per-row tenant routing. To partition
  the outbox table itself, wire its route `tenant_aware` too and use the sharded relay
  (`realtime_tenant_relay_lifecycle_step`).
- **In production**, `TenantShardedSignalSource(shard=…)` replaces the hand-written
  per-tenant read: one consume loop per assigned tenant, bound to it. Hand the same
  `RealtimeShard` to the source, the group-ensure step, and (for a partitioned outbox) the
  relay so they can't drift.
- **Assignment, not discovery** — the shard is a fixed snapshot resolved at startup, so
  onboarding a new tenant (or rebalancing) needs a restart. Broker-level enforcement (so a
  rogue producer can't write another tenant's key) is the operator's job (Redis ACLs).

---
title: MongoDB
icon: lucide/leaf
summary: Document storage, search, transactions, outbox, inbox, idempotency, and durable execution on MongoDB
---

`forze[mongo]` implements document storage, search, transaction coordination,
and the transactional outbox, inbox and idempotency store on MongoDB — the
same contracts as Postgres, behind collections instead of tables.

## Install

```bash
uv add 'forze[mongo]'
```

Needs MongoDB. Multi-document transactions and outbox flush require a **replica
set** (or sharded cluster), not a standalone server.

## The client

```python
from forze_mongo import MongoClient

mongo = MongoClient()
```

`RoutedMongoClient` resolves a per-tenant connection — see
[Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).

### Settings

`MongoSettings` builds the URI from parts, so the pieces that are easy to get wrong —
the `mongodb+srv://` scheme, percent-encoded credentials, and `authSource` (very often
`admin` rather than the database you read, which is what turns a correct password into
"auth failed") — stay in this package:

```python
from forze_mongo import MongoSettings

mongo_settings = MongoSettings(host="m.internal", port=27017, auth_source="admin")

mongo_settings.uri     # SecretStr("mongodb://...@m.internal:27017/?authSource=admin")
mongo_settings.config  # MongoConfig, from the client fields that are set
```

See [connection settings](index.md#connection-settings) for the rules every one of these
models follows.

## Wire it

Relations are `(database, collection)` tuples, keyed by spec name:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_mongo import MongoClient, MongoConfig, MongoDepsModule, MongoDocumentConfig, mongo_lifecycle_step

orders_mongo = MongoDocumentConfig(read=("app", "orders"), write=("app", "orders"))

deps = DepsRegistry.from_modules(
    MongoDepsModule(client=mongo, rw_documents={"orders": orders_mongo}, tx={"orders"}),
)
lifecycle = LifecyclePlan.from_steps(
    mongo_lifecycle_step(uri="mongodb://localhost:27017", db_name="app", config=MongoConfig()),
)
```

## What it provides

| Contract | Keyed by | Module arg |
|----------|----------|------------|
| Document query / command | `DocumentSpec.name` | `rw_documents` / `ro_documents` |
| Transactions | route in `tx` | `tx` |
| Search | `SearchSpec.name` | `searches` |
| Outbox | `OutboxSpec.name` | `outboxes` |
| Inbox | `InboxSpec.name` | `inboxes` |
| Idempotency | `IdempotencySpec.name` | `idempotencies` |
| Counter | `CounterSpec.name` | `counters` |
| Durable execution | `durable_step` / `durable_run` / `durable_schedule` | step memo, run store, and cron schedules — optional |

## Notes

- **You own the collections and indexes.** Forze reads existing collections; it
  doesn't create or index them.
- **Transactions and outbox need a replica set** — a standalone `mongod` can't
  open multi-document transactions. The inbox marks messages without one, but
  the exactly-once guarantee (mark rolls back with the handler) only exists
  inside a transaction.
- The inbox needs no index migration: the dedup key is the document `_id`, so
  concurrent marks serialize on it out of the box. `InboxSpec.ttl` is advisory —
  to actually expire old marks, create a TTL index on `processed_at` with your
  dedup window (you own the collections and indexes).
- **Idempotency is co-located**: the result record is written on the caller's
  session, so it commits atomically with the business writes — the crash window
  an out-of-transaction store (Redis) leaves open. Claims and releases run
  detached, so a claim blocks a concurrent duplicate the moment it is taken and
  survives the rollback of the operation it guards. Expired claims are reclaimed
  in place; a TTL index on `expires_at` is optional cleanup for keys that are
  never reused.
- **Durable execution needs one index**, and only one: a partial unique index on
  the run collection's `idempotency_key`
  (`partialFilterExpression: {idempotency_key: {$type: "string"}}`). Without it
  two simultaneous submits of one key can both insert, and the port promises they
  converge on a single run. The step journal needs none — its dedup key is the
  document `_id`. Indexes on `{status: 1, created_at: 1}` and
  `{enabled: 1, next_fire_at: 1}` keep the recovery scan and the scheduler off
  collection scans as the collections grow.
- **Claiming without row locks.** Postgres hands out runs under
  `FOR UPDATE SKIP LOCKED`; Mongo has no equivalent, so the batch claim reads
  candidates and stamps them in one update whose per-document filter still
  requires the run to be claimable — exactly one scanner wins each. A contended
  batch takes fewer runs than it asked for and the next scan catches up; nothing
  is claimed twice. A scanner that dies between claiming and reading back leaves
  those runs leased to nobody — Postgres has no such window, since its claim and
  its result are one statement — and they come back on the next scan once the
  lease expires, so size `lease_for` for how long you can wait, not just for how
  long a body runs.
- `MongoSearchConfig` is imported from `forze_mongo.execution.deps` (not the
  top-level package).
- Relations accept a static `(database, collection)` tuple or a per-tenant
  resolver; routed clients handle database-per-tenant.

---
title: Cloud Firestore
icon: lucide/flame
summary: Document storage and transactions on Google Cloud Firestore
---

`forze[firestore]` implements document storage and transaction coordination on
Google Cloud Firestore, behind the document contracts.

## Install

```bash
uv add 'forze[firestore]'
```

Needs a Firestore database (or the emulator via the `FIRESTORE_EMULATOR_HOST`
environment variable).

## The client

```python
from forze_firestore import FirestoreClient

fs = FirestoreClient()
```

`RoutedFirestoreClient` resolves a per-tenant project/database.

## Wire it

Relations are `(database, collection)` tuples, keyed by spec name:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_firestore import FirestoreClient, FirestoreDepsModule, FirestoreDocumentConfig, firestore_lifecycle_step

orders_fs = FirestoreDocumentConfig(read=("(default)", "orders"), write=("(default)", "orders"))

deps = DepsRegistry.from_modules(
    FirestoreDepsModule(client=fs, rw_documents={"orders": orders_fs}, tx={"orders"}),
)
lifecycle = LifecyclePlan.from_steps(
    firestore_lifecycle_step(project_id="my-project", database="(default)"),
)
```

## What it provides

| Contract | Keyed by | Module arg |
|----------|----------|------------|
| Document query / command | `DocumentSpec.name` | `rw_documents` / `ro_documents` |
| Transactions | route in `tx` | `tx` |
| Counter | `CounterSpec.name` | `counters` |

## Notes

- **You own collections and indexes.** Forze reads existing collections; it
  doesn't create them.
- Relations are `(database, collection)`; the emulator is selected by the
  `FIRESTORE_EMULATOR_HOST` env var.
- Routed clients vary the project/database per tenant (using Application Default
  Credentials).
- **Counters are transactional read-modify-write**, and Firestore sustains roughly
  one write per second per document — a hot counter contends and retries. Allocate
  blocks with `incr_batch` to amortize the ceiling, or route high-rate counters to
  a Redis-backed adapter.
- No graph support — Firestore is documents only.
- No aggregations in the MVP adapter — an `aggregate_*` query (grouped or aggregate
  rows) is rejected up front with a clean `precondition` naming the backend, not a
  500 deep in the gateway. Use Postgres or Mongo where you need them.

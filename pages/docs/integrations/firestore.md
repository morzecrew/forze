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

## Notes

- **You own collections and indexes.** Forze reads existing collections; it
  doesn't create them.
- Relations are `(database, collection)`; the emulator is selected by the
  `FIRESTORE_EMULATOR_HOST` env var.
- Routed clients vary the project/database per tenant (using Application Default
  Credentials).
- No graph support — Firestore is documents only.

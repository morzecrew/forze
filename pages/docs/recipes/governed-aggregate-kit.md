---
title: Declare a governed aggregate
icon: lucide/blocks
summary: One AggregateKit declaration → a persisted, soft-deletable, searchable, invariant-guarded, event-relaying slice
---

Standing up a *governed* aggregate by hand means assembling the same wiring every
time: the document registry, the typed facade, the search index-on-write sync, the
soft-delete read filter, the invariant checks inside the transaction, and the
four-piece outbox dance. Each verb is one call; the composition between them is not.

`AggregateKit` collapses that composition into **one typed declaration**. It composes
*wiring, not models* — you still write the four models — and emits the slice as
**separate** artifacts, so the app and backend layers stay decoupled.

The runnable version lives at `examples/recipes/aggregate_kit/` and runs on the
in-memory mock — no infrastructure needed.

## The four models

The models are yours — the only thing the kit cannot invent. Here `Task` extends the
soft-delete mixin (the precondition for `soft_delete=True`) and emits an event when it
is completed:

```python
--8<-- "recipes/aggregate_kit/app.py:domain"
```

## One declaration

`AggregateKit` bundles the spec with its optional concerns. This one is persisted,
soft-deletable, kept in an external search index on every write, guarded by a
cross-record invariant, and event-relaying:

```python
--8<-- "recipes/aggregate_kit/app.py:kit"
```

Each concern is opt-in and independently useful on its own:

- **`soft_delete`** — the generated `LIST` excludes soft-deleted rows and `GET` 404s
  one; you get `delete`/`restore` ops and an optional after-commit `purge`.
- **`search`** — the external index is kept in sync on every committed write
  ([search stays consistent](../data-events/events-sagas.md)); a searchable aggregate
  that silently drifts is worse than no search.
- **`invariants`** — each [`SystemInvariant`](../writing-operation/system-invariants.md) is enforced
  *preventively* inside the write transaction, at the isolation floor it needs, so a
  write that would break the law is rolled back.
- **`outbox`** — the [transactional outbox](transactional-outbox.md) wiring: the in-tx
  flush, the domain-event → outbox bridge, and the relay step, from one `OutboxEmit`.

## What it emits — separately

The kit never returns a coupled god-object. It emits the app-layer registry, the typed
facade, and the domain-event bridges as distinct artifacts; which store backs it stays
your call, wired into the deps module:

```python
--8<-- "recipes/aggregate_kit/app.py:wiring"
```

- `registry(tx_route=…)` — the composed, frozen operation registry.
- `facade(runtime)` — a per-call, precisely-typed `DocumentFacade` over it
  (`create`/`get`/`list`/`update` keep your `C`/`U`/`R` types — no erosion).
- `domain_events()` — the outbox staging bridges, for the deps module.
- `lifecycle_steps()` — the outbox relay, for the runtime.

Field encryption declared on the spec flows through untouched. Backend config
(`rw_documents=` / `searches=` / `outboxes=`) and HTTP routes stay yours — wire them
over `registry()` with your deps module and the
[route generators](../integrations/fastapi.md) — so the hexagonal layer split holds.

## The escape hatch

The kit gives the governed-CRUD floor; a bespoke lifecycle comes through a first-class
escape hatch, not a fall-off-the-cliff:

```python
kit = AggregateKit(
    spec=TASK_SPEC,
    handlers={DocumentKernelOp.UPDATE: MyCustomUpdate},  # override a generated op's handler
    extra_ops=my_report_registry,                        # merge bespoke operations
)
```

`handlers=` replaces a generated op's handler while keeping the rest of the slice
(routes/facade/relay) intact; `extra_ops=` merges custom operations (a lifecycle
transition, a report) into the composed registry.

## Honest limits

- **It does not reduce the models.** You still write Domain / Create / Update / Read +
  the `DocumentSpec` — Python has no `Partial<T>`/`Pick<T>`, and the four contracts
  genuinely diverge. The kit collapses *wiring*, never the models.
- **It does not invent a lifecycle.** `soft_delete=True` gives the governed-CRUD floor;
  a status machine (like `StoredFileKit`'s pending → ready → failed) comes through the
  escape hatch.
- **It does not couple to a backend.** `registry()` / `facade()` are backend-agnostic;
  you wire the store yourself. No `AggregateKit(...).build_everything(client)`.

## Notes

- The outbox flush is attached to `update` (the op that emits): `@event_emitter` fires
  as an update persists, so a generated `create` never stages. Emit on create/delete
  through the escape hatch.
- Preventive invariants default to `SERIALIZABLE` — the isolation most predicate-over-a-
  read-set laws need to survive write skew. The kit opens the write transaction there.

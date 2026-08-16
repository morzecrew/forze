# AggregateKit

One `AggregateKit` declaration composes a governed vertical slice — CRUD, soft delete, search sync, cross-record invariants and transactional notifications — from models and a spec you already have. Declare those first ([aggregate models](aggregate-models.md), [document and search specs](document-spec.md)); reach for [operation composition](operation-composition.md) only when you need something the kit does not cover.

## Governed aggregates: AggregateKit

For the governed-aggregate case — CRUD plus soft delete, search sync, cross-record invariants, and/or a transactional outbox — **lead with `AggregateKit`** instead of hand-assembling the registries below. One typed declaration composes the wiring (you still write the four models and the `DocumentSpec`; see [document and search specs](document-spec.md)):

```python
from forze_kits.aggregates import AggregateKit
from forze_kits.integrations.outbox import EmitMapping, OutboxEmit, RelayBinding

TASKS = AggregateKit(
    spec=TASK_SPEC,                 # DocumentSpec — field encryption on it flows through
    soft_delete=True,               # requires the is_deleted mixins on the models
    search=TASK_INDEX,              # SearchSpec — index kept in sync on every committed write
    invariants=(PROJECT_BUDGET,),   # SystemInvariant, enforced preventively in the write tx
    outbox=OutboxEmit(
        spec=TASK_EVENTS,
        emits=(EmitMapping(event=TaskCompleted, event_type="task.completed", to_payload=...),),
        relay=RelayBinding(queue_spec=TASKS_QUEUE),
    ),
)
```

The kit emits **separate** artifacts — never a coupled god-object:

- `TASKS.registry(tx_route=...)` — the composed, frozen operation registry.
- `TASKS.facade(runtime, tx_route=...)` — a per-call, precisely-typed `DocumentFacade` factory.
- `TASKS.domain_events()` — the outbox staging bridges, for the deps module (e.g. `MockDepsModule(domain_events=TASKS.domain_events())`).
- `TASKS.lifecycle_steps(tx_route=...)` — the outbox relay (plus the search-sync relay/consumer with durable delivery), for the runtime.
- `TASKS.backend_requirements(tx_route=...)` — a checklist of the routes / tx / keyring the deps module must wire (`crypto_required` is set when the spec declares field encryption); assert it in a startup test.

Pass the **same** `tx_route` everywhere — it must match a transaction route the deps module registers. Backend config (`rw_documents=` / `searches=` / `outboxes=`) stays yours, keyed by the reported route names. HTTP is one call: `attach_aggregate_routes(router, TASKS, ctx_dep=ctx_dep, style="rest", tx_route=...)` from `forze_fastapi.routes` projects the whole slice — see [FastAPI routes](fastapi-generated-routes.md).

Search-sync delivery is a choice:

- **Default (after-commit, at-most-once)** — bounded in-place retry after commit; on exhaustion the index stays stale for that row until its next write (a reconcilable WARNING is logged).
- **Durable (`search_delivery=OutboxSearchSync()`, from `forze_kits.aggregates.search`)** — an identity-only marker staged in the write's transaction, relayed at-least-once, applied by a consumer that re-reads committed state (idempotent, reorder-safe, inbox-deduped). Wire the reported `search_sync_route` — one name for its outbox, queue, and inbox — in the deps module; `lifecycle_steps(tx_route=...)` then carries the relay and consumer.

Composing `soft_delete` + `search`, the kit's search reads exclude soft-deleted rows, so the `SearchSpec` **must declare `is_deleted` in `facetable_fields`** (the external index must be able to filter it) — the kit fails closed at construction otherwise.

The kit is a floor, not a ceiling: `handlers={DocumentKernelOp.UPDATE: MyCustomUpdate}` overrides a generated op's handler; `extra_ops=my_registry` merges bespoke operations; `build_unfrozen()` returns the composed registry for advanced merging before freeze. When the *entire* operation surface is bespoke, hand-wire with the registries below instead.

See the runnable recipe (`examples/recipes/aggregate_kit/` in the Forze repo) and [Declare a governed aggregate](https://morzecrew.github.io/forze/latest/recipes/governed-aggregate-kit/).

## Reference

- [Declare a governed aggregate (AggregateKit)](https://morzecrew.github.io/forze/latest/recipes/governed-aggregate-kit/)

# Execution context

### Execution context

`ExecutionContext` resolves infrastructure by **logical spec** (`spec.name` routes factories). Common helpers:

| API | Returns | Notes |
|-----|---------|--------|
| `ctx.deps.provide(key, route=...)` | `T` | Simple registered dependency |
| `ctx.document.query(spec)` / `ctx.doc.query(spec)` | `DocumentQueryPort` | Reads, listings |
| `ctx.document.command(spec)` / `ctx.doc.command(spec)` | `DocumentCommandPort` | Creates, updates, deletes |
| `ctx.cache(spec)` | `CachePort` | `CacheSpec` |
| `ctx.counter(spec)` | `CounterPort` | `CounterSpec` |
| `ctx.storage.query(spec)` | `StorageQueryPort` | `StorageSpec` (download, list) |
| `ctx.storage.command(spec)` | `StorageCommandPort` | `StorageSpec` (upload, delete) |
| `ctx.search.query(spec)` | `SearchQueryPort` | Full-text search |
| `ctx.search.hub(spec)` | `SearchQueryPort` | Hub search |
| `ctx.search.federated(spec)` | `SearchQueryPort` | Federated search |
| `ctx.graph.query(spec)` / `ctx.graph.command(spec)` | `GraphQueryPort` / `GraphCommandPort` | `GraphModuleSpec` |
| `ctx.inference.model(spec)` | `InferencePort` | `InferenceSpec` — read-plane, callable from a `QUERY` |
| `ctx.transaction(route)` | `TransactionManagerPort` | Transaction route (e.g. `"default"`) |
| `ctx.tx_ctx.scope(route)` | async context manager yielding a handle | Transaction scope; `tx.rollback_only()` for a dry run |
| `ctx.resilience()` | `ResilienceExecutorPort` | Run a call under a named policy — see [resilience and deadlines](resilience.md) |

For configurable keys without a convenience wrapper, use `ctx.deps.resolve_configurable(ctx, DepKey, spec, route=spec.name)`.

**Counters never join your transaction.** `ctx.counter(spec)` allocations commit independently on every backend (Redis, Postgres, Mongo, Firestore) — a rolled-back transaction does not give the number back. That is the point: a sequence that could be rolled back would reissue an id it already handed out. Treat an allocated number as spent, and let gaps happen. Counters resolve through the bound tenant and fold the spec route into the stored key, so two specs sharing one relation keep separate sequences. Postgres additionally needs an app-migrated counter table.

See [Execution reference](https://morzecrew.github.io/forze/latest/writing-operation/wiring/).

### Handler pattern

Handlers implement `Handler[Args, R]` from `forze.application.contracts.execution` and are registered on `OperationRegistry`:

```python
import attrs

from forze.application.contracts.execution import Handler


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetProject(Handler[UUID, ProjectReadModel]):
    doc: DocumentQueryPort[ProjectReadModel]

    async def __call__(self, args: UUID) -> ProjectReadModel:
        return await self.doc.get(args)
```

Factories receive `ExecutionContext` and inject ports: `lambda ctx: GetProject(doc=ctx.document.query(project_spec))`. The `@attrs.define(kw_only=True)` gives the handler its keyword constructor — `Handler` is a `Protocol` and provides no `__init__`.

### Transactions

`ctx.tx_ctx.scope(route)` is an **async context manager**. Pass the **same route** registered on your deps module (prefer a shared `StrEnum`, e.g. `TxRoute.DEFAULT`). Nested calls reuse the active transaction (savepoints when supported).

```python
async with ctx.tx_ctx.scope(TxRoute.DEFAULT):
    doc_c = ctx.document.command(project_spec)
    await doc_c.create(cmd1)
    await doc_c.create(cmd2)
```

Use `await ctx.tx_ctx.run_or_defer(callback)` for side effects that must run only after the root transaction commits (runs immediately when no transaction is active).

### Dry runs: roll back on purpose

The scope yields a handle. `tx.rollback_only()` lets the body run to the end, then rolls the
scope back instead of committing, and the caller carries on as after a commit. Nothing logs
above `DEBUG`:

```python
async with ctx.tx_ctx.scope(TxRoute.DEFAULT) as tx:
    report = await import_rows(ctx, rows)

    if dry_run:
        tx.rollback_only()
# rolled back if dry_run; report is still here
```

- **Root scope:** the whole transaction rolls back, and work queued with `run_or_defer`
  never runs.
- **Nested scope:** only its savepoint rolls back, with the work it deferred, and the outer
  scope carries on. That needs savepoints, which the manager reports as
  `TxCapabilities.savepoints`: Postgres and the mock's `transactions="strict"` have them.
  Mongo, Firestore, Neo4j and the mock's default `journal` do not, and `rollback_only()`
  raises `precondition` (`tx_nested_rollback_unsupported`) at the call.
- **A route on another client** opens a transaction of its own that commits when its scope
  exits, which the root cannot undo. Under a root already marked it rolls back too, but once
  one has exited, `rollback_only()` on the root raises `precondition`
  (`tx_rollback_after_independent_commit`). Mark the root before opening such scopes. Routes
  on the same client share the root's transaction.
- **After the scope exits**, `rollback_only()` raises `precondition` (`tx_scope_ended`).
- **An exception after the mark** still propagates and is logged as usual; the mark never
  swallows an error.

The scope rolls back by throwing `RollbackSignal` (from
`forze.application.contracts.transaction`) into the manager. It is a `BaseException`, like a
cancellation: never raise or catch it yourself. A custom `TransactionManagerPort` that rolls
back on cancellation and re-raises needs no change.

Stage hooks use `BeforeStep` / `OnSuccessStep` on `OperationRegistry.bind(...)` — see [Middleware and plans](https://morzecrew.github.io/forze/latest/writing-operation/capability-execution/).

### Identity and tenancy

Bind `AuthnIdentity` / `TenantIdentity` at the HTTP, Socket.IO, queue worker, or Temporal worker boundary:

```python
with ctx.inv_ctx.bind(metadata=metadata, authn=identity, tenant=tenant):
    ...
```

Handlers read `ctx.inv_ctx.get_authn()` / `ctx.inv_ctx.get_tenant()`; they should not call `inv_ctx.bind` themselves. See [authentication](authn.md).

## Anti-patterns

- **Raising through the scope to discard a dry run** — it rolls back, but Postgres logs it as an error with a traceback. Call `tx.rollback_only()`.
- **Calling `rollback_only()` after a scope on another client's route has exited** — its commit already happened and is refused; mark the root first.

## Reference

- [Execution reference](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)
- [Transactions and dry runs](https://morzecrew.github.io/forze/latest/writing-operation/transactions/)

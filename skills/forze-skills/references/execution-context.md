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
| `ctx.tx_ctx.scope(route)` | async context manager | Transaction scope |
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

Stage hooks use `BeforeStep` / `OnSuccessStep` on `OperationRegistry.bind(...)` — see [Middleware and plans](https://morzecrew.github.io/forze/latest/writing-operation/capability-execution/).

### Identity and tenancy

Bind `AuthnIdentity` / `TenantIdentity` at the HTTP, Socket.IO, queue worker, or Temporal worker boundary:

```python
with ctx.inv_ctx.bind(metadata=metadata, authn=identity, tenant=tenant):
    ...
```

Handlers read `ctx.inv_ctx.get_authn()` / `ctx.inv_ctx.get_tenant()`; they should not call `inv_ctx.bind` themselves. See [authentication](authn.md).

## Reference

- [Execution reference](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)

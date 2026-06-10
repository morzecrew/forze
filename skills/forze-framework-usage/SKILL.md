---
name: forze-framework-usage
description: >-
  Guides correct use of Forze ExecutionContext, document/query/command ports,
  search, cache, counters, storage, messaging, workflows, identity, and
  transactions in handlers. Use when implementing features or handlers with
  Forze specs, hexagonal ports, or runtime context.
---

# Forze Framework Usage

Use when writing code that **consumes** Forze (not when authoring new adapters). Pair with [`forze-domain-aggregates`](../forze-domain-aggregates/SKILL.md) for models/specs, [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md) for `StrEnum` route names, and [`forze-wiring`](../forze-wiring/SKILL.md) for runtime and composition.

## Core concepts

### Layered architecture

Dependencies flow **inward**:

- **Domain** — pure business logic, no external deps
- **Application** — handlers, contracts, composition; imports domain only
- **Infrastructure** — adapters implementing contracts
- **Interface** — HTTP, WebSocket; resolves handlers via frozen registry + context

Handlers and domain models **never** import adapter classes or infrastructure packages.

### Contracts and adapters

The application declares **what** it needs via protocol interfaces (contracts). Infrastructure provides **how** (adapters). Resolve ports from `ExecutionContext`; never import adapters into handlers.

```python
# Correct: resolve port from context
doc_q = ctx.document.query(project_spec)
result = await doc_q.get(some_id)

# Wrong: importing adapter
from forze_postgres.adapters.document import PostgresDocumentAdapter  # Never in handlers
```

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
| `ctx.tx_ctx.resolver(route)` | `TransactionManagerPort` | Transaction route (e.g. `"default"`) |
| `ctx.tx_ctx.scope(route)` | async context manager | Transaction scope |

For configurable keys without a convenience wrapper, use `ctx.deps.resolve_configurable(ctx, DepKey, spec, route=spec.name)`.

See [Execution reference](https://morzecrew.github.io/forze/in-depth/wiring/).

### Handler pattern

Handlers implement `Handler[Args, R]` from `forze.application.contracts.execution` and are registered on `OperationRegistry`:

```python
from forze.application.contracts.execution import Handler

class GetProject(Handler[UUID, ProjectReadModel]):
    doc: DocumentQueryPort[ProjectReadModel]

    async def __call__(self, args: UUID) -> ProjectReadModel:
        return await self.doc.get(args)
```

Factories receive `ExecutionContext` and inject ports: `lambda ctx: GetProject(doc=ctx.document.query(project_spec))`.

### Transactions

`ctx.tx_ctx.scope(route)` is an **async context manager**. Pass the **same route** registered on your deps module (prefer a shared `StrEnum`, e.g. `TxRoute.DEFAULT`). Nested calls reuse the active transaction (savepoints when supported).

```python
async with ctx.tx_ctx.scope(TxRoute.DEFAULT):
    doc_c = ctx.document.command(project_spec)
    await doc_c.create(cmd1)
    await doc_c.create(cmd2)
```

Use `ctx.tx_ctx.defer_after_commit()` for side effects that must run only after the root transaction commits.

Stage hooks use `BeforeStep` / `OnSuccessStep` on `OperationRegistry.bind(...)` — see [Middleware and plans](https://morzecrew.github.io/forze/in-depth/capability-execution/).

### Identity and tenancy

Bind `AuthnIdentity` / `TenantIdentity` at the HTTP, Socket.IO, queue worker, or Temporal worker boundary:

```python
with ctx.inv_ctx.bind(metadata=metadata, authn=identity, tenant=tenant):
    ...
```

Handlers read `ctx.inv_ctx.get_authn()` / `ctx.inv_ctx.get_tenant()`; they should not call `inv_ctx.bind` themselves. See [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md).

## Query syntax

Filters use the shared DSL: `{"$values": {...}}`, `{"$and": [...]}`, `{"$or": [...]}`.

**Field shortcuts:**

| Value | Meaning |
|-------|---------|
| `"active"` | `$eq` |
| `["a", "b"]` | `$in` |
| `null` | `$null: true` |

**Operators:** `$eq`, `$neq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$null`, `$empty`, `$superset`, `$subset`, `$overlaps`, `$disjoint`, `$like`, `$ilike`, `$regex`.

**Sorts:** `{"created_at": "desc", "id": "asc"}`.

```python
filters = {"$values": {"status": "active", "is_deleted": False}}
page = await doc_q.find_page(
    filters=filters,
    pagination={"limit": 20, "offset": 0},
    sorts={"created_at": "desc"},
)
rows, total = page.hits, page.count
```

## Common patterns

### Document reads and writes

```python
doc_q = ctx.document.query(project_spec)
doc_c = ctx.document.command(project_spec)

project = await doc_q.get(doc_id)
page = await doc_q.find_page(filters=..., pagination={"limit": 20, "offset": 0})
rows = page.hits

created = await doc_c.create(CreateProjectCmd(title="New"))
updated = await doc_c.update(doc_id, current_rev, UpdateProjectCmd(title="Updated"))
await doc_c.delete(doc_id, current_rev)
await doc_c.kill(doc_id)
```

### Search

```python
search = ctx.search.query(project_search_spec)
page = await search.search_page(query="roadmap", filters=..., limit=20, offset=0)
hits, total = page.hits, page.count
```

### Counter (e.g. number_id)

```python
from forze.application.contracts.counter import CounterSpec

counter = ctx.counter(CounterSpec(name="tickets"))
next_id = await counter.incr()
```

### Object storage

```python
from forze.application.contracts.storage import StorageSpec, UploadedObject

spec = StorageSpec(name=ResourceName.ATTACHMENTS)
stored = await ctx.storage.command(spec).upload(
    UploadedObject(filename="file.pdf", data=data, description="Contract")
)
downloaded = await ctx.storage.query(spec).download(stored.key)
```

### Queue, pub/sub, stream, and workflow ports

```python
from forze.application.contracts.queue import QueueCommandDepKey

queue = ctx.deps.resolve_configurable(ctx, QueueCommandDepKey, order_queue, route=order_queue.name)
await queue.enqueue("orders", args, type="order.created")
```

See [`forze-messaging-streaming`](../forze-messaging-streaming/SKILL.md) and [`forze-temporal-workflows`](../forze-temporal-workflows/SKILL.md).

## Gotchas

- **`ctx.doc_query` / `ctx.doc_command` are removed** — use `ctx.document.query` / `ctx.document.command`.
- **`ctx.dep(...)` on context is removed** — use `ctx.deps.provide` or `ctx.deps.resolve_configurable`.
- **`ctx.transaction()` is removed** — use `ctx.tx_ctx.scope(route)`.
- **`ctx.counter("name")` is wrong** — pass `CounterSpec(name=...)`.
- **`UsecaseRegistry` is removed** — use `OperationRegistry` + `.freeze()`.
- **Do not nest incompatible tx backends** (e.g. Postgres + Mongo in one scope).

## Anti-patterns

1. Importing adapters in handlers — resolve via `ctx.document.query` / `command` / other ports.
2. Domain importing application or infrastructure — keep domain pure.
3. Raw SQL/ORM in handlers — use document/search/cache/storage ports.
4. Passing unfrozen registry to FastAPI attach — call `.freeze()` after plan binding.

## Reference

- [Execution reference](https://morzecrew.github.io/forze/in-depth/wiring/)
- [Contracts and adapters](https://morzecrew.github.io/forze/core-concepts/contracts/)
- [Query syntax](https://morzecrew.github.io/forze/reference/query-syntax/)
- [Contracts overview](https://morzecrew.github.io/forze/reference/contracts/)

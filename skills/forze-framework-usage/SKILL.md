---
name: forze-framework-usage
description: >-
  Guides correct use of Forze ExecutionContext, document/query/command ports,
  search, cache, counters, storage, messaging, workflows, identity, and
  transactions in usecases. Use when implementing features or usecases with
  Forze specs, hexagonal ports, or runtime context.
---

# Forze Framework Usage

Use when writing code that **consumes** Forze (not when authoring new adapters). Pair with [`forze-domain-aggregates`](../forze-domain-aggregates/SKILL.md) for models/specs, [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md) for `StrEnum` route names, and [`forze-wiring`](../forze-wiring/SKILL.md) for runtime and composition.

## Core concepts

### Layered architecture

Dependencies flow **inward**:

- **Domain** — pure business logic, no external deps
- **Application** — usecases, contracts, composition; imports domain only
- **Infrastructure** — adapters implementing contracts
- **Interface** — HTTP, WebSocket; invokes usecases via context

Usecases and domain models **never** import adapter classes or infrastructure packages.

### Contracts and adapters

The application declares **what** it needs via protocol interfaces (contracts). Infrastructure provides **how** (adapters). Resolve ports from `ExecutionContext`; never import adapters into usecases.

```python
# Correct: resolve port from context
doc_q = self.ctx.doc_query(project_spec)
result = await doc_q.get(some_id)

# Wrong: importing adapter
from forze_postgres.adapters.document import PostgresDocumentAdapter  # Never in usecases
```

### Execution context

`ExecutionContext` resolves infrastructure by **logical spec** (`spec.name` routes factories). Common helpers:

| Method | Returns | Notes |
|--------|---------|--------|
| `dep(key, route=...)` | `T` | Generic resolution by `DepKey` |
| `doc_query(spec)` | `DocumentQueryPort` | Reads, listings |
| `doc_command(spec)` | `DocumentCommandPort` | Creates, updates, deletes |
| `cache(spec)` | `CachePort` | `CacheSpec` |
| `counter(spec)` | `CounterPort` | `CounterSpec` |
| `storage(spec)` | `StoragePort` | `StorageSpec` |
| `search_query(spec)` | `SearchQueryPort` | Full-text search |
| `txmanager(route)` | `TxManagerPort` | Transaction route (e.g. `"default"`) |

Also available: `embeddings_provider`, `hub_search_query`, `federated_search_query`, distributed lock query/command, tenant resolver — see [`pages/docs/core-package/execution.md`](../../pages/docs/core-package/execution.md).

For keys without a convenience method, use `ctx.dep(DepKey, route=spec.name)(ctx, spec)`. Queue, pub/sub, stream, workflow, authn/authz, and secrets patterns have dedicated skills.

### Usecase pattern

Usecases extend `Usecase[Args, R]`, hold `ExecutionContext`, implement `main(args) -> R`:

```python
from forze.application.execution import Usecase

class GetProject(Usecase[UUID, ProjectReadModel]):
    async def main(self, args: UUID) -> ProjectReadModel:
        doc_q = self.ctx.doc_query(project_spec)
        return await doc_q.get(args)
```

Typical reads raise `NotFoundError` when missing (adapter-defined); do not assume `None`.

Usecases resolve ports from `self.ctx`; they do not take ports in the constructor except when composition factories inject them.

### Transactions

`transaction(route)` is an **async context manager**. Pass the **same route** registered on your deps module (prefer a shared `StrEnum`, e.g. `TxRoute.DEFAULT`). Nested calls reuse the active transaction (savepoints when supported). Mixing incompatible managers in one scope raises `CoreError`.

```python
async with self.ctx.transaction(TxRoute.DEFAULT):
    doc_c = self.ctx.doc_command(project_spec)
    await doc_c.create(cmd1)
    await doc_c.create(cmd2)
```

Use `defer_after_commit()` or `run_after_commit_or_now()` for side effects that must happen only after the root transaction commits:

```python
async with self.ctx.transaction(TxRoute.DEFAULT):
    created = await self.ctx.doc_command(project_spec).create(args)
    self.ctx.defer_after_commit(lambda: notify_project_created(created.id))
```

### Identity and tenancy

Bind `AuthnIdentity` / `TenantIdentity` at the HTTP, Socket.IO, queue worker, or Temporal worker boundary when needed. Usecases may read `ctx.get_authn_identity()` / `ctx.get_tenancy_identity()` but should not call `ctx.bind_call(...)` themselves. See [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md).

## Query syntax

Filters use the shared DSL: `{"$fields": {...}}`, `{"$and": [...]}`, `{"$or": [...]}`.

**Field shortcuts:**

| Value | Meaning |
|-------|---------|
| `"active"` | `$eq` |
| `["a", "b"]` | `$in` |
| `null` | `$null: true` |

**Operators:** `$eq`, `$neq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$null`, `$empty`, `$superset`, `$subset`, `$overlaps`, `$disjoint`.

**Sorts:** `{"created_at": "desc", "id": "asc"}`.

```python
filters = {"$fields": {"status": "active", "is_deleted": False}}
rows, total = await doc_q.find_many(
    filters=filters, limit=20, offset=0, sorts={"created_at": "desc"}
)
```

## Common patterns

### Document reads and writes

Use `doc_query` for read-only operations and `doc_command` for mutations:

```python
doc_q = self.ctx.doc_query(project_spec)
doc_c = self.ctx.doc_command(project_spec)

project = await doc_q.get(doc_id)
rows, total = await doc_q.find_many(filters=..., limit=20, offset=0)

created = await doc_c.create(CreateProjectCmd(title="New"))
updated = await doc_c.update(
    doc_id, current_rev, UpdateProjectCmd(title="Updated")
)
await doc_c.delete(doc_id, current_rev)
await doc_c.kill(doc_id)
```

### Search

```python
search = self.ctx.search_query(project_search_spec)
hits, total = await search.search(query="roadmap", filters=..., limit=20, offset=0)
```

### Counter (e.g. number_id)

```python
from forze.application.contracts.counter import CounterSpec

counter = self.ctx.counter(CounterSpec(name="tickets"))
next_id = await counter.incr()
```

### Object storage

```python
from forze.application.contracts.storage import StorageSpec

storage = self.ctx.storage(StorageSpec(name=ResourceName.ATTACHMENTS))
stored = await storage.upload("file.pdf", data, description="Contract")
downloaded = await storage.download(stored.key)
```

See [`forze-storage-s3`](../forze-storage-s3/SKILL.md) for S3 wiring and tenant-aware bucket behavior.

### Queue, pub/sub, stream, and workflow ports

Contracts without convenience methods resolve the routed factory and call it with `(ctx, spec)`:

```python
from forze.application.contracts.queue import QueueCommandDepKey
from forze.application.contracts.workflow import WorkflowCommandDepKey

queue = self.ctx.dep(QueueCommandDepKey, route=order_queue.name)(self.ctx, order_queue)
await queue.enqueue("orders", args, type="order.created")

workflow = self.ctx.dep(
    WorkflowCommandDepKey,
    route=onboarding_workflow.name,
)(self.ctx, onboarding_workflow)
handle = await workflow.start(StartOnboarding(project_id=args.project_id))
```

See [`forze-messaging-streaming`](../forze-messaging-streaming/SKILL.md) and [`forze-temporal-workflows`](../forze-temporal-workflows/SKILL.md).

## Gotchas

- **`doc_read` / `doc_write` are obsolete** — use `doc_query` / `doc_command`.
- **`ctx.search` is obsolete** — use `ctx.search_query(spec)`.
- **`ctx.counter("name")` is wrong** — pass `CounterSpec(name=...)`.
- **`ctx.storage("bucket")` is wrong** — pass `StorageSpec(name=...)`.
- **`transaction()` requires a route** — must match a registered tx manager route.
- **`ctx.workflow_command(...)` does not exist** — resolve `WorkflowCommandDepKey` with `route=spec.name`.
- **Do not nest incompatible tx backends** (e.g. Postgres + Mongo in one scope).

## Anti-patterns

1. Importing adapters in usecases — resolve via `ctx.doc_query` / `doc_command` / other ports.
2. Domain importing application or infrastructure — keep domain pure.
3. Raw SQL/ORM in usecases — use document/search/cache/storage ports.
4. Constructing `ExecutionContext` by hand in apps — obtain via `runtime.get_context()` or FastAPI `ctx_dep`.

## Reference

- [`pages/docs/core-package/execution.md`](../../pages/docs/core-package/execution.md)
- [`pages/docs/core-concepts/contracts-adapters.md`](../../pages/docs/core-concepts/contracts-adapters.md)
- [`pages/docs/core-package/query-syntax.md`](../../pages/docs/core-package/query-syntax.md)
- [`pages/docs/core-package/contracts.md`](../../pages/docs/core-package/contracts.md)

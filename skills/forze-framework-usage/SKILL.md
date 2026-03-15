---
name: forze-framework-usage
description: Write code that uses the Forze framework correctly. Apply when the user asks to implement features, usecases, or integrate Forze into their application.
---

# Forze Framework Usage

Use this skill when writing code that **uses** Forze (not when developing new adapters). The agent must understand framework concepts and produce code aligned with Forze patterns.

## Core concepts

### Layered architecture

Dependencies flow **inward**:

- **Domain** — pure business logic, no external deps
- **Application** — usecases, contracts, composition; imports domain only
- **Infrastructure** — adapters implementing contracts
- **Interface** — HTTP, WebSocket; invokes usecases via context

Usecases and domain models **never** import adapter classes or infrastructure packages.

### Contracts and adapters

The application declares **what** it needs via protocol interfaces (contracts). Infrastructure provides **how** (adapters). Usecases resolve ports from `ExecutionContext`; they never import adapters.

```python
# Correct: resolve port from context
doc = self.ctx.doc_read(project_spec)
result = await doc.get(some_id)

# Wrong: importing adapter
from forze_postgres import PostgresDocumentAdapter  # Never do this in usecases
```

### Execution context

`ExecutionContext` is the central resolution point. Usecases receive `ctx` and resolve ports from it:

| Method | Returns | Example |
|--------|---------|---------|
| `ctx.doc_read(spec)` | DocumentReadPort | `doc = ctx.doc_read(project_spec)` |
| `ctx.doc_write(spec)` | DocumentWritePort | `doc = ctx.doc_write(project_spec)` |
| `ctx.cache(spec)` | CachePort | `cache = ctx.cache(cache_spec)` |
| `ctx.counter(namespace)` | CounterPort | `counter = ctx.counter("tickets")` |
| `ctx.storage(bucket)` | StoragePort | `storage = ctx.storage("attachments")` |
| `ctx.search(spec)` | SearchReadPort | `search = ctx.search(search_spec)` |
| `ctx.txmanager()` | TxManagerPort | `tx = ctx.txmanager()` |

For contracts without a convenience method, use `ctx.dep(DepKey)(ctx, spec)`.

### Usecase pattern

Usecases extend `Usecase[Args, R]`, receive `ExecutionContext`, and implement `main(args) -> R`:

```python
from forze.application.execution import Usecase

class GetProject(Usecase[UUID, ProjectReadModel]):
    async def main(self, args: UUID) -> ProjectReadModel:
        doc = self.ctx.doc_read(project_spec)
        result = await doc.get(args)
        if result is None:
            raise NotFoundError("Project not found")
        return result
```

Usecases resolve ports from `self.ctx`; they do not receive ports via constructor (except when built by composition factories).

### Transactions

Use `ctx.transaction()` for transactional scope. Nested calls reuse the same transaction (savepoints when supported):

```python
async with self.ctx.transaction():
    doc = self.ctx.doc_write(project_spec)
    await doc.create(cmd1)
    await doc.create(cmd2)
    # Both commit or roll back together
```

## Query syntax

Filters use a shared DSL. Shape: `{"$fields": {...}}`, `{"$and": [expr, ...]}`, `{"$or": [expr, ...]}`.

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
rows, total = await doc.find_many(filters=filters, limit=20, offset=0, sorts={"created_at": "desc"})
```

## Common patterns

### Document CRUD

Use `DocumentReadPort` for reads, `DocumentWritePort` for mutations. Both accept a `DocumentSpec`:

```python
doc_read = self.ctx.doc_read(project_spec)
doc_write = self.ctx.doc_write(project_spec)

# Read
project = await doc_read.get(id)
rows, total = await doc_read.find_many(filters=..., limit=20, offset=0)

# Write
created = await doc_write.create(CreateProjectCmd(title="New"))
updated = await doc_write.update(id, UpdateProjectCmd(title="Updated"), rev=current_rev)
await doc_write.delete(id, rev=current_rev)  # soft delete
await doc_write.kill(id)  # hard delete
```

### Search

```python
search = self.ctx.search(project_search_spec)
hits, total = await search.search(query="roadmap", filters=..., limit=20, offset=0)
```

### Counter (e.g. number_id)

```python
counter = self.ctx.counter("projects")
next_id = await counter.incr()
```

### Object storage

```python
storage = self.ctx.storage("attachments")
stored = await storage.upload("file.pdf", data, description="Contract")
downloaded = await storage.download(stored.key)
```

## Anti-patterns

1. **Importing adapters in usecases** — resolve via `ctx.doc_read(spec)` etc.
2. **Domain importing application/infrastructure** — domain is pure, no ports or DB.
3. **Mixing transaction scopes** — don't resolve Postgres and Mongo ports in the same transaction.
4. **Using raw SQL/ORM in usecases** — use document/search/cache ports instead.
5. **Creating ExecutionContext manually** — get it from `runtime.get_context()` or `ctx_dep`.

## Reference

- Docs: `pages/docs/` (getting-started, core-concepts, core-package, integrations)
- Contracts catalog: `pages/docs/core-concepts/contracts-adapters.md`
- Query syntax: `pages/docs/core-package/query-syntax.md`

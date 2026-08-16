# Handler patterns

## Common patterns

For standard document CRUD in application/driving code (routes, services), prefer a **`DocumentFacade`** — and `SearchFacade` for search — over resolving ports by hand; see [document access](document-facade.md). The port-level snippets below are what a **custom handler** does internally, once a facade operation can't express the work.

### Document reads and writes (inside a custom handler)

```python
doc_q = ctx.document.query(project_spec)
doc_c = ctx.document.command(project_spec)

project = await doc_q.get(doc_id)
page = await doc_q.find_page(filters=..., pagination={"limit": 20, "offset": 0})
rows = page.hits

created = await doc_c.create(CreateProjectCmd(title="New"))
updated = await doc_c.update(doc_id, current_rev, UpdateProjectCmd(title="Updated"))
await doc_c.kill(doc_id)  # hard delete; soft delete is a kit concern (SoftDeletionMixin + registry)
```

### Search

```python
search = ctx.search.query(project_search_spec)
page = await search.search_page("roadmap", filters=..., pagination={"limit": 20, "offset": 0})
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

See [queues](messaging-queues.md) and [Temporal workflows](temporal.md).

## Gotchas

- **`ctx.doc_query` / `ctx.doc_command` are removed** — use `ctx.document.query` / `ctx.document.command`.
- **`ctx.dep(...)` on context is removed** — use `ctx.deps.provide` or `ctx.deps.resolve_configurable`.
- **`ctx.transaction()` takes a route** — the no-argument form is gone; `ctx.transaction(route)` still resolves the `TransactionManagerPort`. To *open* a scope, use `ctx.tx_ctx.scope(route)`.
- **`ctx.counter("name")` is wrong** — pass `CounterSpec(name=...)`.
- **`UsecaseRegistry` is removed** — use `OperationRegistry` + `.freeze()`.
- **Do not nest incompatible tx backends** (e.g. Postgres + Mongo in one scope).

## Anti-patterns

- Importing adapters in handlers — resolve via `ctx.document.query` / `command` / other ports.
- Raw SQL/ORM in handlers — use document/search/cache/storage ports.

## Reference

- [Execution reference](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)
- [Operation composition](https://morzecrew.github.io/forze/latest/writing-operation/capability-execution/)

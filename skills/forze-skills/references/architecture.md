# Architecture

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

## Anti-patterns

- Domain importing application or infrastructure — keep domain pure.

## Reference

- [Contracts and adapters](https://morzecrew.github.io/forze/latest/core-concepts/contracts/)
- [Contracts overview](https://morzecrew.github.io/forze/latest/reference/contracts/)

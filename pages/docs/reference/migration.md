---
title: Migration guide
icon: lucide/arrow-up-circle
summary: Breaking changes and how to update your code
---

This page documents breaking changes between Forze versions and how to migrate existing code.

## Removed APIs

### Context accessors

The flat context accessors have been replaced with namespaced alternatives:

| Removed | Replacement |
|---------|-------------|
| `ctx.doc_query(spec)` | `ctx.document.query(spec)` |
| `ctx.doc_command(spec)` | `ctx.document.command(spec)` |
| `ctx.doc_read(spec)` | `ctx.document.query(spec)` |
| `ctx.doc_write(spec)` | `ctx.document.command(spec)` |
| `ctx.search_query(spec)` | `ctx.search.query(spec)` |

The namespaced accessors are clearer and group related operations.

### Dependency resolution

| Removed | Replacement |
|---------|-------------|
| `ctx.dep(...)` | `ctx.deps.provide(...)` or `ctx.deps.resolve_configurable(...)` |
| `ctx.transaction()` | `ctx.tx_ctx.scope(route)` |

### Registry

| Removed | Replacement |
|---------|-------------|
| `UsecaseRegistry` | `OperationRegistry` with `.freeze()` |

Build your registry with `build_document_registry()` or `OperationRegistry()`, then call `.freeze()` before passing it to the runtime.

### Create command alias

| Deprecated | Replacement |
|------------|-------------|
| `CreateDocumentCmd` | `BaseDTO` |

`CreateDocumentCmd` still works as an alias but is deprecated. Use `BaseDTO` for new code.

### FastAPI endpoint helpers

The `forze_fastapi.endpoints.*` helpers have been removed:

| Removed | Replacement |
|---------|-------------|
| `attach_document_endpoints` | `attach_document_routes` from `forze_fastapi.routes` |
| `attach_search_endpoints` | `attach_search_routes` from `forze_fastapi.routes` |
| `attach_http_endpoint` | Define your own routes that dispatch through the registry |
| `build_http_endpoint_spec` | (removed) |

The new `forze_fastapi.routes` module generates routes from a frozen operation registry:

```python
from forze_fastapi.routes import attach_document_routes

attach_document_routes(app, registry, user_spec)
```

Or define custom routes that resolve a context and dispatch through the facade:

```python
from fastapi import APIRouter, Depends

router = APIRouter(prefix="/users", tags=["users"])

@router.get("/{user_id}")
async def get_user(user_id: UUID, ctx=Depends(context_dependency)):
    return await ctx.document.query(user_spec).get(user_id)

app.include_router(router)
```

## Migration checklist

When upgrading Forze:

1. **Search for removed accessors** — grep for `ctx.doc_query`, `ctx.doc_command`, `ctx.doc_read`, `ctx.doc_write`, `ctx.search_query`
2. **Update dependency resolution** — replace `ctx.dep(...)` with `ctx.deps.provide(...)` or `ctx.deps.resolve_configurable(...)`
3. **Update transaction scoping** — replace `ctx.transaction()` with `ctx.tx_ctx.scope(route)`
4. **Update registry usage** — replace `UsecaseRegistry` with `OperationRegistry` and add `.freeze()`
5. **Update FastAPI routes** — migrate from `forze_fastapi.endpoints.*` to `forze_fastapi.routes.*`
6. **Replace CreateDocumentCmd** — use `BaseDTO` for new create commands

## See also

- [Wiring](../in-depth/wiring.md) — current dependency resolution patterns
- [Runtime](../core-concepts/runtime.md) — execution context usage
- [FastAPI integration](../integrations/fastapi.md) — route generation

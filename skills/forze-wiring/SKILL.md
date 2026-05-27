---
name: forze-wiring
description: >-
  Wires Forze ExecutionRuntime, DepsPlan, lifecycle, built-in deps modules,
  document/search composition, registry stage authoring, and interface entry
  points. Use when bootstrapping an app or composing runtime, lifecycle, and
  operation registries.
---

# Forze Wiring

Use when setting up the Forze runtime, dependency plan, lifecycle, operation composition, and interface layer. For logical spec names, routes, and `StrEnum` wiring, see [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md). For custom dependency modules, see [`forze-deps-modules`](../forze-deps-modules/SKILL.md). For HTTP details, see [`forze-fastapi-interface`](../forze-fastapi-interface/SKILL.md). For day-to-day handler code, see [`forze-framework-usage`](../forze-framework-usage/SKILL.md).

## Runtime setup

Kernel **specs** (`DocumentSpec`, `SearchSpec`, `CacheSpec`, …) declare model types and logical `name` only—no DSNs, table names, collection paths, or index DDL. **Deps modules** (`PostgresDepsModule`, `MongoDepsModule`, `RedisDepsModule`, …) map that same `name` to physical configs (read/write relations, Redis namespaces, `PostgresSearchConfig`, …). **`DepsPlan.from_modules(...)`** merges those modules so `ExecutionContext` resolves factories by route `spec.name` (for example `DocumentQueryDepKey` / `DocumentCommandDepKey`). See [`pages/docs/concepts/specs-and-wiring.md`](../../pages/docs/concepts/specs-and-wiring.md).

### Dependency plan

Pass **`DepsModule` instances** to `DepsPlan.from_modules`. Each module’s `__call__` returns a `Deps` container; the plan merges them (conflicting keys raise `CoreError`).

```python
from enum import StrEnum

from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
from forze_postgres import PostgresDepsModule, postgres_lifecycle_step, PostgresClient, PostgresConfig
from forze_redis import RedisDepsModule, redis_lifecycle_step, RedisClient, RedisConfig


class ResourceName(StrEnum):
    PROJECTS = "projects"


class TxRoute(StrEnum):
    DEFAULT = "default"


postgres_client = PostgresClient()
redis_client = RedisClient()

deps_plan = DepsPlan.from_modules(
    PostgresDepsModule(
        client=postgres_client,
        rw_documents={
            ResourceName.PROJECTS: {
                "read": ("public", "projects"),
                "write": ("public", "projects"),
                "bookkeeping_strategy": "database",
            },
        },
        tx={TxRoute.DEFAULT},
    ),
    RedisDepsModule(
        client=redis_client,
        caches={ResourceName.PROJECTS: {"namespace": "app:projects"}},
    ),
)
```

Alternatively, a single callable module may return `Deps.merge(...)` — see [`pages/docs/getting-started.md`](../../pages/docs/getting-started.md).

Merge optional integration modules the same way — for example `TenancyDepsModule` from `forze_identity.tenancy.execution` registers `TenantResolverDepKey` / `TenantManagementDepKey` routes for document-backed tenant resolution (see [`pages/docs/concepts/multi-tenancy.md`](../../pages/docs/concepts/multi-tenancy.md)):

```python
from forze_identity.tenancy.execution import TenancyDepsModule

deps_plan = DepsPlan.from_modules(
    PostgresDepsModule(...),
    TenancyDepsModule(tenant_resolver={"main"}),
)
```

### Lifecycle plan

Manages startup/shutdown of connection pools:

```python
lifecycle_plan = LifecyclePlan.from_steps(
    postgres_lifecycle_step(
        dsn="postgresql://app:app@localhost:5432/app",
        config=PostgresConfig(min_size=2, max_size=15),
    ),
    redis_lifecycle_step(
        dsn="redis://localhost:6379/0",
        config=RedisConfig(max_size=20),
    ),
)
```

### Execution runtime

```python
runtime = ExecutionRuntime(deps=deps_plan, lifecycle=lifecycle_plan)
```

Run work inside `runtime.scope()`:

```python
async with runtime.scope():
    ctx = runtime.get_context()
```

## Document composition

### Registry and transaction plan

`build_document_registry` registers standard CRUD handlers. **Transactions are not implicit** — bind a transaction route on write operations, then **freeze** before HTTP attach:

```python
from forze.application.composition.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)

project_dtos = DocumentDTOs(
    read=ProjectReadModel,
    create=CreateProjectCmd,
    update=UpdateProjectCmd,
)

write_ops = [
    project_spec.default_namespace.key(op)
    for op in (
        DocumentKernelOp.CREATE,
        DocumentKernelOp.UPDATE,
        DocumentKernelOp.DELETE,
        DocumentKernelOp.KILL,
        DocumentKernelOp.RESTORE,
    )
]

registry = (
    build_document_registry(project_spec, project_dtos)
    .bind(*write_ops)
    .bind_tx()
    .set_route("default")
    .finish(deep=True)
    .freeze()
)
```

### Custom handlers and stage hooks

```python
from forze.application.contracts.execution import BeforeStep
from forze.application.composition.document import DocumentKernelOp

create_op = project_spec.default_namespace.key(DocumentKernelOp.CREATE)


def auth_before_factory(ctx):
    async def _before(args):
        if not is_authorized(ctx):
            raise PermissionError("Not authorized")
    return _before


registry = (
    build_document_registry(project_spec, project_dtos)
    .bind(create_op)
    .bind_tx()
    .set_route("default")
    .finish(deep=False)
    .bind_outer()
    .before(BeforeStep(id="auth", factory=auth_before_factory, priority=100))
    .finish(deep=True)
    .freeze()
)
```

Custom operations use explicit operation keys on the same registry:

```python
archive_op = project_spec.default_namespace.key("archive")

registry = build_document_registry(project_spec, project_dtos)
registry = registry.set_handler(
    archive_op,
    lambda ctx: ArchiveProject(doc=ctx.document.command(project_spec)),
)
registry = (
    registry.bind(archive_op)
    .bind_tx()
    .set_route("default")
    .finish(deep=True)
    .freeze()
)
```

### Stage order

Outer `before` / `wrap` / `on_success` / `on_failure` / `finally_`, then optional transaction scope (`tx_before`, handler, transactional `on_success`, `after_commit`, `dispatch_after_commit`). Higher `priority` runs first within the same stage. See [`pages/docs/reference/middleware-plans.md`](../../pages/docs/reference/middleware-plans.md).

## FastAPI integration

### Context dependency

```python
def context_dependency():
    return runtime.get_context()
```

### Document endpoints

```python
from fastapi import APIRouter

from forze_fastapi.endpoints.document import attach_document_endpoints

router = APIRouter(prefix="/projects", tags=["projects"])
attach_document_endpoints(
    router,
    document=project_spec,
    dtos=project_dtos,
    registry=registry,
    ctx_dep=context_dependency,
)

app.include_router(router)
```

### Lifespan with runtime scope

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with runtime.scope():
        yield


app = FastAPI(lifespan=lifespan)
```

## Mapping steps

Inject computed fields (e.g. `number_id`, `creator_id`) before the handler runs:

```python
from forze.application.composition.document import build_document_create_mapper
from forze.application.mapping import CreatorIdStep, NumberIdStep

mapper = (
    build_document_create_mapper(project_spec, project_dtos)
    .with_steps(NumberIdStep(), CreatorIdStep())
)
```

## Testing with Mock

In-memory adapters — no external services:

```python
from forze.application.execution import DepsPlan, ExecutionRuntime
from forze_mock import MockDepsModule

mock_module = MockDepsModule()
runtime = ExecutionRuntime(deps=DepsPlan.from_modules(mock_module))

async with runtime.scope():
    ctx = runtime.get_context()
    doc_q = ctx.document.query(project_spec)
    result = await doc_q.get(some_uuid)
```

## Search composition

```python
from forze.application.composition.search import SearchFacade, build_search_registry
from forze.application.dto.search import SearchRequestDTO

search_registry = build_search_registry(project_search_spec).freeze()

facade = SearchFacade(
    ctx=ctx,
    registry=search_registry,
    namespace=project_search_spec.default_namespace,
)
result = await facade.search(SearchRequestDTO(query="roadmap", limit=20))
```

## Anti-patterns

1. **Hand-building `Deps` for production** — prefer `DepsPlan.from_modules` and integration modules.
2. **Skipping lifecycle** — real adapters need pools started/stopped.
3. **`get_context()` outside `runtime.scope()`** — raises `RuntimeError`.
4. **Missing `ctx_dep` on FastAPI routers** — each request needs a context from the active scope.
5. **Attaching HTTP routes without `.freeze()`** — call `.freeze()` after `bind_tx().set_route(...).finish(...)`.
6. **Duplicating literal route strings** — use shared `StrEnum` values for spec names and transaction routes.

## Reference

- [`pages/docs/getting-started.md`](../../pages/docs/getting-started.md)
- [`pages/docs/concepts/operation-composition.md`](../../pages/docs/concepts/operation-composition.md)
- [`pages/docs/reference/composition.md`](../../pages/docs/reference/composition.md)
- [`pages/docs/integrations/fastapi.md`](../../pages/docs/integrations/fastapi.md)
- [`pages/docs/integrations/mock.md`](../../pages/docs/integrations/mock.md)

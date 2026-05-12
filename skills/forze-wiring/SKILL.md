---
name: forze-wiring
description: >-
  Wires Forze ExecutionRuntime, DepsPlan, lifecycle, built-in deps modules,
  document/search composition, UsecasePlan transactions, and interface entry
  points. Use when bootstrapping an app or composing runtime, lifecycle, and
  usecase registries.
---

# Forze Wiring

Use when setting up the Forze runtime, dependency plan, lifecycle, usecase composition, and interface layer. For logical spec names, routes, and `StrEnum` wiring, see [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md). For custom dependency modules, see [`forze-deps-modules`](../forze-deps-modules/SKILL.md). For HTTP details, see [`forze-fastapi-interface`](../forze-fastapi-interface/SKILL.md). For day-to-day usecase code, see [`forze-framework-usage`](../forze-framework-usage/SKILL.md).

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

Merge optional integration modules the same way — for example `TenancyDepsModule` from `forze_tenancy.execution` registers `TenantResolverDepKey` / `TenantManagementDepKey` routes for document-backed tenant resolution (see [`pages/docs/concepts/multi-tenancy.md`](../../pages/docs/concepts/multi-tenancy.md)):

```python
from forze_tenancy.execution import TenancyDepsModule

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

`build_document_registry` registers standard CRUD usecases. **Transactions are not implicit** — attach a `UsecasePlan` with `.tx(..., route=...)` / `DocumentOperation` keys, or use `build_default_tx_document_plan`:

```python
from forze.application.composition.document import (
    DocumentDTOs,
    build_default_tx_document_plan,
    build_document_registry,
)

project_dtos = DocumentDTOs(
    read=ProjectReadModel,
    create=CreateProjectCmd,
    update=UpdateProjectCmd,
)

registry = build_document_registry(project_spec, project_dtos)
registry.extend_plan(build_default_tx_document_plan(TxRoute.DEFAULT), inplace=True)
```

Equivalent explicit plan:

```python
from forze.application.composition.document import DocumentOperation
from forze.application.execution import UsecasePlan

document_plan = (
    UsecasePlan()
    .tx(DocumentOperation.CREATE, route=TxRoute.DEFAULT)
    .tx(DocumentOperation.UPDATE, route=TxRoute.DEFAULT)
    .tx(DocumentOperation.KILL, route=TxRoute.DEFAULT)
    .tx(DocumentOperation.DELETE, route=TxRoute.DEFAULT)
    .tx(DocumentOperation.RESTORE, route=TxRoute.DEFAULT)
)
registry.extend_plan(document_plan, inplace=True)
```

### Custom usecases and middleware

```python
from forze.application.composition.document import DocumentOperation
from forze.application.execution import UsecasePlan


def auth_guard(ctx):
    async def guard(args):
        if not is_authorized(ctx):
            raise PermissionError("Not authorized")
    return guard


extra_plan = (
    UsecasePlan()
    .before(DocumentOperation.CREATE, auth_guard, priority=100)
    .after_commit(DocumentOperation.CREATE, notify_effect)
)
registry.extend_plan(extra_plan, inplace=True)
```

Custom operations use **string** keys merged into the same registry/plan:

```python
registry.register(
    "archive",
    lambda ctx: ArchiveProject(ctx=ctx),
    inplace=True,
)
registry.extend_plan(
    UsecasePlan().tx("archive", route=TxRoute.DEFAULT),
    inplace=True,
)
```

### Plan buckets (order)

`outer_before` → `outer_wrap` → [transaction] → `in_tx_before` → `in_tx_wrap` → usecase → `in_tx_after` → `outer_after` → `after_commit`. Higher priority runs first within a bucket.

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

Inject computed fields (e.g. `number_id`, `creator_id`) before the usecase:

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
    doc_q = ctx.doc_query(project_spec)
    result = await doc_q.get(some_uuid)
```

## Search composition

```python
from forze.application.composition.search import (
    SearchDTOs,
    SearchUsecasesFacade,
    build_search_registry,
)
from forze.application.dto.search import SearchRequestDTO

search_dtos = SearchDTOs(read=ProjectReadModel)
search_registry = build_search_registry(project_search_spec, search_dtos)

facade = SearchUsecasesFacade(ctx=ctx, reg=search_registry)
result = await facade.search(SearchRequestDTO(query="roadmap", limit=20))
```

## Anti-patterns

1. **Hand-building `Deps` for production** — prefer `DepsPlan.from_modules` and integration modules.
2. **Skipping lifecycle** — real adapters need pools started/stopped.
3. **`get_context()` outside `runtime.scope()`** — raises `RuntimeError`.
4. **Missing `ctx_dep` on FastAPI routers** — each request needs a context from the active scope.
5. **Expecting `tx_document_plan`** — use `build_default_tx_document_plan` or an explicit `UsecasePlan`.
6. **Duplicating literal route strings** — use shared `StrEnum` values for spec names and transaction routes.

## Reference

- [`pages/docs/getting-started.md`](../../pages/docs/getting-started.md)
- [`pages/docs/core-package/composition.md`](../../pages/docs/core-package/composition.md)
- [`pages/docs/integrations/fastapi.md`](../../pages/docs/integrations/fastapi.md)
- [`pages/docs/integrations/mock.md`](../../pages/docs/integrations/mock.md)

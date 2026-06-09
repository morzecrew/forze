---
name: forze-wiring
description: >-
  Wires Forze ExecutionRuntime, DepsRegistry, lifecycle, built-in deps modules,
  document/search composition, operation pipeline stages, and interface entry
  points. Use when bootstrapping an app or composing runtime, lifecycle, and
  operation registries.
---

# Forze Wiring

Use when setting up the Forze runtime, dependency registry, lifecycle, operation composition, and interface layer. For logical spec names, routes, and `StrEnum` wiring, see [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md). For dependency resolution, see [`forze-deps-consumption`](../forze-deps-consumption/SKILL.md); for private integrations, see [`forze-custom-deps`](../forze-custom-deps/SKILL.md). For HTTP details, see [`forze-fastapi-interface`](../forze-fastapi-interface/SKILL.md). For day-to-day handler code, see [`forze-framework-usage`](../forze-framework-usage/SKILL.md).

For which plan enables deps, lifecycle hooks, or operation stages, see [Three execution plans](https://morzecrew.github.io/forze/docs/reference/execution/#three-execution-plans) (`DepsRegistry`, `LifecyclePlan`, `OperationRegistry`).

## Runtime setup

Logical **specs** (`DocumentSpec`, `SearchSpec`, `CacheSpec`, …) declare model types and `name` only—no DSNs, table names, collection paths, or index DDL. **Deps modules** (`PostgresDepsModule`, `MongoDepsModule`, `RedisDepsModule`, …) map that same `name` to physical configs (read/write relations, Redis namespaces, `PostgresSearchConfig`, …). **`DepsRegistry.from_modules(...)`** merges those modules so `ExecutionContext` resolves factories by route `spec.name` (for example `DocumentQueryDepKey` / `DocumentCommandDepKey`). See [Specs and wiring](https://morzecrew.github.io/forze/docs/concepts/specs-and-wiring/).

### Dependency registry

Pass **`DepsModule` instances** to `DepsRegistry.from_modules`. Each module’s `__call__` returns a `Deps` container; the plan merges them (conflicting keys raise `CoreException`).

```python
from enum import StrEnum

from forze.application.execution import DepsRegistry, ExecutionRuntime, LifecyclePlan
from forze_postgres import (
    PostgresClient,
    PostgresConfig,
    PostgresDepsModule,
    PostgresDocumentConfig,
    postgres_lifecycle_step,
)
from forze_redis import RedisDepsModule, redis_lifecycle_step, RedisClient, RedisConfig


class ResourceName(StrEnum):
    PROJECTS = "projects"


class TxRoute(StrEnum):
    DEFAULT = "default"


postgres_client = PostgresClient()
redis_client = RedisClient()

deps_registry = DepsRegistry.from_modules(
    PostgresDepsModule(
        client=postgres_client,
        rw_documents={
            ResourceName.PROJECTS: PostgresDocumentConfig(
                read=("public", "projects"),
                write=("public", "projects"),
                bookkeeping_strategy="database",
            ),
        },
        tx={TxRoute.DEFAULT},
    ),
    RedisDepsModule(
        client=redis_client,
        caches={ResourceName.PROJECTS: {"namespace": "app:projects"}},
    ),
)
```

Alternatively, a single callable module may return `Deps.merge(...)` — see [Getting started](https://morzecrew.github.io/forze/docs/getting-started/).

Merge optional integration modules the same way — for example `TenancyDepsModule` from `forze_identity.tenancy.execution` registers `TenantResolverDepKey` / `TenantManagementDepKey` routes for document-backed tenant resolution (see [Multi-tenancy](https://morzecrew.github.io/forze/docs/concepts/multi-tenancy/)):

```python
from forze_identity.tenancy.execution import TenancyDepsModule

deps_registry = DepsRegistry.from_modules(
    PostgresDepsModule(...),
    TenancyDepsModule(tenant_resolver={"main"}),
)
```

### Lifecycle plan

Manages startup/shutdown of connection pools. Use `LifecyclePlan.from_modules(...)` for integration modules (for example `PostgresLifecycleModule`) or `from_steps(...)` for individual factories. Call `freeze()` to build topological waves using `requires` / `provides` / `depends_on` on each `LifecycleStep`, then pass the frozen plan to `ExecutionRuntime`. Use `with_concurrent()` when independent steps in the same wave may start in parallel.

```python
from forze_postgres import PostgresLifecycleModule

lifecycle_plan = LifecyclePlan.from_modules(
    PostgresLifecycleModule(
        client=pg,
        dsn="postgresql://app:app@localhost:5432/app",
        config=PostgresConfig(min_size=2, max_size=15),
    ),
).with_steps(
    redis_lifecycle_step(
        dsn="redis://localhost:6379/0",
        config=RedisConfig(max_size=20),
    ),
)
```

### Execution runtime

```python
runtime = ExecutionRuntime(
    deps=deps_registry.freeze(),
    lifecycle=lifecycle_plan.freeze(),
)
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
from forze_kits.aggregates.document import (
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
from forze_kits.aggregates.document import DocumentKernelOp

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

Outer `before` / `wrap` / `on_success` / `on_failure` / `finally_`, then optional transaction scope (`tx_before`, handler, transactional `on_success`, `after_commit`, `dispatch_after_commit`). Higher `priority` runs first within the same stage. See [Middleware and plans](https://morzecrew.github.io/forze/docs/reference/middleware-plans/).

## FastAPI integration

### Context dependency

```python
def context_dependency():
    return runtime.get_context()
```

### Endpoints

> **Note:** the former `forze_fastapi.endpoints.*` router helpers (`attach_document_endpoints`, `attach_search_endpoints`, `attach_http_endpoint`, …) have been removed. Define your own FastAPI routes that resolve a context with the dependency above, dispatch through your operation registry / facade (see [Search composition](#search-composition)), and return the result. Use `SecurityContextMiddleware` for identity binding and `register_exception_handlers(app)` for error mapping (see [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md) and [`forze-observability-errors`](../forze-observability-errors/SKILL.md)). A canonical endpoint pattern is being reworked alongside the docs refresh.

```python
from fastapi import APIRouter

router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("/{project_id}")
async def get_project(project_id: UUID, ctx=Depends(context_dependency)):
    return await ctx.document.query(project_spec).get(project_id)


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
from forze_kits.aggregates.document import DocumentMappers, build_document_registry
from forze_kits.domain.creator_id import CreatorIdMappingStepFactory
from forze_kits.domain.number_id import NumberIdMappingStepFactory
from forze_kits.mapping import PydanticPipelineMapperFactory

create_mapper = PydanticPipelineMapperFactory(
    in_=CreateProjectRequest,
    out=CreateProjectCmd,
    step_factories=(
        NumberIdMappingStepFactory(spec=project_counter_spec),
        CreatorIdMappingStepFactory(),  # configured per your identity resolver
    ),
)

registry = build_document_registry(
    project_spec,
    project_dtos,
    DocumentMappers(create=create_mapper),
).freeze()
```

See [`forze-domain-aggregates`](../forze-domain-aggregates/SKILL.md) and the mapping reference for step configuration.

## Testing with Mock

In-memory adapters — no external services:

```python
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_mock import MockDepsModule

mock_module = MockDepsModule()
runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(mock_module).freeze())

async with runtime.scope():
    ctx = runtime.get_context()
    doc_q = ctx.document.query(project_spec)
    result = await doc_q.get(some_uuid)
```

## Search composition

```python
from forze_kits.aggregates.search import SearchFacade, SearchRequestDTO, build_search_registry

search_registry = build_search_registry(project_search_spec).freeze()

facade = SearchFacade(
    ctx=ctx,
    registry=search_registry,
    namespace=project_search_spec.default_namespace,
)
result = await facade.search(SearchRequestDTO(query="roadmap", limit=20))
```

## Transactional notifications

```python
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze_kits.integrations.outbox import outbox_flush_tx_on_success_factory, relay_outbox_to_queue
from forze_kits.integrations.notify import EmailNotification, NotificationRouter, process_notification_message

events_spec = OutboxSpec(
    name="events",
    codec=...,
    destination=OutboxDestination.queue(route="notifications", channel="notifications"),
)
router = NotificationRouter()
router.register("project.created", lambda e: [EmailNotification(...)])
# stage in handler; flush via outbox_flush_tx_on_success_factory; relay; worker calls process_notification_message
```

See [Transactional notifications](https://morzecrew.github.io/forze/docs/recipes/transactional-notifications/).

## Anti-patterns

1. **Hand-building `Deps` for production** — prefer `DepsRegistry.from_modules` and integration modules.
2. **Skipping lifecycle** — real adapters need pools started/stopped.
3. **`get_context()` outside `runtime.scope()`** — raises `RuntimeError`.
4. **Missing `ctx_dep` on FastAPI routers** — each request needs a context from the active scope.
5. **Attaching HTTP routes without `.freeze()`** — call `.freeze()` after `bind_tx().set_route(...).finish(...)` on operation registries, and on deps/lifecycle plans before `ExecutionRuntime`.
6. **Duplicating literal route strings** — use shared `StrEnum` values for spec names and transaction routes.

## Reference

- [Getting started](https://morzecrew.github.io/forze/docs/getting-started/)
- [Operation composition](https://morzecrew.github.io/forze/docs/concepts/operation-composition/)
- [Composition reference](https://morzecrew.github.io/forze/docs/reference/composition/)
- [FastAPI integration](https://morzecrew.github.io/forze/docs/integrations/fastapi/)
- [Mock integration](https://morzecrew.github.io/forze/docs/integrations/mock/)

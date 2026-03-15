---
name: forze-wiring
description: Wire Forze runtime, dependencies, composition, and interface (FastAPI). Apply when the user asks to set up the application, configure adapters, or expose endpoints.
---

# Forze Wiring

Use this skill when setting up the Forze runtime, dependency plan, lifecycle, usecase composition, and interface layer (e.g. FastAPI).

## Runtime setup

### Dependency plan

Combine adapter modules with `Deps.merge`. Each integration package provides a `*DepsModule`:

```python
from forze.application.execution import Deps, DepsPlan, ExecutionRuntime, LifecyclePlan
from forze_postgres import PostgresDepsModule, postgres_lifecycle_step, PostgresClient, PostgresConfig
from forze_redis import RedisDepsModule, redis_lifecycle_step, RedisClient, RedisConfig

postgres_client = PostgresClient()
redis_client = RedisClient()

deps_plan = DepsPlan.from_modules(
    lambda: Deps.merge(
        PostgresDepsModule(
            client=postgres_client,
            rev_bump_strategy="database",
            history_write_strategy="database",
        )(),
        RedisDepsModule(client=redis_client)(),
    ),
)
```

`Deps.merge` raises if any key is registered twice — catches misconfiguration early.

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

Run the app inside `runtime.scope()`:

```python
async with runtime.scope():
    ctx = runtime.get_context()
    # Application runs here
```

## Document composition

### Registry and plan

`build_document_registry` registers standard CRUD usecases. `tx_document_plan` wraps them in transactions:

```python
from forze.application.composition.document import (
    DocumentDTOs,
    build_document_registry,
    tx_document_plan,
)

project_dtos = DocumentDTOs(
    read=ProjectReadModel,
    create=CreateProjectCmd,
    update=UpdateProjectCmd,
)

registry = build_document_registry(project_spec, project_dtos)
registry.extend_plan(tx_document_plan, inplace=True)
```

### Custom usecases and middleware

Add custom operations and guards/effects:

```python
from forze.application.composition.document import DocumentOperation, tx_document_plan

def auth_guard(ctx):
    async def guard(args):
        if not is_authorized(ctx):
            raise PermissionError("Not authorized")
    return guard

plan = (
    tx_document_plan
    .tx("archive")  # Add transaction for custom op
    .before(DocumentOperation.CREATE, auth_guard, priority=100)
    .after_commit(DocumentOperation.CREATE, notify_effect)
)

registry = build_document_registry(project_spec, project_dtos)
registry = registry.register("archive", lambda ctx: ArchiveProject(ctx=ctx))
registry.extend_plan(plan, inplace=True)
```

### Plan buckets (order)

`outer_before` → `outer_wrap` → [transaction] → `in_tx_before` → `in_tx_wrap` → usecase → `in_tx_after` → `outer_after` → `after_commit`. Higher priority runs first (outermost).

## FastAPI integration

### Context dependency

```python
def context_dependency():
    return runtime.get_context()
```

### Document router

```python
from forze_fastapi.routers import build_document_router

app.include_router(
    build_document_router(
        prefix="/projects",
        tags=["projects"],
        registry=registry,
        spec=project_spec,
        dtos=project_dtos,
        ctx_dep=context_dependency,
    )
)
```

### Lifespan with runtime scope

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with runtime.scope():
        yield

app = FastAPI(lifespan=lifespan)
```

## Mapping steps

Inject computed fields (e.g. `number_id`, `creator_id`) before the usecase:

```python
from forze.application.mapping import NumberIdStep, CreatorIdStep, build_document_create_mapper

mapper = (
    build_document_create_mapper(project_spec, project_dtos)
    .with_steps(NumberIdStep(), CreatorIdStep())
)
```

## Testing with Mock

No external services needed:

```python
from forze.application.execution import DepsPlan, ExecutionRuntime
from forze_mock import MockDepsModule

module = MockDepsModule()
runtime = ExecutionRuntime(deps=DepsPlan.from_modules(module))

async with runtime.scope():
    ctx = runtime.get_context()
    doc = ctx.doc_read(project_spec)
    result = await doc.get(some_uuid)
```

## Search composition

```python
from forze.application.composition.search import (
    SearchDTOs,
    SearchUsecasesFacade,
    build_search_registry,
)

search_dtos = SearchDTOs(read=ProjectReadModel)
search_registry = build_search_registry(project_search_spec, search_dtos)

facade = SearchUsecasesFacade(ctx=ctx, reg=search_registry)
result = await facade.search(SearchRequestDTO(query="roadmap", limit=20))
```

## Anti-patterns

1. **Building Deps manually for production** — use `DepsPlan.from_modules` with integration modules.
2. **Skipping lifecycle** — real adapters need connection pools started/stopped.
3. **Creating context outside scope** — `get_context()` raises if not inside `runtime.scope()`.
4. **Forgetting `ctx_dep`** — FastAPI router needs it to resolve ports per request.

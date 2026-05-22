# Application Layer

## What problem this solves

Handlers need to coordinate domain logic and infrastructure without importing concrete adapters directly.

## When you need this

Use this when you write operations, resolve dependencies, add stage hooks, or build an execution runtime.


The application layer **orchestrates** domain logic and coordinates infrastructure. It defines *what* happens, not *how* persistence or transport work.

## Key ideas

The application layer is built around a few core concepts:

- **Execution context**: the single point through which handlers resolve all dependencies. No handler imports an adapter directly.
- **Handlers**: self-contained operations that implement one business action. Built-in handlers receive ports in their constructor; custom handlers implement `Handler[Args, R]`.
- **Operation plans**: composable stage hooks (before, on-success, transaction boundaries, after-commit) registered on `OperationRegistry` without modifying handler core logic.
- **Runtime**: the container that manages dependency injection, lifecycle hooks, and context creation.

Together these ensure that business logic remains independent of infrastructure choices.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/execution-runtime.svg" alt="Execution runtime overview">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/execution-runtime.svg" alt="Execution runtime overview">
</div>

## Execution context

`ExecutionContext` is the central dependency resolution point. Handler factories and lifecycle hooks receive it to obtain typed ports:

| API | Returns | Purpose |
|-----|---------|---------|
| `ctx.deps.provide(key, route=...)` | `T` | Resolve a registered simple dependency |
| `ctx.document.query(spec)` | `DocumentQueryPort` | Read-only document port |
| `ctx.document.command(spec)` | `DocumentCommandPort` | Read-write document port |
| `ctx.cache(spec)` | `CachePort` | Cache port (`CacheSpec`) |
| `ctx.counter(spec)` | `CounterPort` | Namespace-scoped counter (`CounterSpec`) |
| `ctx.tx.resolver(route)` | `TransactionManagerPort` | Transaction manager |
| `ctx.storage(spec)` | `StoragePort` | Object storage (`StorageSpec`) |
| `ctx.search.query(spec)` | `SearchQueryPort` | Full-text search port |
| `ctx.tx.scope(route)` | async context manager | Enter a transaction scope |

When `DocumentSpec.cache` is set, the registered document query/command factory resolves `ctx.cache(spec.cache)` while building the adapter.

Nested `ctx.tx.scope()` calls reuse the same transaction with savepoints when the backend supports them.

## Handlers

A **handler** is a single, well-defined business action. Built-in document handlers are attrs classes that take ports in `__init__` and implement `async def __call__(self, args) -> R`:

    :::python
    from uuid import UUID

    from forze.application.contracts.document import DocumentQueryPort
    from forze.application.contracts.execution import Handler

    class GetProject(Handler[UUID, ProjectReadModel]):
        doc: DocumentQueryPort[ProjectReadModel]

        async def __call__(self, args: UUID) -> ProjectReadModel:
            return await self.doc.get(args)

Register handlers on `OperationRegistry` and resolve them through a frozen registry (see [Middleware & Plans](../reference/middleware-plans.md)).

## Stage hooks

Stage hooks are authored as `BeforeStep`, `OnSuccessStep`, and related types on `OperationRegistry.bind(...)`. They run before, around, or after the handler without replacing the domain result.

Example precondition as `BeforeStep`:

    :::python
    from forze.application.contracts.execution import BeforeStep
    from forze.base.errors import ValidationError

    def require_active_project_factory(ctx):
        async def _before(args: UUID) -> None:
            doc = ctx.document.query(project_spec)
            project = await doc.get(args)
            if project.is_deleted:
                raise ValidationError("Project is archived.")
        return _before

    step = BeforeStep(id="active_check", factory=require_active_project_factory)

Bind transaction routes and stages when building the registry:

    :::python
    registry = (
        build_document_registry(project_spec, dtos)
        .bind(*write_ops)
        .bind_tx()
        .set_route("default")
        .finish(deep=True)
        .freeze()
    )

See [Capability execution](../reference/capability-execution.md) for `requires` / `provides` on graph steps.

## Execution runtime

The `ExecutionRuntime` combines the dependency plan, lifecycle plan, and execution context into a scoped runtime:

    :::python
    from forze.application.execution import (
        ExecutionRuntime,
        DepsPlan,
        LifecyclePlan,
    )

    runtime = ExecutionRuntime(
        deps=deps_plan,
        lifecycle=lifecycle_plan,
    )

Use `scope()` as an async context manager:

    :::python
    async with runtime.scope():
        ctx = runtime.get_context()

The scope lifecycle:

1. **Create context**: build the dependency container from the deps plan
2. **Startup**: run all lifecycle startup hooks in order
3. **Yield**: the application runs
4. **Shutdown**: run all lifecycle shutdown hooks in reverse order
5. **Reset**: clear the context

If a startup hook fails, already-executed steps are shut down in reverse before the exception propagates.

## Dependency plan

A `DepsPlan` collects module callables and merges them into a single `Deps` container on build:

    :::python
    from forze.application.execution import Deps, DepsPlan

    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(postgres_module(), redis_module()),
    )

`Deps` is an in-memory container keyed by `DepKey[T]`. Each integration package provides a `DepsModule` that registers its adapters and clients. `Deps.merge()` combines multiple containers and raises `CoreError` on key conflicts.

You can also build plans incrementally:

    :::python
    plan = DepsPlan.from_modules(base_module)
    plan = plan.with_modules(cache_module, search_module)

## Lifecycle plan

The `LifecyclePlan` manages startup and shutdown hooks for infrastructure clients:

    :::python
    from forze.application.execution import LifecyclePlan

    lifecycle = LifecyclePlan.from_steps(
        postgres_lifecycle_step(dsn="postgresql://..."),
        redis_lifecycle_step(dsn="redis://..."),
    )

Each `LifecycleStep` has a unique name, a startup hook, and a shutdown hook. Steps run in order at startup and in reverse order at shutdown. Name collisions raise `CoreError`.

Integration packages provide factory functions (e.g. `postgres_lifecycle_step()`) that create pre-configured steps.

## Document operations

Forze ships built-in handlers for standard document CRUD:

| Operation | Handler class | Args | Returns |
|-----------|--------------|------|---------|
| `GET` | `GetDocument` | `DocumentIdDTO` | `R` (read model) |
| `CREATE` | `CreateDocument` | `C` (create cmd) | `R` |
| `UPDATE` | `UpdateDocument` | `DocumentUpdateDTO[U]` | `DocumentUpdateRes[R]` |
| `KILL` | `KillDocument` | `DocumentIdDTO` | `None` |
| `LIST` | `ListDocuments` | list request DTO | `Paginated[R]` |
| `RAW_LIST` | `ProjectedListDocuments` | projected list request | projected page |

See [Composition & Mapping](../reference/composition.md) for `build_document_registry`.

## Facades

`DocumentFacade` ties together an execution context and a frozen registry. It provides typed access to resolved handlers:

    :::python
    from forze.application.composition.document import (
        DocumentDTOs,
        DocumentFacade,
        build_document_registry,
    )

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos).freeze()

    facade = DocumentFacade(ctx=ctx, registry=registry, namespace=project_spec.default_namespace)
    project = await facade.create(CreateProjectCmd(title="New"))

The facade exposes typed attributes: `get`, `create`, `update`, `kill`, and others depending on the spec.

Similarly, `SearchFacade` provides `search` and related operations for full-text search.

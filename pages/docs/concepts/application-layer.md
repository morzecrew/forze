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
| `ctx.document.query(spec)` / `ctx.doc.query(spec)` | `DocumentQueryPort` | Read-only document port (`doc` is an alias) |
| `ctx.document.command(spec)` / `ctx.doc.command(spec)` | `DocumentCommandPort` | Read-write document port |
| `ctx.cache(spec)` | `CachePort` | Cache port (`CacheSpec`) |
| `ctx.counter(spec)` | `CounterPort` | Namespace-scoped counter (`CounterSpec`) |
| `ctx.tx_ctx.resolver(route)` | `TransactionManagerPort` | Transaction manager |
| `ctx.storage(spec)` | `StoragePort` | Object storage (`StorageSpec`) |
| `ctx.search.query(spec)` | `SearchQueryPort` | Full-text search port |
| `ctx.search.command(spec)` | `SearchCommandPort` | External index maintenance (when wired) |
| `ctx.tx_ctx.scope(route)` | async context manager | Enter a transaction scope |

When `DocumentSpec.cache` is set, the registered document query/command factory resolves `ctx.cache(spec.cache)` while building the adapter.

Nested `ctx.tx_ctx.scope()` calls reuse the same transaction with savepoints when the backend supports them.

## Handlers

A **handler** is a single, well-defined business action. Built-in document handlers are attrs classes that take ports in `__init__` and implement `async def __call__(self, args) -> R`:

    :::python
    from forze.application.contracts.document import DocumentQueryPort
    from forze.application.contracts.execution import Handler
    from forze_kits.aggregates.document.handlers import DocumentIdDTO, GetDocument

    # Factory registered on OperationRegistry (see build_document_registry)
    handler_factory = lambda ctx: GetDocument(doc=ctx.document.query(project_spec))

Register handlers on `OperationRegistry` and resolve them through a frozen registry (see [Middleware & Plans](../reference/middleware-plans.md)). Kernel handlers such as `GetDocument` take `DocumentIdDTO` (not a bare UUID) as args.

## Stage hooks

Stage hooks are authored as `BeforeStep`, `OnSuccessStep`, and related types on `OperationRegistry.bind(...)`. They run before, around, or after the handler without replacing the domain result.

Example precondition as `BeforeStep` on an outer scope:

    :::python
    from forze_kits.aggregates.document import build_document_registry
    from forze.application.contracts.execution import BeforeStep
    from forze_kits.aggregates.document.handlers import DocumentIdDTO
    from forze.base.errors import ValidationError

    def require_active_project_factory(ctx):
        async def _before(args: DocumentIdDTO) -> None:
            doc = ctx.document.query(project_spec)
            project = await doc.get(args.id)
            if project.is_deleted:
                raise ValidationError("Project is archived.")
        return _before

    active_check = BeforeStep(id="active_check", factory=require_active_project_factory)

    registry = (
        build_document_registry(project_spec, dtos)
        .bind(project_spec.default_namespace.key("update"))
        .bind_tx()
        .set_route("default")
        .finish(deep=False)
        .bind_outer()
        .before(active_check)
        .finish(deep=True)
        .freeze()
    )

See [Capability execution](../reference/capability-execution.md) for `requires` / `provides` on graph steps and [Middleware & Plans](../reference/middleware-plans.md) for the full binder flow.

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

The `LifecyclePlan` manages startup and shutdown hooks for infrastructure clients. Canonical API details live in [Execution](../reference/execution.md#lifecycle).

    :::python
    from forze.application.execution import LifecyclePlan
    from forze_postgres import PostgresLifecycleModule, PostgresDepsModule
    from forze_redis.execution.lifecycle import redis_lifecycle_step

    lifecycle = LifecyclePlan.from_modules(
        PostgresLifecycleModule(client=pg, dsn="postgresql://..."),
    ).with_steps(redis_lifecycle_step(dsn="redis://..."))

Each `LifecycleStep` is a `GraphStep` with `requires`, `provides`, and `depends_on` for declarative ordering at `LifecyclePlan.freeze()`. Steps run by wave at startup (forward) and shutdown (reverse). If startup fails, completed steps are shut down in reverse wave order before the exception propagates.

Use `LifecycleModule` implementations (for example `PostgresLifecycleModule`) or step factories (`postgres_lifecycle_step()`, …). Deps registration stays on `DepsModule`; lifecycle stays separate.

### Routed clients

When using tenant-routed clients (`RoutedPostgresClient`, `RoutedRedisClient`, and similar), prefer the integration lifecycle step for that client. Those steps call `routed_client_lifecycle_step` internally, which runs `startup` / `close` on the routed client pool. Do not hand-roll connect/disconnect on a shared non-routed client when routing is enabled. See [Multi-tenancy](multi-tenancy.md) and [Postgres integration](../integrations/postgres.md#operational-notes).

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
| `LIST_CURSOR` | `CursorListDocuments` | cursor list request DTO | cursor page |
| `RAW_LIST_CURSOR` | `ProjectedCursorListDocuments` | projected cursor list request | projected cursor page |
| `AGG_LIST` | `AggregatedListDocuments` | aggregated list request | aggregated page |

Soft delete and restore use separate registry builders (`build_soft_deletion_registry`), not `DocumentKernelOp` suffixes.

See [Composition & Mapping](../reference/composition.md) for `build_document_registry`.

## Facades

`DocumentFacade` ties together an execution context and a frozen registry. It provides typed access to resolved handlers:

    :::python
    from forze_kits.aggregates.document import (
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

Similarly, `SearchFacade` exposes `search`, `projected_search`, `cursor_search`, and `projected_cursor_search` for full-text search.

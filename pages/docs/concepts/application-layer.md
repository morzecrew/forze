# Application Layer

## What problem this solves

Usecases need to coordinate domain logic and infrastructure without importing concrete adapters directly.

## When you need this

Use this when you write operations, resolve dependencies, add middleware, or build an execution runtime.


The application layer **orchestrates** domain logic and coordinates infrastructure. It defines *what* happens, not *how* persistence or transport work.

## Key ideas

The application layer is built around a few core concepts:

- **Execution context**: the single point through which usecases resolve all dependencies. No usecase ever imports an adapter directly.
- **Usecases**: self-contained operations that implement one business action. Each usecase receives an execution context and resolves typed ports from it.
- **Middleware**: composable wrappers (guards, effects, transaction boundaries) that run before, around, or after a usecase without modifying its core logic.
- **Runtime**: the container that manages dependency injection, lifecycle hooks, and context creation.

Together these ensure that business logic remains independent of infrastructure choices.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/execution-runtime.svg" alt="Execution runtime overview">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/execution-runtime.svg" alt="Execution runtime overview">
</div>

## Execution context

`ExecutionContext` is the central dependency resolution point. Usecases and factories receive it and use its methods to obtain typed ports:

| Method | Returns | Purpose |
|--------|---------|---------|
| `dep(key)` | `T` | Resolve any dependency by typed key |
| `doc_query(spec)` | `DocumentQueryPort` | Read-only document port |
| `doc_command(spec)` | `DocumentCommandPort` | Read-write document port |
| `cache(spec)` | `CachePort` | Cache port (`CacheSpec`) |
| `counter(spec)` | `CounterPort` | Namespace-scoped counter (`CounterSpec`) |
| `txmanager(route)` | `TxManagerPort` | Transaction manager |
| `storage(spec)` | `StoragePort` | Object storage (`StorageSpec`) |
| `search_query(spec)` | `SearchQueryPort` | Full-text search port |
| `transaction()` | async context manager | Enter a transaction scope |

When `DocumentSpec.cache` is set, the registered document query/command factory resolves `ctx.cache(spec.cache)` while building the adapter.

Nested `transaction()` calls reuse the same transaction with savepoints when the backend supports them.

## Usecases

A **usecase** is a single, well-defined business action. It subclasses `Usecase[Args, R]`, implements `main()`, and is invoked via `__call__()` which runs the full middleware chain:

    :::python
    from uuid import UUID
    from forze.application.execution import Usecase

    class GetProject(Usecase[UUID, ProjectReadModel]):
        async def main(self, args: UUID) -> ProjectReadModel:
            doc = self.ctx.doc_query(project_spec)
            return await doc.get(args)


    class CreateProject(Usecase[CreateProjectCmd, ProjectReadModel]):
        async def main(self, args: CreateProjectCmd) -> ProjectReadModel:
            doc = self.ctx.doc_command(project_spec)
            return await doc.create(args)

Every usecase has:

- `ctx`: the execution context for resolving ports
- `middlewares`: a tuple of middleware wrappers (guards, effects, transaction)
- `with_middlewares(*mw)`: returns a new usecase with additional middlewares appended

## Middleware system

Middlewares wrap the usecase call chain. Three protocol types exist:

**Guard**: runs before the usecase. Raises to abort:

    :::python
    from forze.application.execution import Guard

    class RequireActiveProject(Guard[UUID]):
        async def __call__(self, args: UUID) -> None:
            doc = self.ctx.doc_query(project_spec)
            project = await doc.get(args)

            if project.is_deleted:
                raise ValidationError("Project is archived.")

**Effect**: runs after the usecase returns. May transform the result:

    :::python
    from forze.application.execution import Effect

    class LogCreation(Effect[CreateProjectCmd, ProjectReadModel]):
        async def __call__(
            self,
            args: CreateProjectCmd,
            res: ProjectReadModel,
        ) -> ProjectReadModel:
            logger.info("Created project %s", res.id)
            return res

**Middleware**: wraps the next call with full control:

    :::python
    from forze.application.execution import Middleware, NextCall

    class TimingMiddleware(Middleware[Any, Any]):
        async def __call__(self, next: NextCall, args: Any) -> Any:
            start = time.monotonic()
            result = await next(args)
            elapsed = time.monotonic() - start
            logger.info("Elapsed: %.3fs", elapsed)

            return result

Built-in middleware implementations:

| Class | Purpose |
|-------|---------|
| `GuardMiddleware` | Wraps a `Guard`: runs it before `next` |
| `EffectMiddleware` | Wraps an `Effect`: runs it after `next` |
| `TxMiddleware` | Wraps `next` inside `ctx.transaction("default")`, supports after-commit effects |

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

Forze ships built-in usecases for standard document CRUD:

| Operation | Usecase class | Args | Returns |
|-----------|--------------|------|---------|
| `GET` | `GetDocument` | `DocumentIdDTO` | `R` (read model) |
| `CREATE` | `CreateDocument` | `C` (create cmd) | `R` |
| `UPDATE` | `UpdateDocument` | `DocumentUpdateDTO[U]` | `DocumentUpdateRes[R]` |
| `KILL` | `KillDocument` | `DocumentIdDTO` | `None` |
| `DELETE` | `DeleteDocument` | `DocumentIdRevDTO` | `R` |
| `RESTORE` | `RestoreDocument` | `DocumentIdRevDTO` | `R` |
| `LIST` | `TypedListDocuments` | `tL` (list request) | `Paginated[R]` |
| `RAW_LIST` | `RawListDocuments` | `rL` (raw list request) | `RawPaginated` |


## Facades

A `DocumentUsecasesFacade` ties together an execution context and a registry. It provides typed access to resolved usecases:

    :::python
    from forze.application.composition.document import (
        DocumentDTOs,
        DocumentUsecasesFacade,
        build_document_registry,
    )

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)

    facade = DocumentUsecasesFacade(ctx=ctx, reg=registry)
    project = await facade.create(CreateProjectCmd(title="New"))

The facade exposes typed attributes: `get`, `create`, `update`, `kill`, `delete`, `restore`. Each resolves a composed `Usecase` from the registry.

Similarly, `SearchUsecasesFacade` provides `search` and `raw_search` attributes for full-text search operations.

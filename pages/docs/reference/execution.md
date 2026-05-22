# Execution

The execution engine manages dependency injection, context creation, and application lifecycle. It connects domain models and contracts to infrastructure adapters at runtime. For the conceptual overview, see [Application Layer](../concepts/application-layer.md). Optional **capability-driven** guard/effect ordering lives on `UsecaseRegistry` stage authoring; see [Capability execution](capability-execution.md).

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/deps-resolution.svg" alt="Dependency resolution from DepsPlan through modules, Deps, keys, and ports">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/deps-resolution.svg" alt="Dependency resolution from DepsPlan through modules, Deps, keys, and ports">
</div>

## ExecutionContext

The central dependency resolution point. Every usecase and factory receives an `ExecutionContext` to resolve infrastructure ports:

    :::python
    from forze.application.execution import ExecutionContext

    doc = ctx.doc_query(project_spec)
    result = await doc.get(some_id)

### Resolution methods

| Method | Returns | Purpose |
|--------|---------|---------|
| `dep(key)` | `T` | Resolve any dependency by typed key |
| `doc_query(spec)` | `DocumentQueryPort` | Read-only document port |
| `doc_command(spec)` | `DocumentCommandPort` | Read-write document port |
| `cache(spec)` | `CachePort` | Cache port for a namespace |
| `counter(spec)` | `CounterPort` | Namespace-scoped counter (`CounterSpec`) |
| `txmanager(route)` | `TxManagerPort` | Transaction manager for a registered route |
| `storage(spec)` | `StoragePort` | Object storage (`StorageSpec`) |
| `search_query(spec)` | `SearchQueryPort` | Full-text search port |

When `DocumentSpec.cache` is set, the document dep factory resolves `ctx.cache(spec.cache)` while building the adapter. TTL defaults come from `CacheSpec`.

### Transactions

`transaction()` returns an async context manager that scopes a transaction:

    :::python
    async with ctx.transaction("default"):
        doc = ctx.doc_command(project_spec)
        await doc.create(CreateProjectCmd(title="New"))
        await doc.create(CreateProjectCmd(title="Another"))
        # Both creates commit or roll back together

Nested calls reuse the same transaction. Savepoints are used when the backend supports them:

    :::python
    async with ctx.transaction("default"):
        # outer transaction
        async with ctx.transaction("default"):
            # nested: same transaction, savepoint

The context validates that ports resolved inside a transaction match the active transaction scope — mixing different transaction managers (e.g. Postgres and Mongo) raises `CoreError`.

### Cycle detection

Each `Deps` container owns a resolution stack per async task (via a per-instance `ContextVar`). Stacks are isolated across containers: two `Deps` instances in the same task do not share cycle state. A resolution frame is `(DepKey.name, optional route)` — for example `document_query@items` or `postgres_client` when no route is used.

| API | When to use |
|-----|-------------|
| `resolve_configurable(ctx, key, spec, route=...)` | Configurable ports: lookup factory and call `factory(ctx, spec)` under a scope |
| `resolve_simple(ctx, key, route=...)` | Simple ports: lookup factory and call `factory(ctx)` under a scope |
| `resolution_scope(key, route=...)` | When the caller owns factory invocation (for example transaction manager resolution) |
| `provide(key, route=...)` | Plain lookup; raises `CoreError` if the same frame is already on this container's stack |

If a factory (or misconfigured container) requests the same frame again while it is still resolving, Forze raises `CoreError` with a chain such as `Cyclic dependency resolution: document_query@test -> tx_manager@mock -> document_query@test` instead of exhausting the Python stack.

Plain singleton lookups (lifecycle hooks fetching a shared client) use `provide()` when no conflicting frame is active — nested `provide(PostgresClientDepKey)` inside an outer port factory is allowed because the frames differ. When resolution tracing is enabled, `provide()` under an active stack still records an observed edge to the looked-up frame (without pushing it).

#### Observed resolution graph (development)

Enable tracing on a container with `Deps(trace_resolution=True)` or set `FORZE_DEPS_TRACE=1` (or `true` / `yes`) before `DepsPlan.build()` (unless `build(trace_resolution=False)` overrides it). While resolving, Forze records directed edges `(parent, child)` where the child depends on the parent — on scope push and on `provide()` under an active stack.

Read the current task's trace with `deps.resolution_trace()` and export a snapshot via `trace.to_dag()` (`DirectedAcyclicGraph` for topological order). `registered_frames()` lists all statically registered frames. Tracing is diagnostic only; runtime stack checks remain the production guard.

## Dependencies

### DepKey

A typed key identifying a dependency. Used for both registration (in dep modules) and resolution (via `ctx.deps`):

    :::python
    from forze.application.contracts.base import DepKey

    MyClientKey = DepKey[MyClient]("my_client")

### Deps

In-memory dependency container implementing `DepsPort`:

    :::python
    from forze.application.execution import Deps

    deps = Deps(deps={
        DocumentQueryDepKey: my_doc_query_factory,
        CacheDepKey: my_cache_factory,
    })

| Method | Purpose |
|--------|---------|
| `provide(key, route=...)` | Look up a registered provider or instance; cycle-check only |
| `resolve_configurable(ctx, key, spec, route=...)` | Resolve a configurable port under a scope |
| `resolve_simple(ctx, key, route=...)` | Resolve a simple port under a scope |
| `resolution_scope(key, route=...)` | Context manager for custom lookup + invoke |
| `resolution_trace()` | Observed edges for the current task when tracing is enabled |
| `registered_frames()` | Static inventory of registered frames |
| `exists(key, route=...)` | Check registration |
| `merge(*deps)` | Combine containers; raises `CoreError` on key conflicts |
| `without(key)` | Return a container without the given key |
| `empty()` | Check if the container is empty |

`Deps.merge()` catches misconfigured plans early by failing on duplicate keys.

### DepsModule

Protocol for a callable that produces a `Deps` container. Integration packages expose modules that register their adapters:

    :::python
    from forze.application.execution import Deps, DepsModule

    def postgres_module() -> Deps:
        return Deps(deps={
            DocumentQueryDepKey: pg_doc_query_factory,
            DocumentCommandDepKey: pg_doc_command_factory,
            TxManagerDepKey: pg_tx_factory,
        })

### DepsPlan

Declarative plan that collects `DepsModule` callables and merges them into a single `Deps` on build:

    :::python
    from forze.application.execution import DepsPlan

    plan = DepsPlan.from_modules(
        postgres_module,
        redis_module,
    )

    # Or build incrementally
    plan = DepsPlan.from_modules(base_module)
    plan = plan.with_modules(cache_module, search_module)

| Method | Purpose |
|--------|---------|
| `from_modules(*modules)` | Create a plan from modules |
| `with_modules(*modules)` | Return a new plan with additional modules |
| `build()` | Invoke all modules and merge into a single `Deps` |

When `build()` is called, each module callable is invoked and the results are merged via `Deps.merge()`.

## Lifecycle

### LifecycleHook

Protocol for startup/shutdown hooks. Receives the `ExecutionContext`:

    :::python
    async def startup_postgres(ctx: ExecutionContext) -> None:
        client = ctx.deps.provide(PostgresClientKey)
        await client.connect()

    async def shutdown_postgres(ctx: ExecutionContext) -> None:
        client = ctx.deps.provide(PostgresClientKey)
        await client.disconnect()

### LifecycleStep

Named pair of startup and shutdown hooks:

    :::python
    from forze.application.execution import LifecycleStep

    pg_step = LifecycleStep(
        name="postgres",
        startup=startup_postgres,
        shutdown=shutdown_postgres,
    )

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `name` | `str` | — | Unique name for collision detection |
| `startup` | `LifecycleHook` | no-op | Hook to run on startup |
| `shutdown` | `LifecycleHook` | no-op | Hook to run on shutdown |

Integration packages typically provide factory functions that return pre-configured steps.

### LifecyclePlan

Ordered sequence of lifecycle steps:

    :::python
    from forze.application.execution import LifecyclePlan

    lifecycle = LifecyclePlan.from_steps(pg_step, redis_step)
    lifecycle = lifecycle.with_steps(s3_step)

| Method | Purpose |
|--------|---------|
| `from_steps(*steps)` | Create a plan; raises on name collisions |
| `with_steps(*steps)` | Append steps; raises on name collisions |
| `startup(ctx)` | Run startup hooks in order |
| `shutdown(ctx)` | Run shutdown hooks in reverse order |

Startup behavior: if a hook fails, all previously started steps are shut down in reverse order before re-raising. Shutdown behavior: exceptions are swallowed so all steps are attempted.

## ExecutionRuntime

Combines the dependency plan, lifecycle plan, and context into a scoped runtime:

    :::python
    from forze.application.execution import ExecutionRuntime

    runtime = ExecutionRuntime(
        deps=deps_plan,
        lifecycle=lifecycle_plan,
    )

Use `scope()` as an async context manager:

    :::python
    async with runtime.scope():
        ctx = runtime.get_context()
        # Application runs here

### Scope lifecycle

1. **Create context** — build `Deps` from the deps plan, store in a `RuntimeVar`
2. **Startup** — run all lifecycle startup hooks in order
3. **Yield** — the application runs
4. **Shutdown** — run all lifecycle shutdown hooks in reverse order
5. **Reset** — clear the context

### Methods

| Method | Purpose |
|--------|---------|
| `get_context()` | Return the current `ExecutionContext`; raises if not in scope |
| `create_context()` | Build and store the context (idempotent within a scope) |
| `startup()` | Run lifecycle startup hooks |
| `shutdown()` | Run lifecycle shutdown hooks and reset context |
| `scope()` | Async context manager combining all of the above |

The runtime is typically created once at application startup (e.g. in a FastAPI lifespan) and shared across requests:

    :::python
    from contextlib import asynccontextmanager
    from fastapi import FastAPI

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with runtime.scope():
            yield

    app = FastAPI(lifespan=lifespan)

## Putting it together

A complete wiring example showing deps, lifecycle, and runtime:

    :::python
    from forze.application.execution import (
        Deps,
        DepsPlan,
        ExecutionRuntime,
        LifecyclePlan,
        LifecycleStep,
    )

    # 1. Define dep modules
    def infra_module() -> Deps:
        return Deps.merge(
            postgres_deps_module(),
            redis_deps_module(),
        )

    # 2. Build plans
    deps_plan = DepsPlan.from_modules(infra_module)

    lifecycle_plan = LifecyclePlan.from_steps(
        LifecycleStep(
            name="postgres",
            startup=pg_startup,
            shutdown=pg_shutdown,
        ),
        LifecycleStep(
            name="redis",
            startup=redis_startup,
            shutdown=redis_shutdown,
        ),
    )

    # 3. Create runtime
    runtime = ExecutionRuntime(
        deps=deps_plan,
        lifecycle=lifecycle_plan,
    )

    # 4. Use in application
    async with runtime.scope():
        ctx = runtime.get_context()
        doc = ctx.doc_query(project_spec)
        page = await doc.find_many(pagination={"limit": 10, "offset": 0})
        projects = page.hits

## Troubleshooting

| Symptom | Likely cause | Fix | See also |
|---------|--------------|-----|----------|
| A lifecycle startup step did not run before a request or usecase. | The runtime scope was not entered, or `startup()` was not called from the application lifespan. | Use `async with runtime.scope()` for tests and workers, or call `runtime.startup()`/`runtime.shutdown()` from the framework lifespan. | [FastAPI integration](../integrations/fastapi.md#lifecycle-step) |
| Resolving `ctx.document.query(...)`, `ctx.deps.provide(...)`, or another helper raises a missing dependency error. | The `DepsPlan` does not include the module that registers that key/route, or the route does not match the spec name. | Add the correct integration deps module and verify the spec name, dependency key, and route are registered together. | [Specs and wiring](../concepts/specs-and-wiring.md) |
| `Cyclic dependency resolution: ...` from `Deps`. | A factory calls back into a dependency that is already on the resolution stack (same key and route). | Break the cycle in module wiring or split dependencies; do not rely on infinite recursion. | [Cycle detection](#cycle-detection) |
| `Deps.merge()` raises a key collision while building the plan. | Multiple modules provide the same dependency key and route. | Register only one provider per key/route, or separate providers with routed names before merging modules. | [Dependencies](#dependencies) |

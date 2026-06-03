# Execution

The execution engine manages dependency injection, context creation, and application lifecycle. It connects domain models and contracts to infrastructure adapters at runtime. For the conceptual overview, see [Application Layer](../concepts/application-layer.md). Stage hooks and operation plans live on `OperationRegistry`; see [Middleware & Plans](middleware-plans.md) and [Capability execution](capability-execution.md).

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/deps-resolution.svg" alt="Dependency resolution from DepsRegistry through modules, Deps, keys, and ports">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/deps-resolution.svg" alt="Dependency resolution from DepsRegistry through modules, Deps, keys, and ports">
</div>

## ExecutionContext

The central dependency resolution point. Every handler factory and lifecycle hook receives an `ExecutionContext` to resolve infrastructure ports:

    :::python
    from forze.application.execution import ExecutionContext

    doc = ctx.document.query(project_spec)
    result = await doc.get(some_id)

`ExecutionContext` exposes nested convenience layers:

| Attribute | Purpose |
|-----------|---------|
| `deps` | Underlying `Deps` container (`provide`, `resolve_configurable`, …) |
| `document` / `doc` | Document query and command ports |
| `search` | Search, hub search, federated search, and snapshot ports |
| `cache`, `counter`, `storage`, `embeddings`, `dlock` | Other configurable ports |
| `tenancy` | Tenant resolver and management ports |
| `tx_ctx` | Transaction scope and manager resolution |
| `inv_ctx` | Invocation metadata, authn, and tenant binding |

### Port resolution

| API | Returns | Purpose |
|-----|---------|---------|
| `ctx.deps.provide(key, route=...)` | `T` | Resolve a registered simple dependency |
| `ctx.document.query(spec)` | `DocumentQueryPort` | Read-only document port |
| `ctx.document.command(spec)` | `DocumentCommandPort` | Read-write document port |
| `ctx.doc.query` / `ctx.doc.command` | same | Alias for `document` |
| `ctx.cache(spec)` | `CachePort` | Cache port for a namespace |
| `ctx.counter(spec)` | `CounterPort` | Namespace-scoped counter (`CounterSpec`) |
| `ctx.storage(spec)` | `StoragePort` | Object storage (`StorageSpec`) |
| `ctx.search.query(spec)` | `SearchQueryPort` | Full-text search port |
| `ctx.search.hub(spec)` | `SearchQueryPort` | Hub (multi-leg) search |
| `ctx.search.federated(spec)` | `SearchQueryPort` | Federated search |
| `ctx.tx_ctx.resolver(route)` | `TransactionManagerPort` | Transaction manager for a registered route |

When `DocumentSpec.cache` is set, the document dep factory resolves `ctx.cache(spec.cache)` while building the adapter. TTL defaults come from `CacheSpec`.

### Transactions

Use `ctx.tx_ctx.scope(route)` as an async context manager:

    :::python
    async with ctx.tx_ctx.scope("default"):
        doc = ctx.document.command(project_spec)
        await doc.create(CreateProjectCmd(title="New"))
        await doc.create(CreateProjectCmd(title="Another"))
        # Both creates commit or roll back together

Nested scopes reuse the same transaction. Savepoints are used when the backend supports them:

    :::python
    async with ctx.tx_ctx.scope("default"):
        # outer transaction
        async with ctx.tx_ctx.scope("default"):
            # nested: same transaction, savepoint

The context validates that ports resolved inside a transaction match the active transaction scope — mixing different transaction managers (e.g. Postgres and Mongo) raises `CoreError`.

Queue work to run after a successful root commit with `ctx.tx_ctx.defer_after_commit(callback)` (see transaction context helpers in source).

### Invocation metadata

Bind per-request metadata, authn, and tenant context for the duration of a call:

    :::python
    from forze.application.execution import ExecutionContext, InvocationMetadata

    metadata = InvocationMetadata(
        execution_id=...,
        correlation_id=...,
        causation_id=...,
    )
    with ctx.inv_ctx.bind(metadata=metadata, authn=identity, tenant=tenant):
        # handlers and ports see bound identity / tenant
        principal = ctx.inv_ctx.get_authn()
        tenant_ctx = ctx.inv_ctx.get_tenant()

| Method | Purpose |
|--------|---------|
| `ctx.inv_ctx.bind(metadata=..., authn=..., tenant=...)` | Context manager; merges structlog context vars |
| `ctx.inv_ctx.get_metadata()` | Current `InvocationMetadata` or `None` |
| `ctx.inv_ctx.get_authn()` | Current `AuthnIdentity` or `None` |
| `ctx.inv_ctx.get_tenant()` | Current `TenantIdentity` or `None` |

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

Enable resolution tracing on the **registry** (preferred) or a one-off container:

    :::python
    registry = DepsRegistry.from_modules(postgres_module).with_tracing(resolution=True)
    frozen = registry.freeze()

Or set `FORZE_DEPS_TRACE=1` (or `true` / `yes`) and call `registry.freeze()` (unless `freeze(trace_resolution=False)` overrides env).

While resolving, Forze records directed edges `(parent, child)` where the child depends on the parent — on scope push and on `provide()` under an active stack. Recording is handled by `ResolutionTracer` on `Deps` (`resolution_tracer`); cycle detection stays on the container stack regardless of tracing.

Read the current task's trace with `deps.resolution_trace()` and export via `trace.to_dag()` (routed frames) or `trace.to_key_dag()` (canonical key-level graph with routes collapsed). `registered_frames()` lists all statically registered frames. `Deps.merge()` combines registries only — it does not enable tracing from partial containers unless you pass `resolution_tracer=` explicitly.

#### Runtime tracing (development)

Enable runtime tracing on the registry:

    :::python
    registry = DepsRegistry.from_modules(mock_module).with_tracing(runtime=True)
    frozen = registry.freeze()

Or set `FORZE_RUNTIME_TRACE=1` (or `true` / `yes`) before `registry.freeze()` (unless `freeze(trace_runtime=False)` overrides env).

Root transaction scope boundaries use `TxTracer` on `TransactionContext`, wired from `ExecutionContext` via `tx_tracer_from_runtime(deps.runtime_tracer)` when runtime tracing is enabled — no separate flag. Only root `ctx.tx_ctx.scope(...)` enter/exit is recorded (nested scopes do not emit extra tx events).

While a handler runs, Forze records transaction scope boundaries and **configurable port** calls (via `Deps.resolve_configurable`) at the coordinator boundary — internal gateway reads after writes are not traced.

Read the current task's sequence with `deps.runtime_trace()` and log-friendly lines via `trace.format_lines()`. Pass an integration-specific validator to `validate_runtime_trace(trace, validator=...)` — for example Firestore's `validate_reads_before_writes_in_tx` from `forze_firestore.execution.trace_validation`, which flags `document_query` reads after `document_command` writes in the same transaction segment (reads after `tx.exit` are allowed). Use `on_violation="raise"` or `assert_runtime_trace_valid(trace, validator)` for test failures with a full report.

**Development workflow**

| Tool | Purpose |
|------|---------|
| `run_traced_operation(registry, op, args, ctx, validators=...)` | Mock dry-run: run handler, return result + trace + violations |
| `assert_trace_contains` / `assert_trace_equals` | Golden subsequence checks on `TracingEvent` lists |
| `FORZE_RUNTIME_TRACE_LOG=1` | Log full trace at DEBUG after `run_traced_operation` (or call `log_runtime_trace(deps)`) |
| `tests.support.runtime_tracing` | `traced_deps`, `traced_ctx`, `assert_deps_runtime_trace_valid` for pytest |

`ExecutionRuntime` picks up tracing when `FORZE_RUNTIME_TRACE` is set before `DepsRegistry.freeze()`, or use `DepsRegistry(...).with_tracing(runtime=True).freeze()`.

**Limitations:** port/coordinator boundary only (not gateway internals); `resolve_simple` records `op=resolve` (dependency lookup, not port methods); sync and async port methods are traced; buffer capped at `RuntimeTrace.MAX_EVENTS` (10_000) with a `tracing truncated` marker. Tracing is diagnostic only; production code should not rely on it.

Example trace lines:

```text
0000 tx enter route=mock tx=mock depth=1
0001 document count surface=document_query route=projects phase=query depth=1
0002 tx exit route=mock tx=mock depth=1
```

## Dependencies

### DepKey

A typed key identifying a dependency. Used for both registration (in dep modules) and resolution (via `ctx.deps`):

    :::python
    from forze.application.contracts.base import DepKey

    MyClientKey = DepKey[MyClient]("my_client")

### Deps

In-memory dependency container implementing `DepsPort`. Internally composes a
:class:`~forze.application.execution.deps.registry.DepsRegistry` (static wiring),
a :class:`~forze.application.execution.deps.resolution_context.ResolutionContext`
(cycle stack), and optional tracers; the public surface remains `Deps`.

    :::python
    from forze.application.execution import Deps

    deps = Deps(deps={
        DocumentQueryDepKey: my_doc_query_factory,
        CacheDepKey: my_cache_factory,
    })

| Method | Purpose |
|--------|---------|
| `get_provider(key, route=...)` | Look up a registered factory or instance without cycle checks |
| `provide(key, route=...)` | Look up a registered provider or instance; cycle-check only |
| `resolve_configurable(ctx, key, spec, route=...)` | Resolve a configurable port under a scope |
| `resolve_simple(ctx, key, route=...)` | Resolve a simple port under a scope |
| `resolution_scope(key, route=...)` | Context manager for custom lookup + invoke |
| `resolution_trace()` | Observed edges for the current task when tracing is enabled |
| `runtime_trace()` | Observed runtime sequence for the current task when runtime tracing is enabled |
| `registered_frames()` | Static inventory of registered frames |
| `exists(key, route=...)` | Check registration |
| `merge(*deps, resolution_tracer=..., runtime_tracer=...)` | Combine registries; optional tracers on the result |
| `resolution_tracer` / `runtime_tracer` | Composable recorders (`Noop*` default; `Recording*` when enabled) |
| `trace_resolution` / `trace_runtime` | Read-only: whether the corresponding tracer is recording |
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

### DepsRegistry

Authoring registry that collects `DepsModule` callables and merges them into a `FrozenDepsRegistry` on `freeze()`:

    :::python
    from forze.application.execution import DepsRegistry

    registry = DepsRegistry.from_modules(
        postgres_module,
        redis_module,
    )

    # Or build incrementally
    registry = DepsRegistry.from_modules(base_module)
    registry = registry.with_modules(cache_module, search_module)

| Method | Purpose |
|--------|---------|
| `from_modules(*modules)` | Create a registry from modules |
| `from_deps(*deps)` | Create a registry from pre-built registration blobs |
| `with_modules(*modules)` | Return a new registry with additional modules |
| `with_deps(*deps)` | Append pre-built registration fragments |
| `with_tracing(resolution=..., runtime=...)` | Attach tracers when `freeze()` runs (`bool` or tracer instance) |
| `freeze(trace_resolution=..., trace_runtime=...)` | Merge modules; apply tracers; return `FrozenDepsRegistry` |

When `freeze()` is called, each module callable is invoked, provider stores are merged, and tracing policy is applied once on the frozen registry. Pass the result to `ExecutionRuntime`; per-scope resolution happens via `FrozenDepsRegistry.resolve()` inside `create_context()`.

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

Pair of startup and shutdown hooks identified by `id` (a `GraphStep`):

    :::python
    from forze.application.execution.lifecycle import LifecyclePlan, LifecycleStep

    pg_step = LifecycleStep(
        id="postgres",
        startup=startup_postgres,
        shutdown=shutdown_postgres,
    )

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `id` | `StrKey` | — | Unique step identifier (used in logs and rollback traces) |
| `startup` | `LifecycleHook` | no-op | Hook to run on startup |
| `shutdown` | `LifecycleHook` | no-op | Hook to run on shutdown |
| `requires` | `tuple[StrKey, ...]` | `()` | Capability keys that must be provided by an earlier step |
| `provides` | `tuple[StrKey, ...]` | `()` | Capability keys this step provides after startup |
| `depends_on` | `tuple[StrKey, ...]` | `()` | Step ids that must run before this step |
| `priority` | `int` | `0` | Tie-break within a topological layer (higher runs first) |

`LifecycleStep` is defined in `forze.application.contracts.execution` and re-exported from `forze.application.execution.lifecycle`. Integration packages typically provide factory functions (e.g. `postgres_lifecycle_step()`) that return pre-configured steps.

Capability metadata **orders** steps at freeze time only; it does not skip hooks when a capability is missing (unlike operation `before` hooks).

#### Routed clients

For tenant-routed integration clients, use the package lifecycle step (for example `postgres_lifecycle_step` with `RoutedPostgresClient`). Internally these call `routed_client_lifecycle_step` from `forze.application.execution.lifecycle.builtin`, which invokes `startup` / `close` on the client. See [Multi-tenancy](../concepts/multi-tenancy.md) and integration guides.

### LifecycleModule

Protocol for a callable that returns lifecycle steps (mirrors `DepsModule`). Integration packages may expose attrs classes (for example `PostgresLifecycleModule`) that register pool startup plus optional follow-up steps.

### LifecyclePlan

Collects modules and/or steps, then freezes into a validated `ExecutionGraph`:

    :::python
    from forze.application.execution import LifecycleModule, LifecyclePlan

    lifecycle = LifecyclePlan.from_modules(pg_lifecycle_module, redis_lifecycle_module)
    lifecycle = lifecycle.with_steps(custom_step)
    frozen = lifecycle.freeze()

| Method | Purpose |
|--------|---------|
| `from_modules(*modules)` | Create a plan from lifecycle modules |
| `from_steps(*steps)` | Create a plan from plain steps |
| `with_modules(*modules)` | Append modules to a new plan instance |
| `with_steps(*steps)` | Append steps to a new plan instance |
| `with_concurrent(concurrent=True)` | Run steps in the same wave concurrently at runtime |
| `freeze()` | Invoke modules, merge with steps, build topological waves |
| `build()` | Deprecated alias for `freeze()` |

### FrozenLifecyclePlan and ResolvedLifecyclePlan

After `freeze()`, the plan exposes an `ExecutionGraph` with `waves`. Call `resolve(ctx)` for a runnable snapshot (identity mapping today), then `startup(ctx)` / `shutdown(ctx)`:

| Type | Purpose |
|------|---------|
| `FrozenLifecyclePlan` | Validated graph + `concurrent` flag; `resolve(ctx)` → `ResolvedLifecyclePlan` |
| `ResolvedLifecyclePlan` | Runnable plan; runs startup forward by wave, shutdown reverse by wave |

`ExecutionRuntime` accepts only `FrozenDepsRegistry` and `FrozenLifecyclePlan` (call `DepsRegistry.freeze()` and `LifecyclePlan.freeze()` first). It does not coerce authoring plans. Startup behavior: if a hook fails, completed steps are shut down in reverse wave order before re-raising. Shutdown behavior: exceptions are swallowed so all steps are attempted. When `concurrent=True`, steps within the same wave run in parallel (`asyncio.gather`); waves still run sequentially.

## Three execution plans

Forze uses three declarative plans in the execution layer. They are siblings: `ExecutionRuntime` holds **frozen** deps and lifecycle registries only; `OperationRegistry` is built and frozen separately (facades, HTTP attach, workers).

| Plan | Collects | Terminal step | Typical enablement |
|------|----------|-----------------|-------------------|
| `DepsRegistry` | modules, pre-built `Deps` | `freeze()` → `FrozenDepsRegistry` → `resolve()` → `FrozenDeps` | `DepsRegistry.from_modules(...).with_tracing(...).freeze()`; env `FORZE_DEPS_TRACE` |
| `LifecyclePlan` | modules, `LifecycleStep` | `freeze()` → `FrozenLifecyclePlan` → `resolve` → run | `LifecyclePlan.from_modules(...).freeze()` or `from_steps(...).freeze()` in app lifespan |
| `OperationRegistry` | handlers, plans, `PlanPatch` | `freeze()` → `FrozenOperationRegistry` | `.patch(selector)` / `.bind(...)` / `OperationRegistry.merge` |

**Where do I enable X?**

- Port and dependency wiring → `DepsRegistry` + `DepsModule` (`.freeze()` before `ExecutionRuntime`)
- Database and client startup → `LifecyclePlan`
- Middleware, transaction routes, dispatch → `OperationRegistry` (then `.freeze()`)

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/operation-registry.svg" alt="Operation registry freeze and facade resolution">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/operation-registry.svg" alt="Operation registry freeze and facade resolution">
</div>

See [Operation composition](../concepts/operation-composition.md) for registry authoring.

## ExecutionRuntime

Combines frozen dependency and lifecycle registries into a scoped runtime (operation registry is separate; see [Three execution plans](#three-execution-plans)). Authoring plans must be frozen first — the runtime does not coerce `DepsRegistry` or `LifecyclePlan` instances:

    :::python
    from forze.application.execution import DepsRegistry, ExecutionRuntime

    runtime = ExecutionRuntime(
        deps=deps_registry.freeze(),
        lifecycle=lifecycle_plan.freeze(),
    )

Use `scope()` as an async context manager:

    :::python
    async with runtime.scope():
        ctx = runtime.get_context()
        # Application runs here

### Scope lifecycle

1. **Create context** — resolve `FrozenDeps` from the frozen deps registry, store in a `RuntimeVar`
2. **Startup** — run lifecycle startup hooks by wave (forward)
3. **Yield** — the application runs
4. **Shutdown** — run lifecycle shutdown hooks by wave (reverse)
5. **Reset** — clear the context

### Methods

| Method | Purpose |
|--------|---------|
| `get_context()` | Return the current `ExecutionContext`; raises if not in scope |
| `create_context()` | Resolve per-scope `FrozenDeps` and store the context (idempotent within a scope) |
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

## Operation registry (summary)

Transport layers resolve handlers from a **frozen** `FrozenOperationRegistry`:

    :::python
    from forze.application.execution import OperationRegistry, make_registry_operation_resolver

    registry = (
        OperationRegistry(
            handlers={
                "projects.get": lambda ctx: GetDocument(doc=ctx.document.query(spec)),
            },
        )
        .bind("projects.get")
        .bind_outer()
        .before(before_step)
        .finish(deep=True)
        .freeze()
    )
    resolver = make_registry_operation_resolver(registry)
    handler = resolver("projects.get", ctx)
    result = await handler(args)

See [Middleware & Plans](middleware-plans.md) for stage authoring and [Composition & Mapping](composition.md) for built-in document/search/storage registries.

## Putting it together

A complete wiring example showing deps, lifecycle, runtime, and operation registry:

    :::python
    from forze.application.execution import (
        Deps,
        DepsRegistry,
        ExecutionRuntime,
        OperationRegistry,
    )
    from forze.application.execution.lifecycle import LifecyclePlan, LifecycleStep
    from forze.base.primitives import str_key_selector

    # 1. Define dep modules
    def infra_module() -> Deps:
        return Deps.merge(
            postgres_deps_module(),
            redis_deps_module(),
        )

    # 2. Build and freeze plans
    deps_registry = DepsRegistry.from_modules(infra_module)

    lifecycle_plan = LifecyclePlan.from_steps(
        LifecycleStep(
            id="postgres",
            startup=pg_startup,
            shutdown=pg_shutdown,
        ),
        LifecycleStep(
            id="redis",
            startup=redis_startup,
            shutdown=redis_shutdown,
        ),
    )

    # 3. Operation registry (separate from ExecutionRuntime)
    registry = (
        OperationRegistry(handlers={"projects.list": lambda ctx: ...})
        .patch(str_key_selector.all_keys())
        .bind_tx()
        .set_route("default")
        .finish(deep=True)
        .freeze()
    )

    # 4. Create runtime (frozen registries only)
    runtime = ExecutionRuntime(
        deps=deps_registry.freeze(),
        lifecycle=lifecycle_plan.freeze(),
    )

    # 5. Use in application
    async with runtime.scope():
        ctx = runtime.get_context()
        doc = ctx.document.query(project_spec)
        page = await doc.find_page(pagination={"limit": 10, "offset": 0})
        projects = page.hits
        # resolved = registry.resolve("projects.list", ctx)

## Troubleshooting

| Symptom | Likely cause | Fix | See also |
|---------|--------------|-----|----------|
| A lifecycle startup step did not run before a request or handler. | The runtime scope was not entered, or `startup()` was not called from the application lifespan. | Use `async with runtime.scope()` for tests and workers, or call `runtime.startup()`/`runtime.shutdown()` from the framework lifespan. | [FastAPI integration](../integrations/fastapi.md#lifecycle-step) |
| Resolving `ctx.document.query(...)`, `ctx.deps.provide(...)`, or another helper raises a missing dependency error. | The `DepsRegistry` does not include the module that registers that key/route, or the route does not match the spec name. | Add the correct integration deps module and verify the spec name, dependency key, and route are registered together. | [Specs and wiring](../concepts/specs-and-wiring.md) |
| `ExecutionRuntime` construction fails or type errors on `deps` / `lifecycle`. | An authoring `DepsRegistry` or `LifecyclePlan` was passed without `.freeze()`. | Call `.freeze()` on both plans before `ExecutionRuntime(...)`; the runtime accepts only frozen registries. | [ExecutionRuntime](#executionruntime) |
| `Cyclic dependency resolution: ...` from `Deps`. | A factory calls back into a dependency that is already on the resolution stack (same key and route). | Break the cycle in module wiring or split dependencies; do not rely on infinite recursion. | [Cycle detection](#cycle-detection) |
| `Deps.merge()` raises a key collision while building the plan. | Multiple modules provide the same dependency key and route. | Register only one provider per key/route, or separate providers with routed names before merging modules. | [Dependencies](#dependencies) |
| FastAPI attach raises about unfrozen registry. | `attach_*_endpoints` requires `FrozenOperationRegistry`. | Call `.freeze()` after `.finish(deep=True)` on the registry binder chain. | [FastAPI integration](../integrations/fastapi.md) |

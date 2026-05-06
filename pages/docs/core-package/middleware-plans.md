# Middleware & Plans

The middleware system wraps usecases with cross-cutting behavior: authorization, transaction boundaries, logging, side effects. Plans and registries provide a declarative way to compose these into per-operation chains. For the conceptual overview, see [Application Layer](../core-concepts/application-layer.md) and [Usecase Composition](../core-concepts/usecase-composition.md).

## Usecase

`Usecase[Args, R]` is the base class for all application usecases. It receives an `ExecutionContext` and implements business logic in `main()`:

    :::python
    from forze.application.execution import Usecase

    class GetProject(Usecase[UUID, ProjectRead]):
        async def main(self, args: UUID) -> ProjectRead:
            doc = self.ctx.doc_query(project_spec)
            return await doc.get(args)

Invoke a usecase via `__call__()`, which runs the full middleware chain:

    :::python
    get_project = GetProject(ctx=ctx)
    project = await get_project(some_id)

| Attribute | Type | Purpose |
|-----------|------|---------|
| `ctx` | `ExecutionContext` | Execution context for resolving ports |
| `middlewares` | `tuple[Middleware, ...]` | Middleware chain wrapping the usecase |

`with_middlewares(*mw)` returns a new usecase instance with additional middlewares appended.

## Middleware protocols

Three protocol types describe different positions in the chain:

### Guard

Runs **before** the usecase. Raises to abort the chain:

    :::python
    from forze.application.execution import Guard

    class RequireAuth(Guard[UUID]):
        async def __call__(self, args: UUID) -> None:
            if not is_authenticated():
                raise ValidationError("Not authenticated")

### Effect

Runs **after** the usecase returns. May inspect or transform the result:

    :::python
    from forze.application.execution import Effect

    class LogCreation(Effect[CreateProjectCmd, ProjectRead]):
        async def __call__(
            self, args: CreateProjectCmd, res: ProjectRead
        ) -> ProjectRead:
            logger.info("Created project %s", res.id)
            return res

### Conditional guards and effects

Run validation or post-processing only when a **synchronous** predicate holds.

Subclass **`ConditionalGuard`** or **`ConditionalEffect`** when the skip logic belongs to one type. Implement **`main`**; override **`condition`** (default is always run):

    :::python
    from forze.application.execution import ConditionalGuard

    class MaybeRequireFoo(ConditionalGuard[MyArgs]):
        def condition(self, args: MyArgs) -> bool:
            return args.mode == "strict"

        async def main(self, args: MyArgs) -> None:
            ...

Use **`WhenGuard`** / **`WhenEffect`** to wrap an existing guard or effect with a predicate at composition time (no subclass):

    :::python
    from forze.application.execution import WhenGuard, WhenEffect

    guard = WhenGuard(guard=existing, when=lambda a: a.tenant_id is not None)
    effect = WhenEffect(effect=existing, when=lambda a, r: r.is_draft)

Skipped **`WhenEffect`** calls return the incoming result unchanged.

### Middleware

Full control over the chain — receives `next` and `args`:

    :::python
    from forze.application.execution import Middleware, NextCall

    class TimingMiddleware(Middleware[Any, Any]):
        async def __call__(self, next: NextCall[Any, Any], args: Any) -> Any:
            start = time.monotonic()
            result = await next(args)
            logger.info("%.3fs", time.monotonic() - start)
            return result

`NextCall[Args, R]` is a type alias for `Callable[[Args], Awaitable[R]]`.

| Helper | Role |
|--------|------|
| `ConditionalGuard[Args]` | Abstract base: `condition` + `main`, unified `__call__` |
| `ConditionalEffect[Args, R]` | Abstract base: same pattern; skip returns `res` |
| `WhenGuard[Args]` | Wraps a `Guard`; runs inner guard when `when(args)` |
| `WhenEffect[Args, R]` | Wraps an `Effect`; runs inner effect when `when(args, res)` |

## Built-in middleware implementations

| Class | Purpose |
|-------|---------|
| `GuardMiddleware[Args, R]` | Wraps a `Guard`: runs it before `next(args)` |
| `EffectMiddleware[Args, R]` | Wraps an `Effect`: runs it after `next(args)` |
| `TxMiddleware[Args, R]` | Wraps `next` inside `ctx.transaction("default")`; supports after-commit effects |

### TxMiddleware

Wraps the usecase in a transaction. After a successful commit, runs registered after-commit effects:

    :::python
    from forze.application.execution import TxMiddleware

    tx_mw = TxMiddleware(ctx=ctx)
    tx_mw = tx_mw.with_after_commit(notify_effect, publish_effect)

After-commit effects run outside the transaction boundary, making them suitable for notifications, event publishing, and other side effects that should not roll back.

## Middleware chain execution

When a usecase is invoked, the chain builds from the middleware tuple (outermost first):

    :::text
    GuardMiddleware(auth)
      → TxMiddleware(ctx)
        → GuardMiddleware(lock)
          → EffectMiddleware(audit)
            → main(args)

Each middleware calls `next(args)` to proceed or raises to abort.

## UsecasePlan

Declares how each operation is composed with middleware. Maps operation keys to middleware buckets:

    :::python
    from forze.application.execution import UsecasePlan

    plan = (
        UsecasePlan()
        .tx("create", route="default")
        .tx("update", route="default")
        .before("create", auth_guard_factory, priority=100)
        .after("create", log_effect_factory, priority=0)
        .after_commit("create", notify_factory)
        .before("*", rate_limit_factory, priority=200)
    )

### Plan buckets

Each operation has seven middleware buckets, executed in this order:

| Bucket | When it runs | Typical use |
|--------|-------------|-------------|
| `outer_before` | Before everything | Authorization, rate limiting |
| `outer_wrap` | Wraps the entire chain | Metrics, retries, error handling |
| *Transaction boundary* | Automatic when `tx=True` | |
| `in_tx_before` | Inside tx, before usecase | Lock acquisition, pre-checks |
| `in_tx_wrap` | Inside tx, wraps usecase | In-transaction cross-cutting |
| `in_tx_after` | Inside tx, after usecase | Audit logging inside tx |
| `outer_after` | After everything | Response transformation |
| `after_commit` | After successful commit | Notifications, event publishing |

The `in_tx_*` and `after_commit` buckets only activate when `tx(op)` has been called for the operation.

### Wildcard

The `"*"` wildcard applies to all operations as a base plan. Per-operation plans extend the base:

    :::python
    plan = (
        UsecasePlan()
        .before("*", global_guard, priority=1000)   # applies to all ops
        .before("create", create_guard, priority=100) # only for create
    )

When an operation is resolved, the wildcard plan and the operation-specific plan are merged.

### Priority ordering

Within a bucket, middlewares are sorted by priority (descending). Higher priority runs first (outermost). Priority values must be unique within a bucket:

    :::python
    plan = (
        UsecasePlan()
        .before("create", rate_limit, priority=200)  # runs first
        .before("create", auth_guard, priority=100)   # runs second
    )

### Plan methods

| Method | Signature | Purpose |
|--------|-----------|---------|
| `tx(op, *, route)` | `(OpKey, route) -> Self` | Enable transaction wrapping |
| `before(op, guard, *, priority=0)` | `(OpKey, GuardFactory, priority) -> Self` | Add to `outer_before` |
| `after(op, effect, *, priority=0)` | `(OpKey, EffectFactory, priority) -> Self` | Add to `outer_after` |
| `wrap(op, mw, *, priority=0)` | `(OpKey, MiddlewareFactory, priority) -> Self` | Add to `outer_wrap` |
| `in_tx_before(op, guard, *, priority=0)` | `(OpKey, GuardFactory, priority) -> Self` | Add to `in_tx_before` |
| `in_tx_after(op, effect, *, priority=0)` | `(OpKey, EffectFactory, priority) -> Self` | Add to `in_tx_after` |
| `in_tx_wrap(op, mw, *, priority=0)` | `(OpKey, MiddlewareFactory, priority) -> Self` | Add to `in_tx_wrap` |
| `after_commit(op, effect, *, priority=0)` | `(OpKey, EffectFactory, priority) -> Self` | Add to `after_commit` |

Factory types:

| Type | Signature |
|------|-----------|
| `GuardFactory` | `(ExecutionContext) -> Guard[Any]` |
| `EffectFactory` | `(ExecutionContext) -> Effect[Any, Any]` |
| `MiddlewareFactory` | `(ExecutionContext) -> Middleware[Any, Any]` |

### Merging plans

Multiple plans can be merged for modular composition:

    :::python
    auth_plan = build_auth_plan()
    audit_plan = build_audit_plan()

    final_plan = UsecasePlan.merge(base_plan, auth_plan, audit_plan)

Per-operation buckets are concatenated. When resolved, duplicates within a bucket are deduplicated by factory identity and priority.

### Resolving a plan

`resolve()` builds a fully composed usecase for an operation:

    :::python
    usecase = plan.resolve("create", ctx, lambda ctx: CreateProject(ctx=ctx))

The method merges the wildcard and operation-specific plans, validates (e.g. in-tx buckets require tx), builds the ordered middleware chain, and wraps the factory result.

### Inspecting a plan

Use `explain()` to debug the middleware chain for an operation:

    :::python
    explanation = plan.explain("create")
    print(explanation.pretty_format())

Output includes bucket names, priorities, factory references, and factory IDs.

## UsecaseRegistry

Maps operation keys to usecase factories. A factory receives `ExecutionContext` and returns a `Usecase`:

    :::python
    from forze.application.execution import UsecaseRegistry

    registry = UsecaseRegistry()
    registry = registry.register("get", lambda ctx: GetProject(ctx=ctx))
    registry = registry.register("create", lambda ctx: CreateProject(ctx=ctx))

### Registration methods

| Method | Purpose |
|--------|---------|
| `register(op, factory, *, inplace=False)` | Register a factory; raises if `op` exists |
| `register_many(ops, *, inplace=False)` | Register multiple factories at once |
| `override(op, factory, *, inplace=False)` | Override an existing factory; raises if `op` missing |
| `override_many(ops, *, inplace=False)` | Override multiple factories |
| `exists(op)` | Check if a factory is registered |

When `inplace=True`, the registry is mutated; otherwise a new instance is returned.

### Extending with plans

    :::python
    registry.extend_plan(auth_plan, inplace=True)

`extend_plan()` merges a `UsecasePlan` into the registry's internal plan.

### Resolving usecases

    :::python
    usecase = registry.resolve("create", ctx)
    result = await usecase(CreateProjectCmd(title="New"))

`resolve()` looks up the factory, builds the middleware chain from the plan, and returns a composed usecase. Pass `debug_plan=True` to print the chain to stdout.

## OperationPlan

Internal building block for `UsecasePlan`. Each operation maps to an `OperationPlan` with per-bucket middleware specs:

    :::python
    from forze.application.execution.plan import OperationPlan, MiddlewareSpec

    op_plan = OperationPlan(tx=True)
    op_plan = op_plan.add("outer_before", MiddlewareSpec(priority=100, factory=my_factory))

| Method | Purpose |
|--------|---------|
| `add(bucket, spec)` | Add a middleware spec to a bucket |
| `build(bucket)` | Return deduplicated, priority-sorted specs for a bucket |
| `validate()` | Ensure in-tx buckets are only used when tx is enabled |
| `merge(*plans)` | Combine multiple operation plans |

You typically interact with `UsecasePlan` rather than `OperationPlan` directly.

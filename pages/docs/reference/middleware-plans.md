# Middleware & Plans

The execution system wraps handlers with stage hooks (before, on-success, transaction boundaries, after-commit, failure, finally). The public composition surface is `OperationRegistry` plus explicit `OperationPlan` steps.

For the conceptual overview, see [Application Layer](../concepts/application-layer.md) and [Operation composition](../concepts/operation-composition.md). For capability-aware ordering inside one stage, see [Capability execution](capability-execution.md).

## Handler

`Handler[Args, R]` is a protocol for application operations. Built-in handlers in `forze.application.handlers` are attrs classes that receive ports in their constructor and implement `__call__(args) -> R`:

    :::python
    from forze.application.contracts.execution import Handler
    from forze.application.handlers.document import DocumentIdDTO, GetDocument

    # Typical factory on OperationRegistry:
    get_factory = lambda ctx: GetDocument(doc=ctx.document.query(project_spec))
    # At runtime: await handler(DocumentIdDTO(id=some_uuid))

Register handler factories on `OperationRegistry` with `set_handler` or the `handlers=` constructor dict. At runtime, `FrozenOperationRegistry.resolve(operation, ctx)` builds the handler and runs the compiled stage pipeline.

## OperationRegistry

`OperationRegistry` maps operation keys to handler factories and owns per-operation `OperationPlan` instances. Author plans with `.bind(...)`, then **freeze** before HTTP or other transports resolve operations:

    :::python
    from forze.application.contracts.execution import BeforeStep
    from forze.application.execution import OperationRegistry

    def _rate_limit_factory(ctx):
        async def _before(_args) -> None:
            ...
        return _before

    registry = (
        OperationRegistry(
            handlers={
                "projects.get": lambda ctx: GetDocument(doc=ctx.document.query(project_spec)),
                "projects.create": lambda ctx: CreateProject(doc=ctx.document.command(project_spec)),
            },
        )
        .bind("projects.create")
        .bind_tx()
        .set_route("default")
        .finish(deep=False)
        .bind_outer()
        .before(BeforeStep(id="rate_limit", factory=_rate_limit_factory, priority=200))
        .finish(deep=True)
        .freeze()
    )

### Binder flow

| Method | Purpose |
|--------|---------|
| `set_handler(op, factory)` | Register or replace a handler factory |
| `bind(*ops)` | Start a plan binder for one or more operation keys |
| `bind_outer()` | Author outer-scope stages (`before`, `wrap`, `on_success`, …) |
| `bind_tx()` | Author transaction scope (`tx_before`, `after_commit`, `set_route`, …) |
| `finish(deep=False)` | Commit scope changes to the parent binder |
| `finish(deep=True)` | Commit all the way back to the registry |
| `freeze()` | Validate patches, resolved plans, dispatch graph; return `FrozenOperationRegistry` |

Built-in composition helpers (`build_document_registry`, `build_search_registry`, …) return an `OperationRegistry` with handlers pre-registered. Bind transaction routes and outer stages, then call `.freeze()` before `attach_*_endpoints`.

### Stage methods (on scope binders)

| Stage method | When it runs | Typical use |
|--------------|--------------|-------------|
| `before` | Before the handler | auth, validation, rate limiting |
| `wrap` | Around the whole chain | metrics, tracing, retries |
| `on_failure` / `finally_` | Around the outer outcome | cleanup, error hooks |
| `on_success` | After successful handler | observe result, side effects |
| `tx_before` | Inside the transaction, before the handler | locks, preconditions |
| `wrap` (on tx binder) | Around the transactional segment | tx-scoped middleware |
| `tx_on_failure` / `tx_finally` | Around the in-tx outcome | tx cleanup or failure hooks |
| `on_success` (on tx binder) | Inside the transaction, after successful handler | writes that must commit |
| `after_commit` | After successful commit | events, notifications |
| `dispatch` / `dispatch_after_commit` | Nested operation dispatch | fan-out, sagas |

Enable a transaction route with `bind_tx().set_route("default")` (or your registered tx route label) on the operations that need transactional stages.

### Step types

Stages are explicit value objects from `forze.application.contracts.execution`:

- `BeforeStep`, `OnSuccessStep` — extend `GraphStep` (`requires`, `provides`, `depends_on`, `priority`)
- `MiddlewareStep`, `OnFailureStep`, `FinallyStep`, `DispatchStep`

Each step carries an `id`, a `factory(ctx)` that returns the callable hook, and optional capability metadata (see [Capability execution](capability-execution.md)).

Example authz as a `BeforeStep` (prefer the built-in helper):

    :::python
    from forze.application.contracts.authz import AuthzSpec
    from forze.application.hooks.authz import authorize_before_step

    step = authorize_before_step(
        step_id="authz",
        spec=AuthzSpec(name="main"),
        action="projects.write",
    )

List/search hardening uses `document_scope_wrap_step` on the same plan — see [Authorization](authorization.md).

## Resolve and inspect

Resolve a composed handler through a frozen registry:

    :::python
    from forze.application.execution import make_registry_operation_resolver

    resolver = make_registry_operation_resolver(registry)
    handler = resolver("projects.create", ctx)
    result = await handler(create_dto)

`FrozenOperationRegistry.resolve(operation, ctx)` is the same entry point. At `freeze()`, the registry checks orphan patches, equal-specificity patch conflicts, transaction route requirements for tx-scoped dispatch, dispatch target existence, and acyclic dispatch graphs.

## Internal types

`OperationPlan`, scope objects (`Scope`, `TransactionScope`), and the operation runner are internal building blocks behind registry binding. Normal application code should compose through `OperationRegistry.bind(...)` and frozen resolution, not by constructing plans by hand unless extending the framework.

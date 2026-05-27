# Operation Composition

## What problem this solves

Repeated stage hooks, transaction setup, follow-up work, and operation wiring become noisy when every handler assembles them by hand.

## Core model

Forze composition has three parts:

1. `OperationRegistry`: maps operation keys to handler factories and owns per-operation `OperationPlan` instances (one of the [three execution plans](../reference/execution.md#three-execution-plans); dependency wiring and startup hooks use `DepsPlan` and `LifecyclePlan` on `ExecutionRuntime` instead).
2. `StrKeyNamespace` / `facade_op(...)`: keep facade operation names suffix-based while producing stable full keys.
3. `OperationFacade`: resolves handlers from a frozen registry through a namespace-aware facade.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/operation-registry.svg" alt="Operation registry and plan resolution">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/operation-registry.svg" alt="Operation registry and plan resolution">
</div>

## Registry

`OperationRegistry` holds handler factories and operation plans. Built-in helpers register standard document/search/storage/authn handlers; you add stages with `.bind(...)` then **freeze** before HTTP attach:

    :::python
    from forze.application.composition.document import build_document_registry

    registry = (
        build_document_registry(project_spec, project_dtos)
        .bind(project_spec.default_namespace.key("create"))
        .bind_tx()
        .set_route("default")
        .finish(deep=True)
        .freeze()
    )

For custom operations, register factories with `set_handler` and use the same bind/freeze flow (see [Middleware & Plans](../reference/middleware-plans.md)).

## Stage authoring

Author stages on scope binders returned by `.bind_outer()` and `.bind_tx()`:

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/operation-composition.svg" alt="Operation composition flow">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/operation-composition.svg" alt="Operation composition flow">
</div>

    :::python
    from forze.application.contracts.execution import BeforeStep
    from forze.application.execution import OperationRegistry

    registry = (
        OperationRegistry(
            handlers={"projects.create": lambda ctx: CreateProject(doc=ctx.document.command(spec))},
        )
        .bind("projects.create")
        .bind_tx()
        .set_route("default")
        .finish(deep=False)
        .bind_outer()
        .before(BeforeStep(id="rate_limit", factory=rate_limit_factory, priority=200))
        .before(BeforeStep(id="auth", factory=auth_factory, priority=100))
        .finish(deep=True)
        .freeze()
    )

### Result ownership

The handler `__call__` defines the domain result.

- `before` and `tx_before` only observe `args`
- `on_success`, transactional `on_success`, and `after_commit` observe `args` and `result`
- `on_failure` observes `args` and `exc`
- `finally_` and `tx_finally` observe `args` and `outcome`

Stage hooks may raise; they do not replace the handler result. Capability metadata on `GraphStep` (`requires`, `provides`, `depends_on`) orders steps within one stage — see [Capability execution](../reference/capability-execution.md).

### Stage order

| Stage | When it runs | Typical use |
|-------|--------------|-------------|
| `before` | Before the handler | auth, input checks, rate limiting |
| `wrap` | Around the whole chain | metrics, retries, logging |
| `on_failure` / `finally_` | Around the outer chain outcome | cleanup, error hooks |
| `tx_before` | Inside the transaction, before the handler | locks, preconditions |
| `on_success` (tx scope) | Inside the transaction, after successful handler | writes that must commit |
| `after_commit` | After a successful root commit | notifications, event publishing |
| `on_success` (outer) | After everything else succeeded | out-of-tx follow-up work |

Inspect merged plans on a frozen registry via internal explain helpers in tests, or trace resolution with `FrozenOperationRegistry.resolve`.

## Facades

Facades provide typed entry points over a **frozen** registry:

    :::python
    from forze.application.composition.document import DocumentFacade, build_document_registry

    registry = build_document_registry(project_spec, project_dtos).freeze()
    facade = DocumentFacade(
        ctx=ctx,
        registry=registry,
        namespace=project_spec.default_namespace,
    )
    project = await facade.create(CreateProjectCmd(title="New"))
    fetched = await facade.get(DocumentIdDTO(id=project.id))

Built-in facades define operations with `facade_op(...)` descriptors on the class; instance access resolves through `namespace`.

## Document and search composition

    :::python
    from forze.application.composition.document import DocumentKernelOp, build_document_registry

    registry = build_document_registry(project_spec, project_dtos)
    write_ops = [
        project_spec.default_namespace.key(op)
        for op in (DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE)
    ]
    registry = (
        registry.bind(*write_ops)
        .bind_tx()
        .set_route("default")
        .finish(deep=True)
        .freeze()
    )

Custom handlers plug into the same registry:

    :::python
    registry = registry.set_handler(
        project_spec.default_namespace.key("archive"),
        lambda ctx: ArchiveProject(doc=ctx.document.command(project_spec)),
        override=True,
    )

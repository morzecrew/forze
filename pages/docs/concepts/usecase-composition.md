# Usecase Composition

## What problem this solves

Repeated guards, transaction setup, follow-up hooks, and operation wiring become noisy when every usecase assembles them by hand.

## Core model

Forze composition has three parts:

1. `UsecaseRegistry`: maps operation names to usecase factories and owns stage authoring.
2. `OperationNamespace` / `facade_op(...)`: keep facade operation names suffix-based while still producing stable full keys.
3. `OperationRef`: carries already-qualified operation keys for endpoint metadata and other plain-data call sites.
4. `UsecasesFacade`: resolves a usecase from the registry through a namespace-aware facade.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/operation-registry.svg" alt="Operation registry and plan resolution">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/operation-registry.svg" alt="Operation registry and plan resolution">
</div>

## Registry

`UsecaseRegistry` is a fluent mutable builder. Register factories, add stage hooks, then finalize once:

    :::python
    from forze.application.execution import UsecaseRegistry

    registry = (
        UsecaseRegistry()
        .register("get", lambda ctx: GetProject(ctx=ctx))
        .register("create", lambda ctx: CreateProject(ctx=ctx))
        .finalize("projects")
    )

For document and search aggregates, `build_document_registry(...)` and `build_search_registry(...)` create the default operation registry for you.

## Stage authoring

`UsecaseRegistry` has two authoring paths:

- simple composition with explicit stage methods
- explicit DAG composition with `PlanDag` and stage-specific `*_dag(...)` methods

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/operation-composition.svg" alt="Operation composition flow">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/operation-composition.svg" alt="Operation composition flow">
</div>

### Simple composition

    :::python
    from forze.application.execution import UsecaseRegistry

    registry = (
        UsecaseRegistry()
        .tx("create", route="default")
        .before("*", rate_limit_guard, priority=200)
        .before("create", auth_guard, priority=100)
        .tx_before("create", lock_guard, priority=50)
        .after_commit("create", publish_event)
    )

Use `"*"` as a wildcard base plan. Operation-specific stages are merged on top when the usecase resolves.

### Result ownership

`Usecase.main()` defines the domain result.

- `before` and `tx_before` only observe `args`
- `after_success`, `tx_after_success`, and `after_commit` observe `args` and `result`
- `on_failure` observes `args` and `exc`
- `finally_` and `tx_finally` observe `args` and `outcome`

Hooks may raise, return `Skip` where capability scheduling allows it, and publish capabilities. They do not replace the usecase result.

### DAG composition

Use a DAG when one schedulable stage has multiple capability dependencies and you want the graph to stay explicit:

    :::python
    from forze.application.execution import AUTHN_PRINCIPAL, DagNode, PlanDag, UsecaseRegistry

    registry = UsecaseRegistry().before_dag(
        "create",
        PlanDag(
            nodes=(
                DagNode(
                    id="authn",
                    factory=principal_guard,
                    provides={AUTHN_PRINCIPAL},
                ),
                DagNode(
                    id="authz",
                    factory=permission_guard,
                    requires={AUTHN_PRINCIPAL},
                ),
            ),
            edges=(("authn", "authz"),),
        ),
    )

This is the only DAG authoring path. For one-off steps, use `before(...)`, `after_success(...)`, or `after_commit(...)` directly.

### Stage order

These are the user-facing stages:

| Stage | When it runs | Typical use |
|-------|--------------|-------------|
| `before` | Before everything | auth, input checks, rate limiting |
| `wrap` | Around the whole chain | metrics, retries, logging |
| `on_failure` / `finally_` | Around the outer chain outcome | cleanup, error hooks |
| `tx_before` | Inside the transaction, before the usecase | locks, preconditions |
| `tx_wrap` | Around the transactional segment | tx-scoped middleware |
| `tx_on_failure` / `tx_finally` | Around the in-tx outcome | tx-scoped cleanup or error hooks |
| `tx_after_success` | Inside the transaction, after successful `main()` | writes that must commit with the tx |
| `after_commit` | After a successful root commit | notifications, event publishing |
| `after_success` | After everything else succeeded | out-of-tx follow-up work |

`UsecaseRegistry.explain(op)` shows the merged runtime order for one operation.

## Facades

Facades provide typed entry points over a registry:

    :::python
    from forze.application.composition.document import (
        DocumentUsecasesFacade,
    )
    from forze.application.execution import operation_namespace_for

    facade = DocumentUsecasesFacade(
        ctx=ctx,
        registry=registry,
        namespace=operation_namespace_for(project_spec),
    )
    project = await facade.create(CreateProjectCmd(title="New"))
    fetched = await facade.get(DocumentIdDTO(id=project.id))

Built-in facades define their operations with `facade_op(...)` descriptors, so instance access resolves through `namespace` while class-level metadata stays explicit.

## Document and search composition

The built-in composition helpers follow the same model:

    :::python
    from forze.application.composition.document import (
        DocumentKernelOp,
        build_document_registry,
    )
    from forze.application.execution import operation_namespace_for

    registry = build_document_registry(project_spec, project_dtos)
    _ops = operation_namespace_for(project_spec)

    registry.tx(_ops.op(DocumentKernelOp.CREATE), route="default")
    registry.tx(_ops.op(DocumentKernelOp.UPDATE), route="default")
    registry.finalize("projects")

Custom operations plug into the same registry:

    :::python
    registry.register("archive", lambda ctx: ArchiveProject(ctx=ctx))
    registry.tx("archive", route="default")
    registry.before("archive", auth_guard, priority=100)

## Mental model

Start simple:

1. register operations
2. add stage methods such as `before(...)`, `tx_before(...)`, and `after_commit(...)`
3. switch to `PlanDag` only when one schedulable stage needs explicit graph structure

Everything else in the execution engine exists to support those two authoring paths, not to be the normal way you compose applications.

# Capability-driven execution

Capability metadata lets Forze order guards and success hooks inside one schedulable stage and skip downstream work when prerequisites become missing.

The public authoring model lives on `UsecaseRegistry`:

- simple stage methods such as `before(...)`, `after_success(...)`, `tx_before(...)`, `tx_after_success(...)`, and `after_commit(...)`
- explicit DAGs built with `DagNode` and `PlanDag`, then attached with `*_dag(...)`

It complements [Middleware & stages](middleware-plans.md) and [Execution](execution.md).

## Design rules

1. Capability keys are dotted strings such as `authn.principal` or `authz.permits:documents.write`.
2. If a schedulable hook returns `Skip`, every key in that step's `provides` set becomes missing in the shared capability store.
3. Within one stage, ordering is topological first and then `(priority descending, original index ascending)` inside each layer.
4. Dependency graphs are validated per stage.
5. Only one step should provide a given capability key in one stage.
6. Raised exceptions still abort execution; capability state does not mask failures.

## Simple stage annotations

These methods accept `requires`, `provides`, and `step_label`:

- `before`
- `after_success`
- `tx_before`
- `tx_after_success`
- `after_commit`

Example:

    :::python
    from forze.application.execution import AUTHN_PRINCIPAL, UsecaseRegistry

    registry = (
        UsecaseRegistry()
        .before(
            "documents.create",
            principal_guard,
            provides={AUTHN_PRINCIPAL},
            step_label="authn",
        )
        .before(
            "documents.create",
            permission_guard,
            requires={AUTHN_PRINCIPAL},
            step_label="authz",
        )
    )

## Explicit DAGs

When a single stage has a real dependency graph, model it directly with `PlanDag`:

    :::python
    from forze.application.execution import AUTHN_PRINCIPAL, DagNode, PlanDag, UsecaseRegistry

    dag = PlanDag(
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
    )

    registry = UsecaseRegistry().before_dag("documents.create", dag)

Available DAG methods:

- `before_dag`
- `after_success_dag`
- `tx_before_dag`
- `tx_after_success_dag`
- `after_commit_dag`

## Runtime behavior

Capability-aware stage middleware is built during `UsecaseRegistry.resolve(...)`.

What stays stable:

- stage-local topological scheduling
- duplicate-provider detection
- missing-provider errors during validation
- skip propagation inside capability segments and across the shared invocation store
- eager registry validation at `UsecaseRegistry.finalize(...)`

`UsecaseRegistry.explain(op)` returns rows in runtime order and shows the scheduled `priority`, `requires`, `provides`, and schedule mode for each step.

## Finalize-time validation

`UsecaseRegistry.finalize(operation_id_prefix=...)` validates capability schedules for every registered operation before the registry becomes immutable:

    :::python
    registry = (
        UsecaseRegistry()
        .register("documents.create", lambda ctx: CreateDocument(ctx=ctx))
        .before("documents.create", principal_guard, provides={"authn.principal"})
        .finalize("documents")
    )

This fails early for missing providers, duplicate providers, cycles, or invalid DAG edges.

## Execution trace

Pass `capability_execution_trace=[]` to `UsecaseRegistry.resolve(...)` in tests or diagnostics to collect `CapabilityExecutionEvent` records for capability-scheduled stages and after-commit execution.

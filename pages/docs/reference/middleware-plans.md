# Middleware & Stages

The execution system wraps usecases with guards, transaction boundaries, success hooks, and cleanup hooks. The public composition surface is `UsecaseRegistry`.

For the conceptual overview, see [Application Layer](../concepts/application-layer.md) and [Usecase Composition](../concepts/usecase-composition.md). For capability-aware ordering inside one stage, see [Capability execution](capability-execution.md).

## Usecase

`Usecase[Args, R]` is the base class for application usecases. It receives an `ExecutionContext` and implements business logic in `main()`:

    :::python
    from forze.application.execution import Usecase

    class GetProject(Usecase[UUID, ProjectRead]):
        async def main(self, args: UUID) -> ProjectRead:
            doc = self.ctx.doc_query(project_spec)
            return await doc.get(args)

Invoke a usecase via `__call__()`, which runs the compiled middleware chain.

## Stage authoring on `UsecaseRegistry`

`UsecaseRegistry` is a fluent mutable builder. Register factories, add stage hooks, and finalize once:

    :::python
    from forze.application.execution import UsecaseRegistry

    registry = (
        UsecaseRegistry()
        .register("get", lambda ctx: GetProject(ctx=ctx))
        .register("create", lambda ctx: CreateProject(ctx=ctx))
        .tx("create", route="default")
        .before("*", rate_limit_guard, priority=200)
        .before("create", auth_guard, priority=100)
        .tx_before("create", lock_guard, priority=50)
        .after_commit("create", publish_event)
        .finalize("projects")
    )

### Stage methods

| Stage method | When it runs | Typical use |
|--------------|--------------|-------------|
| `before` | Before everything | auth, validation, rate limiting |
| `wrap` | Around the whole chain | metrics, tracing, retries |
| `on_failure` / `finally_` | Around the outer outcome | cleanup, error hooks |
| `tx_before` | Inside the transaction, before the usecase | locks, preconditions |
| `tx_wrap` | Around the transactional segment | tx-scoped middleware |
| `tx_on_failure` / `tx_finally` | Around the in-tx outcome | tx cleanup or failure hooks |
| `tx_after_success` | Inside the transaction, after successful `main()` | writes that must commit |
| `after_commit` | After successful commit | events, notifications |
| `after_success` | After everything else succeeded | out-of-tx follow-up work |

`tx(op, route=...)` enables the transactional and after-commit stages for that operation.

### Wildcards

Use `"*"` as a base stage layout shared by all operations:

    :::python
    registry = (
        UsecaseRegistry()
        .before("*", global_guard, priority=1000)
        .before("create", create_guard, priority=100)
    )

At resolve time, Forze merges the wildcard layout with the operation-specific layout.

### DAG authoring

Use `PlanDag` when one schedulable stage has several explicit dependency edges:

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

Available DAG methods:

- `before_dag`
- `after_success_dag`
- `tx_before_dag`
- `tx_after_success_dag`
- `after_commit_dag`

## Resolve and inspect

Resolve a composed usecase through the registry:

    :::python
    usecase = registry.resolve("create", ctx)

Inspect the runtime order with:

    :::python
    explanation = registry.explain("create")

`finalize()` eagerly validates dispatch edges and capability schedules for every registered operation.

## Internal types

`OperationPlan`, `MiddlewareSpec`, and stage scheduling details are internal building blocks behind registry stage authoring. They remain useful for debugging and tests, but normal application code should compose through `UsecaseRegistry` stage methods and `PlanDag`.

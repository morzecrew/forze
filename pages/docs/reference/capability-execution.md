# Capability-driven execution

Capability metadata on graph steps lets Forze order hooks inside one schedulable stage and skip downstream work when prerequisites are missing.

The public authoring model uses `BeforeStep`, `OnSuccessStep`, and related types from `forze.application.contracts.execution` on `OperationRegistry` binders (`bind_outer()`, `bind_tx()`).

It complements [Middleware & Plans](middleware-plans.md) and [Execution](execution.md).

## Design rules

1. Capability keys are plain strings (often dotted), such as `"authn.principal"` or `"authz.permits:documents.write"`.
2. Steps inherit `GraphStep` fields: `requires`, `provides`, `depends_on`, and `priority`.
3. Within one stage, ordering is topological first, then `(priority descending, original index ascending)` inside each layer.
4. Dependency graphs are validated when the operation plan is built.
5. Only one step should provide a given capability key in one stage.
6. Raised exceptions still abort execution; capability state does not mask failures.

## Authoring with `BeforeStep`

Attach steps on a scope binder:

    :::python
    from forze.application.contracts.execution import BeforeStep
    from forze.application.execution import OperationRegistry

    def principal_factory(ctx):
        async def _before(_args) -> None:
            if ctx.inv_ctx.get_authn() is None:
                raise PermissionError("principal required")
        return _before

    def permission_factory(ctx):
        async def _before(_args) -> None:
            ...
        return _before

    registry = (
        OperationRegistry(handlers={"projects.create": create_factory})
        .bind("projects.create")
        .bind_outer()
        .before(
            BeforeStep(
                id="authn",
                factory=principal_factory,
                provides=("authn.principal",),
                priority=100,
            ),
            BeforeStep(
                id="authz",
                factory=permission_factory,
                requires=("authn.principal",),
                priority=50,
            ),
        )
        .finish(deep=True)
        .freeze()
    )

Use `depends_on` when ordering should follow explicit step ids in addition to capability edges.

The same `requires` / `provides` / `depends_on` fields apply to `OnSuccessStep` and transactional steps on `bind_tx()`.

## Runtime behavior

Capability-aware middleware is compiled when `FrozenOperationRegistry.resolve(operation, ctx)` runs.

What stays stable:

- stage-local topological scheduling
- duplicate-provider detection
- missing-provider errors during plan validation
- eager dispatch-graph validation at `freeze()`

## Freeze-time validation

`OperationRegistry.freeze()` validates dispatch edges and operation plans before the registry becomes immutable:

- **Dispatch graph** — targets must exist; edges must form a DAG (no cycles).
- **Orphan patches** — each `patch(selector)` must match at least one registered operation key.
- **Patch specificity** — two patches with the same selector specificity that match the same operation must merge cleanly (for example, conflicting `set_route` values are rejected).
- **Transaction route** — resolved plans with transaction stages or `dispatch` / `dispatch_after_commit` in the tx scope require `bind_tx().set_route(...)`.

    :::python
    registry = (
        OperationRegistry(handlers={...})
        .bind("projects.create")
        .bind_outer()
        .before(authn_step, authz_step)
        .finish(deep=True)
        .freeze()  # raises CoreError on invalid graphs, patches, or tx wiring
    )

Fix capability wiring at registry build time rather than at first HTTP request.

## When to use capabilities

Use capability metadata when several hooks in the **same stage** share prerequisites (authn before authz, lock before write). For a single linear guard, one `BeforeStep` with a higher `priority` is enough.

For cross-cutting HTTP policy, prefer `RequireAuthnFeature` / `RequireTenantFeature` on FastAPI endpoint specs in addition to operation-plan checks so non-HTTP callers still enforce rules in `BeforeStep` hooks.

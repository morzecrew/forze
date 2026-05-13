# Capability-driven usecase execution

This page describes the optional **capability engine** for ordering and skipping
guards and effects inside a single `UsecasePlan` bucket (`outer_before`,
`in_tx_before`, `outer_after`, `in_tx_after`, `after_commit`). It complements
[Middleware and plans](middleware-plans.md) and [Execution](execution.md).

## Design decisions (locked)

1. **Capability keys** are dotted strings (for example `authn.principal`,
   `authz.permits:documents.write`). Stable constants live in
   `forze.application.execution.capability_keys`. Keys are **not** a replacement
   for `ExecutionContext.dep(...)` ports; they label **runtime facts** produced
   by steps in the same invocation.

2. **Skip vs missing**: when a guard or effect returns an explicit **skip**
   outcome, every key in that step’s `provides` set is marked **missing** in the
   shared :class:`~forze.application.execution.capabilities.CapabilityStore`.
   Later steps (including later buckets) whose `requires` are not ready are **not
   invoked** (they are skipped, not errors).

3. **Tie-breaking**: within a bucket, the scheduler uses a directed graph from
   `requires` / `provides`, then a total order on each topological layer:
   `(priority descending, original index ascending)`.

4. **Scopes**: capability **dependency graphs** are computed **per bucket** when
   ordering steps (`schedule_capability_specs`). A key required in `in_tx_before`
   must be provided by another step in **the same** `in_tx_before` bucket for
   scheduling purposes. At **runtime**, a single :class:`~forze.application.execution.capabilities.CapabilityStore`
   is shared across all capability segments in one invocation, so keys marked
   ready or missing in an earlier segment (for example `outer_before`) are
   visible to later segments (for example `outer_after`).

5. **Effects and `R`**: only one step should **provide** a given key per bucket.
   Effects may still transform `R` sequentially; capability keys document
   side-effect facts (for example `search.indexed`), not a general merge algebra.

6. **Failure**: guards that **raise** behave as today (the usecase aborts). The
   capability store does not catch exceptions.

## Non-goals (v1)

- Replacing `wrap` / `Finally` / `OnFailure` with capability graph nodes.
- Parallel execution of independent capability branches inside a transaction.
- A generalized `RegionSpec` tree or multi-transaction composition (future work).

## Enabling the engine

Pass `use_capability_engine=True` when constructing
`UsecasePlan(use_capability_engine=True)` or call
`UsecasePlan.with_capability_engine()`. Merged plans use logical OR of the flag.

When disabled (default), behavior matches the historical flat
`GuardMiddleware` / `EffectMiddleware` chain ordered by priority only.

## Annotating steps

`UsecasePlan.before`, `in_tx_before`, `after`, `in_tx_after`, and `after_commit`
accept optional keyword arguments:

- `requires` / `provides` — iterables of `str` or :class:`~forze.application.execution.capability_keys.CapabilityKey`
  (normalized to `frozenset[str]` on the spec).
- `step_label` — optional stable label for introspection and logs.

## Pipeline helpers (`GuardStep` / `EffectStep`)

`before_pipeline`, `after_pipeline`, `in_tx_before_pipeline`, `in_tx_after_pipeline`,
`after_commit_pipeline`, `outer_pipeline`, and `in_tx_pipeline` accept each entry
as either a plain factory or a frozen :class:`~forze.application.execution.plan.GuardStep` /
:class:`~forze.application.execution.plan.EffectStep` carrying `requires` / `provides` /
`step_label` for that slot (priority still comes from `first_priority` minus the
pipeline index step).

## Introspection

`UsecasePlan.explain(op)` returns an `ExecutionPlanReport` with rows in **resolve
order** (outer segments, optional `tx` marker row, in-transaction segments,
`after_commit`, then `outer_after`). Each :class:`~forze.application.execution.plan.StepExplainRow`
includes `kind` (`guard`, `effect`, `wrap`, `finally`, `on_failure`, `tx`),
`schedule_mode` (`legacy_priority` vs `capability_topo` inside capability buckets),
`dispatch_edge_count`, and the scheduled `priority` / `requires` / `provides`.

## Registry finalize checks

:class:`~forze.application.execution.registry.UsecaseRegistry` runs the same
per-bucket scheduler used at runtime for **every registered operation** when
`UsecasePlan.use_capability_engine` is enabled, so broken graphs fail `finalize`
instead of first `resolve`. With `strict_capability_middleware_without_engine=True`,
`finalize` also rejects plans that attach capability metadata while the merged plan
keeps the engine disabled.

## Execution trace (tests and diagnostics)

`UsecasePlan.resolve(..., capability_execution_trace=[...])` appends
:class:`~forze.application.execution.capabilities.CapabilityExecutionEvent`
records for capability-segment guards/effects and `after_commit` runners when
the engine is on. Log lines for those steps prefer `step_label`, then the spec
factory’s `__qualname__`, then the guard/effect implementation type.

## Integration sketch (authn → authz)

Use :func:`forze.application.guards.authn.authn_principal_capability_guard_factory`
together with `before(..., provides={AUTHN_PRINCIPAL})`, then an authz guard whose
`requires` include the keys returned by
:func:`~forze.application.guards.authz.authz_permission_capability_keys` — see
the authz guard factory and the FastAPI recipe.

## Troubleshooting (`CoreError` from the scheduler)

| Message | Meaning |
|---------|---------|
| capability … **more than one step** | Two steps in the same bucket both `provide` the same key after merge — dedupe or split keys. |
| capability … **no step in this bucket provides it** | A `requires` key has no matching `provides` in **that bucket’s** scheduled graph (for ordering). You may still seed readiness from an earlier segment at runtime if the key is optional. |
| dependency graph … **cycle** | Circular `requires` / `provides` edges — break the cycle or merge steps. |

## Migration notes

- Start with **empty** `requires` / `provides` (defaults) for existing code;
  ordering stays priority-only.
- For authz, use
  `authz_permission_capability_keys(requirement)` together with
  `before(..., requires=..., provides=...)` and ensure an earlier guard in the
  same bucket **provides** `authn.principal` when you require authenticated
  principals.

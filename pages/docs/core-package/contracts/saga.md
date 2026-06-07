# Saga contracts

A **saga** coordinates a multi-step business process across aggregates (or services) where the
steps **can't** share one transaction. Each step commits independently; if a later step fails, the
saga **compensates** the already-committed steps in reverse — semantic rollback, not database
rollback.

## Defining a saga

A saga is a `SagaDefinition` of typed `SagaStep`s, each with an `action` and an optional
`compensation`, threading a working **context** through:

    :::python
    from forze.application.contracts.saga import SagaDefinition, SagaStep
    from forze.application.execution import run_saga

    async def reserve(ctx, s):   # action: returns the updated context
        s = s.with_reservation(await reserve_stock(ctx, s.order_id))
        return s

    async def unreserve(ctx, s): # compensation: undoes a committed step
        await release_stock(ctx, s.reservation_id)

    saga = SagaDefinition(
        name="checkout",
        steps=(
            SagaStep(name="reserve", action=reserve, compensation=unreserve, tx_route="pg"),
            SagaStep(name="charge",  action=charge,  compensation=refund,    tx_route="pg",
                     retry_policy="transient"),
            SagaStep(name="confirm", action=confirm, tx_route="pg"),
        ),
    )

    result = await run_saga(ctx, saga, CheckoutContext(order_id=order_id))

| Field | Purpose |
|-------|---------|
| `action` | Do the step's work; return the updated saga context. |
| `compensation` | Undo a committed step (receives the context *as of that step's completion*). |
| `kind` | The step's role relative to the pivot (see below); defaults to `COMPENSATABLE`. |
| `tx_route` | Commit this step's work in its own transaction on this route. |
| `retry_policy` | Named resilience policy retried around the action (fresh tx per attempt; for a `RETRYABLE` step it drives retry-forward). Use a policy with a `TimeoutStrategy` for a per-step timeout. |
| `compensation_policy` | Named resilience policy retried around the compensation (it must eventually succeed). |
| `idempotent` | Affirm the action is safe to re-execute. **Required** (validated) when the step is retried — i.e. declares `retry_policy` or is `RETRYABLE` — so opting into re-execution is a conscious decision. |

## The pivot — step kinds

Real processes have a **point of no return**. `reserve stock → charge card → ship` — if shipping
fails you must *not* refund and un-reserve; the saga *committed* at the charge, so you retry shipping.
`SagaStepKind` captures this; steps must be ordered `compensatable* pivot? retryable*` (validated):

| Kind | Meaning |
|------|---------|
| `COMPENSATABLE` (default) | Undoable; compensated in reverse if the saga fails **before** the pivot commits. |
| `PIVOT` | The go/no-go commit point. Once it succeeds, the saga is committed. |
| `RETRYABLE` | Follows the pivot; retried *forward* to completion, **never** compensated. |

## Semantics

- **One transaction per step.** Each step with a `tx_route` runs in its own `ctx.tx_ctx.scope` and
  commits independently. A saga therefore **must run outside an enclosing transaction** — starting
  one inside an open transaction raises (each step needs its own commit, not a savepoint).
- **Failure before the pivot → compensate.** The completed compensatable steps are compensated in
  **reverse order** (each in its own transaction, retried under `compensation_policy`), then raise:
  - all compensations succeed → `saga.step_failed` (`DOMAIN`) — the saga is **consistent**.
  - a compensation itself fails → `saga.compensation_failed` (`INFRASTRUCTURE`, carrying the
    originals) — **inconsistent; manual intervention required** (never silently swallowed).
- **Failure after the pivot → forward-incomplete.** The saga is committed, so it is **not**
  compensated; a retryable step that exhausts its `retry_policy` raises `saga.forward_incomplete`
  (`INFRASTRUCTURE`) — the saga must be *completed* (manually or asynchronously), not rolled back.
  (True retry-forever needs the durable adapter; in-process, the policy bounds the attempts.)
- **Composes with what you have.** A step that persists an aggregate dispatches its
  [`@event_emitter`](domain.md) domain events in-transaction (within that step's scope); a step's
  `retry_policy` reuses the [resilience](resilience.md) executor (a fresh transaction per attempt).
- **Tracing.** The executor emits `domain="saga"` events (`saga_started`, `step_completed`,
  `step_failed`, `compensated`, `compensation_failed`).

## Executor

`run_saga(ctx, definition, initial)` runs the saga via the resolved `SagaExecutorPort` — the
in-process `InProcessSagaExecutor` by default (no registration needed).

## Durability — one brain, two drivers

The pivot/compensation **decision logic** lives in a backend-agnostic coordinator,
`SagaProgress` (pure, ctx-free: it tracks the completed steps and the pivot, decides
compensate-vs-forward, and builds the `saga.*` errors). A *driver* supplies the I/O. This keeps the
semantics identical across execution backends instead of forking into two engines:

- **In-process** (`InProcessSagaExecutor`) drives `SagaProgress` with the callable steps +
  `ctx.tx_ctx.scope` transactions. Synchronous; **not crash-resumable** — a process crash mid-saga
  leaves committed steps un-compensated.
- **Temporal** (`forze_temporal.TemporalSaga`) drives the *same* `SagaProgress` from inside a
  workflow, with steps as activities. **Temporal owns durability** — persistence, resume, retries,
  and timeouts (per-activity `RetryPolicy`/timeouts and the workflow history); Forze contributes only
  the saga semantics. Use it inside `@workflow.run`:

      :::python
      saga = TemporalSaga(name="checkout")
      await saga.step("reserve", lambda: workflow.execute_activity(reserve, inp, ...),
                      compensation=lambda: workflow.execute_activity(unreserve, inp, ...))
      await saga.step("charge", lambda: workflow.execute_activity(charge, inp, ...),
                      kind=SagaStepKind.PIVOT)
      await saga.step("ship", lambda: workflow.execute_activity(ship, inp, ...),
                      kind=SagaStepKind.RETRYABLE)

The shared asset is the **coordinator**, not the definition: a Temporal saga's steps are
**activities** (registered, Pydantic-serializable I/O, deterministic workflow code), not the
callable `SagaDefinition`. That divergence is intrinsic to Temporal's model, so the Temporal path is
a workflow helper (started via `DurableWorkflowCommandPort`), not a `SagaExecutorPort`. An in-core
durable store was deliberately **not** built — it would reinvent what Temporal already does.

## Limits of the in-process driver

Two things are **durability features** — reach for the Temporal driver when you need them:

- **`forward_incomplete` is terminal in-process.** A retryable step that exhausts its `retry_policy`
  raises; there is no in-process mechanism to resume it forward later. True retry-to-completion needs
  the durable workflow (Temporal keeps retrying per the activity `RetryPolicy`).
- **No in-flight visibility in-process.** You get `domain="saga"` tracer events
  (`saga_started`/`step_completed`/`saga_committed`/`compensated`/`saga_completed` …), but no queryable
  registry of running sagas — Temporal's history/UI is the answer for operating long-running sagas.

**Deferred:** parallel/fan-out step branches (which change the strictly-sequential model) and a
saga-level deadline (which needs careful mid-step cancellation) are out of scope for now.

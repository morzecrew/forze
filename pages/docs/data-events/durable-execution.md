---
title: Durable execution
icon: lucide/workflow
summary: Crash-resumable workflows, schedules, and functions on an external engine — orchestration that outlives a process
---

Some work outlives the request that starts it: a multi-step fulfilment that runs for days,
retries a flaky payment, waits on a human approval, and must survive a deploy or a crash in
the middle. In-process [sagas](events-sagas.md) coordinate steps *within* one process;
**durable execution** runs the orchestration against a store that persists every step — so a
crash resumes exactly where it left off, not from the top. That store can be an external
engine ([Temporal](../integrations/temporal.md) / [Inngest](../integrations/inngest.md)) or
the [self-hosted tier](#self-hosted-on-your-own-database) on the database you already
operate — Postgres or MongoDB.

## The mental model: journaled progress

A durable workflow is ordinary code whose progress the engine **journals**. Each step's
result is recorded, so after a crash the engine replays the workflow and skips the steps
already done — the slow external calls, the timers, the waits resume rather than repeat. You
write the orchestration; the engine owns the durability, retries, and timers. That's the
difference from a queue task, which runs once with basic redelivery and no memory of where
it was.

## Three forms

- **Workflows** — multi-step, long-running, and *observable*: a `start` returns a handle
  immediately, and a query port reads coarse status, the typed result, or in-flight state.
  Signals and updates push messages into a running workflow.
- **Schedules** — fire a workflow on a cron or interval; the durable counterpart to a
  queue's delayed jobs.
- **Functions** — event-triggered work composed of memoized **steps** (the Inngest model;
  Inngest retries each step; the self-hosted tier runs a step once and can retry the run).

A workflow start returns a handle you observe through the query port:

```python
handle = await workflows.start(FulfilOrder(order_id=order_id), workflow_id=f"fulfil-{order_id}")
run = await queries.describe(handle)          # coarse status: RUNNING / COMPLETED / FAILED / …
result = await queries.result(handle)         # the typed return value, once complete
```

A stable `workflow_id` makes `start` idempotent — the same id won't launch a second run.

## Self-hosted on your own database

The external engines are *operational* dependencies — a Temporal cluster or the Inngest
service. For the common deployment that runs one database and no engine, the self-hosted
tier gives you the **functions** form (memoized steps + crash recovery) and crash-resumable
**sagas** on that database, with nothing to stand up. **Postgres and MongoDB both implement
it**, behind the same ports and the same runner.

It reuses the journaled-progress model, backed by two app-provided relations: a
`durable_step` **memo journal** (each step's result recorded so a replay skips it) and a
`durable_run` **run store** (run instances, claimed for recovery — under
`FOR UPDATE SKIP LOCKED` on Postgres, under a token-stamped batch claim on Mongo, which has
no row locks to take). Wire both on your database's module and drive them with the
`forze_kits` runner:

```python
deps = PostgresDepsModule(
    client=client,
    durable_step=PostgresDurableStepConfig(relation=("public", "durable_step")),
    durable_run=PostgresDurableRunConfig(relation=("public", "durable_run")),
)

registry = DurableFunctionRegistry()
registry.register("fulfil-order", fulfil_order)      # async (ctx, input) -> output
runner = DurableFunctionRunner(registry=registry)

await runner.enqueue(ctx, "fulfil-order", {"order_id": str(order_id)})
```

On MongoDB the wiring is the same shape, over collections instead of tables:

```python
from forze_mongo.execution.deps import MongoDurableRunConfig, MongoDurableStepConfig

deps = MongoDepsModule(
    client=client,
    durable_step=MongoDurableStepConfig(collection=("app", "durable_step")),
    durable_run=MongoDurableRunConfig(collection=("app", "durable_run")),
)
```

The run collection needs one index — a partial unique index on `idempotency_key` — or two
simultaneous submits of one key can both insert. See the
[Durable execution schema](../reference/durable-schema.md#mongo) for it and the
recommended ones.

A registered function does its work in **steps** via the step port; each step memoizes, so a
re-invocation after a crash replays completed steps and resumes at the first incomplete one:

```python
async def fulfil_order(ctx, input):
    step = resolve_durable_step(ctx)
    charge = await step.run("charge", lambda: charge_card(ctx, input))   # journaled once
    await step.run("ship", lambda: ship(ctx, charge))
    return {"shipped": True}
```

A background scanner re-claims runs abandoned by a crash and re-invokes them —
`durable_recovery_background_lifecycle_step(runner=runner)`. It is **multi-worker-safe**:
concurrent scanners never claim the same run (`FOR UPDATE SKIP LOCKED`) and a terminal
write is **fenced** against a reclaimed lease, so a stalled worker whose lease expired can't
finish a run the new owner already took over. A still-executing run heartbeats its lease
alive, so a long body is never reclaimed mid-flight — bounded by the runner's
`max_run_duration` (default 1 hour): past the cap the body is cancelled while the lease is
still held (nothing double-executes) and the run lands `TIMED_OUT` with the deadline reason,
so a body hung on a dead peer can't pin a recovery slot forever. `TIMED_OUT` is its own
terminal rather than a flavour of `FAILED` because the two send you to different places —
`FAILED` to the body's code, `TIMED_OUT` to the cap, the workload's size, or a hung peer. Set
it above your longest body, or `None` to remove the cap. A timeout is not retried: requeue
the run (below) to run it again. Run the scanner unwrapped on every replica. The singleton
lifecycle guard cannot elect one scanner: it holds its lock only while startup launches the
loop, so later replicas scan too, and one that starts while another holds the lock runs none.
`max_concurrency` bounds how many runs a sweep recovers at once. Enqueue with
`run_at=<when>` for a **delayed** run — the scan skips it until it's due.

To **coalesce** "recompute after a change", enqueue with a `coalesce_key`: a submit joins the
run waiting unclaimed under that key, keeping its input and its `run_at`, and records a new
run once that one is claimed. A burst of changes then causes one run, and a change that lands
after the run started causes another, which reads it. A run waiting for a retry, or requeued,
is not joined. The join is atomic at the store — a partial unique index — and needs the
column and index in the [schema](../reference/durable-schema.md); it cannot be combined with
an `idempotency_key`.

```python
await runner.enqueue(
    ctx, "reindex-catalog", {"catalog_id": str(catalog_id)},
    coalesce_key=f"catalog:{catalog_id}",
    run_at=utcnow() + timedelta(seconds=30),
)
```

**Multi-tenant.** The stores resolve their table under the bound tenant. On a **tagged**
shared table (a `tenant_id` column), an unbound scanner recovers every tenant's runs and the
runner re-binds each run's tenant to execute it. On a **namespace** store (a per-tenant
`relation` resolver, one table per tenant schema) pass `tenants=…` to the lifecycle step: each
sweep binds every assigned tenant in turn and recovers its table — shard the tenant set across
instances to parallelize.

Each operation resolves **one** tenant and uses it for everything it decides — the table, the
row's tag and the scoped id together. `enqueue(tenant_id=…)` and a schedule record carrying a
`tenant_id` are therefore honoured in full, including which table the row lands in, so a
control plane bound to nothing can register work for a tenant and that tenant's scanner will
find it. Naming a tenant that **contradicts** a bound one is refused
(`authentication` / `tenant_mismatch`) rather than applied to some of the three.

What a bound caller reaches splits by what the verb does. The verbs taking a run id —
`begin`, `renew`, `load`, `mark_retry` and every terminal write — reach that tenant's runs and **untagged**
ones; the untagged arm is what keeps a run belonging to no tenant completable, since a
terminal write that matched nothing would leave it reclaimed and re-run forever. The verbs
that enumerate or control — the recovery scan, `list_runs`, `request_cancel`, `refuse_cancel`
— match the tenant exactly. Unbound reaches everything, which is what lets one sweep serve
every tenant.

Declaring `required_tenant_isolation` covers the durable routes: a `durable_step`,
`durable_run` or `durable_schedule` wired below the declared floor now fails at wiring rather
than passing on the strength of the other planes. Note the coupling that creates —
`tenant_aware=True` on a run store fails closed when nothing is bound, which is how the
cross-tenant sweep runs, so a deployment declaring `tagged` runs per-tenant recovery
(`tenants=…`) instead.

### Retrying a failed run

A step that raises writes no journal row, and by default the run lands `FAILED`. Give the
runner a retry strategy and a run whose body raises a retryable error goes back to `PENDING`
instead, due after a backoff:

```python
from datetime import timedelta

from forze.application.contracts.resilience import BackoffStrategy, RetryStrategy
from forze.base.exceptions import ExceptionKind

runner = DurableFunctionRunner(
    registry=registry,
    retry=RetryStrategy(
        max_attempts=5,
        backoff=BackoffStrategy(base=timedelta(seconds=10), max=timedelta(minutes=10)),
        retry_on=frozenset({ExceptionKind.INFRASTRUCTURE, ExceptionKind.THROTTLED}),
    ),
)
```

The worker is freed while the run waits; the recovery scan claims it once it is due, on any
replica, and the steps it journaled replay — only the failed step and those after it run
again. `registry.register(name, body, retry=...)` gives one function its own strategy, and
`max_attempts=1` there opts it out of the runner's.

- **What is retried.** A `CoreException` whose kind is in `retry_on`. A vendor exception is
  not: map it to a kind with an exception interceptor. These land as they would have: a
  timeout; a failure while an operator's cancel is pending; a saga that rolled back
  (`domain` — it journals its failed step, so running it again fails the same way); and a
  saga whose rollback failed or whose step's outcome is unknown, which need an operator.
- **The budget is the run's `attempts`.** Every claim spends one, a recovery claim after a
  crash included, so `max_attempts` bounds how often the run is claimed, not how often its
  body raised. The strategy's `budget` is refused: a process-local bucket cannot count claims
  spread across replicas.
- **While it waits** the run is `PENDING` with the failure in `error`. `run_now` returns that
  record instead of raising, and so does a re-submit under its key until it is due. A cancel
  asked of a waiting run is stamped and lands when the retry comes due, without running the
  body (a saga runs again and decides by its pivot); that claim spends an attempt.
- **When it is due** it keeps its `created_at`, so the scan, oldest first, claims it ahead of
  fresh runs. A `throttled` or `unavailable` failure with a `retry_after` (an upstream `429`'s
  `Retry-After`) or a `retry_at` is due no sooner than that, even past the backoff's `max`.
  A hint too far off to schedule — infinite, or past what a `datetime` holds — means never:
  the run lands `FAILED` at once, where an alert sees it and `requeue_failed` can bring it
  back.

An attempt that sends its run back counts under the `retrying` outcome, and the last one
under `failed`: alert on `failed`.

**Requeue a failed run** once whatever failed it is fixed, naming the runs or the function.
A run that landed `FAILED` or `TIMED_OUT` goes back to `PENDING` under its own id, so it too
replays its journal and reruns only the steps it did not finish:

```python
from forze_kits.integrations.durable import resolve_durable_run_admin


async def requeue(ctx):
    admin = resolve_durable_run_admin(ctx)
    await admin.requeue_failed(run_ids=["..."])     # or name="profile"
```

A call naming neither is refused: a run's failure history is not something to bring back
whole. It is scoped like `list_runs`, though an unbound call may name a function across
tenants, and it leaves alone `CANCELLED` and `FORWARD_INCOMPLETE` runs and a failed run whose
cancel ask is unanswered. A requeued run keeps its `attempts`, so one that spent a retry
strategy's budget gets one attempt per requeue, and its `created_at`, so the scan claims
requeued runs ahead of fresh ones.

A saga journals a failed forward step, so requeueing one that rolled back
(`saga.step_failed`) fails it the same way again; that work needs a new run. Requeueing one
whose rollback failed runs again only the compensations that failed.

### Runs recovery cannot decrypt

With payloads encrypted, a run whose tenant's key is gone (an offboarded tenant, a key
pending deletion) cannot be opened when the recovery scan claims it. The scan skips it rather
than failing the batch, and holds it out of later scans for two leases, doubling with each
claim up to a day. A throttled or briefly unreachable KMS is not a lost key: that run keeps
its ordinary lease and is retried.

If the key stays gone, the run is **quarantined** on the claim whose hold would reach the day
— the ninth at the default 5-minute lease, and never the first. The count is the run's
attempts, crash reclaims included, so a run already claimed nine times is quarantined on its
first failed open. `QUARANTINED` is not final, but the scan never claims it, so however many
pile up they cannot crowd the runs behind them. Unlike a held run, a quarantined one does not
come back by itself when its key does: it waits for an operator.

Once a tenant's key is back, bind that tenant, find its quarantined runs (listed with their
payloads unread) and return them to the scan:

```python
from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.tenancy import TenantIdentity
from forze_kits.integrations.durable import resolve_durable_run_admin

with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_id)):
    admin = resolve_durable_run_admin(ctx)
    page = await admin.list_runs(status=DurableRunStatus.QUARANTINED)
    moved = await admin.requeue_quarantined()          # back to PENDING, due now
```

Or name the runs: `requeue_quarantined(run_ids=[...])`. A requeued run keeps its creation
time, so the scan takes it ahead of every fresh run, and keeps its attempt count, so one whose
key is still gone costs a claim and a KMS call and is quarantined again at once. That is why
an unbound requeue must name its runs whenever a quarantined run belongs to a tenant: it is
refused (`validation` / `durable.requeue_unscoped`) rather than bring back every tenant's,
those gone for good among them. A deployment whose runs carry no tenant requeues unbound.

A cancel asked of a quarantined or requeued run is stamped and waits for a claim that can read
the run, since it may be a saga past its pivot. A quiesce counts quarantined runs as unsettled.

Roll this out to every node before any run can be quarantined: a node running an earlier
release cannot read the status, and fails a listing or an idempotent enqueue that meets such a
run. Requeue quarantined runs before rolling back, or they are left where no scan claims them.

### Stopping a run

A "Stop" button on a self-hosted run goes through `request_cancel`:

```python
await runner.request_cancel(ctx, run_id)      # True if the ask was recorded
```

**It is cooperative, and the framework will not pretend otherwise.** There is no in-process
red button in Python: a thread can't be killed, asyncio cancellation lands only at await
points, and a body blocked inside a C extension notices nothing until it returns. So this
*requests* a stop. A `PENDING` run lands `CANCELLED` at once — nothing has executed (one
requeued from quarantine, above, is only stamped). A
`RUNNING` one keeps going until its lease holder sees the stamp on the **next heartbeat**
(`lease_for / heartbeat_divisor`, 100 s at defaults — tune those two if you need tighter),
at which point the body is cancelled at its next await and the run lands `CANCELLED`. A body
that never awaits is bounded only by `max_run_duration`; if you need bounded-latency stop,
structure the body for it (await regularly, `run_cpu` with checkpoints at chunk boundaries).
True hard kill exists only at process/container granularity and is a deployment concern.

`CANCELLED` is deliberately not `FAILED`: nothing is wrong with the code, so it should not
page anyone. Its completed steps stay journaled — cancelling is not a rollback — and
`requeue_failed` leaves it alone.

The ask is **unfenced** and the landing is **fenced**: anyone may ask, but only the run's
current claim holder may write the terminal state, so a stalled replica can't cancel a run
its new owner is midway through. If the holder dies with the stamp down, the recovery scan
claims the run and lands it *without invoking the body* — unless it is a saga, below.

**Cancelling a saga** is decided by the pivot. Before it, compensations run (journaled, as
always) and the run lands `CANCELLED` — stopping mid-flight is safe precisely because
compensation exists. At or after it, the request is **refused** and the saga completes
forward: honouring it there would manufacture a `FORWARD_INCOMPLETE` by operator request.
The refusal is recorded on the run (`cancel_refused_at`) so the operator who pressed Stop and
watched it finish anyway gets the reason. A saga whose holder died before seeing the request is
run again by recovery rather than landed: only its journal says whether it passed its pivot,
so it decides at the first step it would act, exactly as a live saga does. With no step left
to act it completes.

A run **paused waiting on user input** is a different case: its durable run already completed
cleanly (it succeeded at producing the question), so there is nothing to `request_cancel`.
Cancelling there is a domain action on your own task record, not a run-control call.

Engine-backed tiers advertise whether they can do any of this
(`durable_run_control_capabilities(port).supports_cancel`); `runner.request_cancel` fails
closed against one that can't, rather than accepting a request it would silently drop.

**Observability.** Pass `DurableTelemetry.create()` to the runner and scheduler for
OpenTelemetry: a `durable.run` span per execution plus `forze.durable.runs` /
`forze.durable.run.duration` (by name + outcome — `completed` / `failed` /
`forward_incomplete` / `cancelled` / `timed_out` / `reclaimed` / `interrupted` (the body never
finished — a drain or shutdown) / `unrecorded` (the
body finished but its terminal write went unacknowledged — the row may already be terminal or may
still await recovery, and the worker cannot tell),
`forze.durable.recovered`, and
`forze.durable.schedule.fires` metrics. Emits via the global OTel providers — configure the
SDK in your app.

The step journal is exactly-once for a step's **recorded result** — a completed step replays
from the journal instead of re-running. It is **not** exactly-once for arbitrary side
effects: a body can run more than once if a worker is reclaimed (its run lease expired
mid-body) or crashes before the result is journaled — the same at-least-once step guarantee
as Temporal / DBOS / Inngest. **Keep step bodies idempotent** (use an idempotency key on
external calls) for exactly-once effects. The replay guarantee is checked by
[deterministic simulation](../dst/overview.md): a seeded crash fault kills a run mid-step, the
recovery scanner re-invokes it, and the oracle asserts every completed step replays from its
journal instead of re-executing. Keep durable bodies deterministic too — read time / ids
through `utcnow` / `uuid7` and do work in steps — and the simulator explores the crash-point
space for you.

### Keeping the run table bounded

Finished runs are kept until you delete them, with their input, output and step journal. A
retention step deletes them past a window:

```python
lifecycle = [
    durable_run_retention_lifecycle_step(older_than=timedelta(days=30)),
    durable_recovery_background_lifecycle_step(runner=runner),
]
```

Each sweep calls `DurableRunAdminPort.purge_finished(older_than, statuses=..., name=None,
limit=1000)` until a batch comes short, at most `max_batches_per_tick` times, then sleeps
`interval`. It deletes only `completed`, `cancelled`, `failed` and `timed_out` runs created
more than `older_than` ago, oldest first, each with its step journal; `quarantined` and
`forward_incomplete` runs still owe work and cannot be named. The step needs the admin port
(`admin=True` on the run config). Purging a run frees its idempotency key: a later submit
under that key starts a new run.

On Postgres the batch, its rows and its journal go in one statement that skips any row
another transaction holds, so a run being requeued keeps its steps. Concurrent sweepers skip
each other's rows, so run the step on every replica.

### Crash-resumable sagas

The self-hosted tier closes the "an in-process saga is not crash-resumable" gap. Swap the
saga executor for the durable one and run the saga as a durable function — each step **and
each compensation** is journaled, so a crash mid-saga (or mid-rollback) resumes instead of
leaving committed steps un-compensated:

```python
deps = SagaDepsModule(executor=DurableSagaExecutor())      # swap the seam
registry.register(str(saga.name), durable_saga_handler(saga, OrderCtx))
await runner.run_now(ctx, str(saga.name), initial.model_dump(mode="json"))
```

The saga context must be a serializable `pydantic.BaseModel` (it is journaled between
steps). A step failure classified retryable (infrastructure, throttled, concurrency) is
retried in place with backoff before it is journaled — compensation only runs on genuine
failures or exhausted retries, never a one-off blip. A throttled failure waits at least its
`retry_after`; one longer than the schedule's last backoff is journaled at once. This tier is self-hosted only
(Postgres or MongoDB); a full workflow engine (timers, signals, versioning) is still
Temporal/Inngest. The relations come from your migrations; their schema, indexes and
migrations — and the one index the Mongo run store needs — are in the
[Durable execution schema](../reference/durable-schema.md).

### Recurring schedules

A `durable_schedule` table + the `DurableScheduler` fire a run on a **cron cadence**. Put a
schedule and run the scheduler step (alongside the recovery step, which executes the runs it
enqueues):

```python
deps = PostgresDepsModule(client=client, durable_run=..., durable_schedule=...)
# ...or MongoDepsModule(client=client, durable_run=..., durable_schedule=...)

await scheduler.put(ctx, "nightly-report", "report", "0 3 * * *", tz="Europe/Berlin")

lifecycle = [
    durable_scheduler_background_lifecycle_step(scheduler=scheduler),
    durable_recovery_background_lifecycle_step(runner=runner),
]
```

`await scheduler.remove(ctx, "nightly-report")` unregisters a schedule so it never fires
again — distinct from pausing it (`put` it back with `enabled=False`, which keeps the row).

Or **declare the cadence on the function** and let it wire itself: a `DurableFunctionSpec`
with a `DurableFunctionCronTrigger` auto-registers its schedule when you pass the specs to
the step — no manual `put`:

```python
spec = DurableFunctionSpec(
    name="report",
    run=DurableFunctionInvokeSpec(args_type=ReportArgs),
    triggers=(
        DurableFunctionCronTrigger(
            expression="0 3 * * *",
            tz="Europe/Berlin",
            input=ReportArgs(kind="daily"),
        ),
    ),
)
durable_scheduler_background_lifecycle_step(scheduler=scheduler, specs=[spec])
```

`tz` names the IANA zone the expression runs in (UTC when unset). `input` is what each fire
hands the run: a dict or an instance of the spec's `args_type`, checked against it when the
spec is declared. On Inngest, `tz` maps to its `TZ=` prefix, and an `input` is refused,
since an Inngest cron run receives no data.

Auto-registration is **idempotent** — a restart re-uses an unchanged schedule (so it never
resets `next_fire_at` and skips a due fire) and re-registers only when the cron or timezone
changes. A changed `input` alone replaces the stored input and keeps the next fire.

Firing is **fire-once / skip-missed**: if the scheduler was down across several occurrences
it fires once and advances to the next future one (no backfill). It's exactly-once across
replicas — each fire enqueues a run keyed `{schedule_id}:{fire_epoch}` and the next fire is a
compare-and-set — so, like recovery, it's safe to run on every node. "Now" comes from the
`TimeSource` seam, so schedules are deterministic under simulation. Recurring **schedules**
are self-hosted here; a full engine's timers/signals/versioning are still Temporal/Inngest.

## When to reach for it

| You need | Use |
| --- | --- |
| Multi-step work that must survive crashes, with status / retries / timers | **durable execution** |
| A single fire-and-forget task | a [queue](../reference/contracts/messaging.md) |
| Step coordination *within* one process or transaction | a [saga](events-sagas.md) |

To start a workflow **reliably** from a request — only if the write commits — stage it
through the [outbox](events-sagas.md) instead of starting it directly.

The ports and dep keys are the [durable reference](../reference/contracts/durable.md); the
worked flows are the [background work](../recipes/background-workflow.md) and
[scheduled jobs](../recipes/scheduled-queue-jobs.md) recipes.

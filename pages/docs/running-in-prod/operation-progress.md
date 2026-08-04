---
title: Operation progress
icon: lucide/loader
summary: Give long-running work an observable shape — a job record, one progress event, a reporter, and the merge rules that survive out-of-order delivery
---

Anything that runs longer than a request eventually gets asked "how far along is
it?" — an export walking millions of rows, a re-encryption sweep, an index
rebuild, an agent task waiting on a human. Without somewhere to put the answer,
every caller invents the same three things: a status table, an event, and a set
of merge rules. The merge rules are the part that goes wrong quietly, because
progress reports arrive out of order and a naive projection shows a bar jumping
backwards, or a job that failed and then came back to life.

The progress plane is the smallest thing that removes that work. It **observes**
and nothing more: it does not schedule, retry, cancel, or aggregate. What it owns
is one read model, one declared realtime event, a reporter with the throttling
built in, and a projector that holds the merge rules so nobody writes them twice.

## The shape

Four pieces, and each one has exactly one job:

| Piece | What it is |
|---|---|
| `job_record_spec()` | The document collection — one `JobRecord` per job |
| `JOB_PROGRESS_EVENT` | The one declared realtime event (`job.progress`) |
| `ProgressReporter` | The write side the work holds — coalesces and clamps |
| `JobProgressProjector` | The read side — merges events into the record |

A **job** is task-grained, not run-grained: its id is its own, and a durable run
is a link it carries. That is deliberate — one logical task can span many runs,
and the record is what stays continuous across them.

## Reporting from the work

The work holds a reporter and calls it as it advances. `track()` owns the
lifecycle so the job cannot be left running forever by an exception on its way
out:

```python
from forze_kits.integrations.progress import (
    build_job_progress_projector,
    build_progress_reporter,
    progress_outbox_spec,
)

reporter = build_progress_reporter(
    ctx,
    job_id=job_id,
    kind="export",                                   # your vocabulary, not ours
    subject=str(destination),
    projector=build_job_progress_projector(ctx),     # write the record inline
    stream_spec=transport.stream_spec,               # and push it live
    outbox_spec=progress_outbox_spec(),              # transitions, on their own route
)

async with reporter.track("exporting"):
    await export_archive(runtime, destination, scope=scope, progress=reporter)
```

`start()` / `wait()` / `finish()` / `fail()` are the transitions; `report()`,
`advance(done, total)` and `heartbeat()` are the ticks. `advance(0, 0)` is
indeterminate rather than an error — "0 of 0" is what a sweep legitimately knows
before it has counted.

Report from **outside** the unit of work. The inline projector writes through
your ports, so a `fail()` recorded inside a transaction that then rolls back
takes the failure record with it and the job stays "running" forever. Long
sweeps are not one transaction, which is why inline is the default; work that
genuinely is transactional should publish and let a consumer-side projector own
the record.

### Coalescing is the answer to event rate

A row-by-row loop reports thousands of times a second. The reporter keeps at most
one report per window (300 ms by default) and holds the newest value rather than
dropping it — the window bounds how *often* the transport hears from a job, never
which value it ends on. The held value is flushed by the next eligible report, by
any status transition, by reaching `1.0`, and by `flush()`.

So `report(1.0)` as the last thing a job ever says is never swallowed, and a
hundred reports in a burst cost one message. Set `min_interval=0` to emit every
report.

### The bar only moves forward

A resumed worker re-reports counters it already reported. The reporter clamps a
fraction up to the high-water mark at the source, and the projector merges by max
on the other side — two independent layers, because the source that skips the
first one is exactly the source you do not control (an external worker, a future
adapter). A non-finite fraction is refused outright: a `NaN` high-water mark
compares false against everything and would silently disable the clamp for the
rest of the run.

## Ticks and transitions take different lanes

Progress rides the [realtime plane](../data-events/realtime.md), and the split
there maps onto exactly what progress needs:

- **Ticks are ephemeral** (`publish`) — at-most-once, fire-and-forget. Losing one
  costs nothing, because the next one carries a newer value. A sink that refuses
  a tick is logged, never raised: observability must not kill the work it
  observes.
- **Status transitions are durable** (`stage`) — staged in the transaction and
  relayed after commit. Losing one costs an eternally-running job on every
  dashboard, so a refused transition raises.

That is why a reporter wired to the realtime lane requires an outbox route as
well as a stream, and refuses to build without one — and why that route has to be
**progress's own**. Each transition is staged *and flushed*: a job can sit in
`waiting` across the end of the run that paused it, so a transition still buffered
for somebody else's flush is a transition nobody ever sees. Flushing a route the
application also stages business signals through would persist those signals
early, so `progress_outbox_spec()` gives progress a dedicated route relaying into
the same stream, and a reporter handed the realtime channel's own route is
refused. Run a relay for it next to the realtime one:

```python
from forze_kits.integrations.realtime import realtime_relay_lifecycle_step

steps = [
    realtime_relay_lifecycle_step(
        outbox_spec=progress_outbox_spec(), stream_spec=transport.stream_spec
    ),
    # ... the application's own realtime relay
]
```

There is one declared event for every kind of job. Consumers filter on the
payload's `kind`; there is no `export.progress` and no `reencrypt.progress`,
because a per-kind event name makes the realtime egress surface a function of how
many kinds of work you happen to run. Register it in the application's catalog:

```python
from forze.application.contracts.realtime import RealtimeEventCatalog
from forze_kits.integrations.progress import JOB_PROGRESS_EVENT

catalog = RealtimeEventCatalog.of(JOB_PROGRESS_EVENT, *your_events)
```

### When the work and the store are in different processes

The reporter writes the record inline because that is where nearly all long work
runs: a sweep that can reach the job collection should just write to it. A worker
that *cannot* — an external process with no database access — publishes instead,
and something on the other side projects. That something is the same projector,
fed through `apply_signal`:

```python
# transitions relayed to a queue instead of the realtime stream
transitions = progress_outbox_spec(queue="jobs")

async def _project(message):
    await build_job_progress_projector(ctx).apply_signal(message.payload)

steps.append(
    queue_consumer_background_lifecycle_step(
        queue="jobs",
        queue_spec=jobs_queue_spec,
        handler=_project,
        inbox_spec=jobs_inbox_spec,
        tx_route="default",
    )
)
```

That gets you inbox dedup, poison parking and a tx-scoped write for free. What it
does **not** get you is the bar: only transitions are staged, so a record
projected this way moves 0 → 1 with nothing between. The ticks are still on the
stream for a live UI to read; they just do not reach a consumer fed by the outbox.

## The merge rules

The projector is the only place these live, and it applies every event against
the stored row rather than writing what it received:

1. **`progress` merges by max.** It never regresses, whatever the arrival order,
   and an indeterminate `None` never overwrites a fraction someone has been shown.
2. **Everything else follows one key** — `(terminal, at, seq, rank)` — and applies
   only when the incoming key is strictly greater. `at` is the reporter's own
   clock; `seq` is its emission counter, which is what orders a burst that a
   coarse or frozen clock stamps identically.
3. **A terminal status absorbs.** The terminal flag leads the key, so a straggling
   tick cannot resurrect a job that failed, however late its timestamp. Two racing
   terminals still converge: the later one wins, and at the same instant `failed`
   outranks `succeeded`.
4. **`heartbeat_at` takes the max** of every accepted event. It answers "when did
   we last hear from this job", so a reordered straggler must not make a live job
   look stale.
5. **An unknown job id creates its row.** A dashboard started mid-sweep, a
   consumer replaying from an offset — a late joiner gets a record, not an error.

Together these make the projection order-independent: any arrival order of one
job's reports lands on the same record.

`waiting` is the exception to absorption being the only irreversible move. It is
non-terminal and reversible — a resume moves `waiting → running` normally — and
`progress` carries across the pause rather than resetting. That is what makes the
record the spine of a **terminate-and-resume** task: the run that produced a
question for a human genuinely succeeded, and only the *job* is still open. The
question, the answer, and the resume command stay in your domain; the framework
owes the pattern this one status and the grain that spans the pause.

## The collection

`job_record_spec()` hands you the spec; the table is yours. Wire it tenant-aware
and the adapter injects and scopes `tenant_id` — the models carry no tenant field
of their own:

```sql
CREATE TABLE jobs (
    id              uuid PRIMARY KEY,
    rev             integer NOT NULL,
    created_at      timestamptz NOT NULL,
    last_update_at  timestamptz NOT NULL,
    tenant_id       uuid NOT NULL,          -- adapter-managed (tenant_aware routes)
    kind            text NOT NULL,
    status          text NOT NULL,
    progress        double precision,
    message         text,
    subject         text,
    durable_run_id  text,
    error           text,
    heartbeat_at    timestamptz NOT NULL,
    started_at      timestamptz,
    finished_at     timestamptz,
    event_at        timestamptz NOT NULL,
    event_seq       integer NOT NULL
);

CREATE INDEX jobs_staleness ON jobs (tenant_id, status, heartbeat_at);
```

Use `double precision`, not `real`: the merge compares the fraction the store
handed back, and a narrowed column returns a different number than the one that
was written.

Neither the collection nor the transitions route is a spec you wrote, so both are
exactly what inventory reconciliation exists to catch — bound at runtime,
catalogued nowhere, and invisible to an export. Merge the contribution at
assembly, passing the halves this deployment actually wires (a catalogued route
nothing binds fails startup):

```python
from forze_kits.integrations.progress import progress_spec_contributions

registry = SpecRegistry().register(*my_specs).merge(
    progress_spec_contributions(
        spec=job_record_spec(), outbox_spec=progress_outbox_spec()
    )
)
```

The job collection is catalogued **exportable** — nothing recomputes the history
of what ran — while the transitions route is drained like any other outbox.

Pass `encryption=` to seal `message`, `subject` or `error` — the fields that carry
business meaning. Sealing the query surface (`kind`, `status`, `progress`,
`heartbeat_at`, `event_at`, `event_seq`) is refused at build: the projector merges
on those and the staleness sweep filters on them, so sealed they would compare
ciphertext and answer wrongly rather than fail.

## Is anything stuck?

Staleness is the operational question the heartbeat exists for, and it is
answered by the index rather than by reading payloads:

```python
from datetime import timedelta

from forze.base.primitives import utcnow

stalled = await projector.find_stalled(
    silent_since=utcnow() - timedelta(minutes=15),
    limit=50,
)
```

It returns started-but-unfinished jobs, quietest first, so an operator can look at
a bounded number of the worst offenders whatever the collection's size. A finished
job is never stuck, however long ago it finished; a `waiting` one still counts —
a task paused on a human answer that nobody answers is exactly what you want to
hear about. The page is capped, so never read `len(...)` as "how many are stuck":
`count_stalled(...)` is that question.

Nothing in the request path is going to notice a job that stopped reporting four
hours ago, so the plane sweeps for you and exports the answer:

```python
from forze_kits.integrations.progress import (
    instrument_job_progress,
    job_staleness_lifecycle_step,
)

step, monitor = job_staleness_lifecycle_step(silent_after=timedelta(minutes=15))
instrument_job_progress(monitor)          # gauges, via the global OTel meter
lifecycle_steps.append(step)
```

Three gauges, and the third is the one that is easy to leave out:

| Metric | Says |
|---|---|
| `forze.jobs.stalled` | how many jobs are started, unfinished, and silent |
| `forze.jobs.stalled.oldest_silence` | how long the quietest one has been silent (seconds) |
| `forze.jobs.staleness.scan_age` | how old that answer is (seconds; `-1` = never swept) |

An OTel callback is synchronous and the staleness query is not, so the gauges read
the last sweep's answer — the *sweep* interval, not the scrape interval, is this
signal's resolution. Which means a sweep loop that dies leaves the first two frozen
at their last value, almost always zero, and every dashboard built on them alone
goes green at the moment it stops knowing anything. **Alert on stuck jobs or a scan
age that stops moving**, never on the count by itself.

`silent_after` has no default: it must be comfortably longer than how often the
work in question reports, or every healthy job reads as stuck. Pass `kinds=(...)`
to break the gauges out per job kind — declared up front, because `kind` is your
vocabulary and labelling by whatever the collection happens to hold makes metric
cardinality a function of runtime data. The tenant is never a label; a
`tenants=` sweep sums into one number and the record answers *which*.

The panels and alert rules keyed to these names ship with the rest of the
reference dashboards — see [Observability](observability.md).

## What stays out

- **No `ctx.progress`.** The reporter is built where the job is, and passed. Most
  operations have no job, and widening the execution context for a kit-level
  concern inverts the layering.
- **No phases, sub-tasks, weighted trees or ETA.** Aggregation semantics are
  domain-specific: run one job per phase, or put the phase in `message`. An ETA is
  a UI computation over the record's history, not stored state.
- **No cancellation.** A job record observes; asking work to stop is run control.
- **No scheduler and no retry.** The work runs wherever it runs — a handler, a
  durable run, an external process — and this plane only watches.

Two long walks already report through it: the archive export in
[Portability](portability.md) and `rebuild_search_index`, both of which take a
`progress=` reporter and leave the job's own start and finish to whoever created
it — one job legitimately covers several sweeps.

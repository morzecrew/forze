# RFC 0070 — Outbox consume mode

- **Status:** 🚧 In progress
- **Scope:** An opt-in storage mode for the Postgres outbox that writes a row that publishes on
  its first try twice, at staging and when it is deleted after its publish, rather than three
  times plus a retention delete. A retry or a park adds an update, as today.
  - A relay pass runs in one transaction. It locks a batch without creating new row versions,
    publishes each row, deletes the published ones, and records retries and parks per row,
    then commits.
  - A failed pass ends the background step's tick and backs it off.
  - An optional insert-only delivery log keeps "was it delivered" answerable.

  The outbox ports keep their methods and meanings. The relay gains a pass scope, a pass
  deadline and a publish timeout. Staging, other backends' outboxes and exactly-once delivery are out of
  scope.
- **Related:**
  - [`src/forze_postgres/adapters/outbox.py`](../src/forze_postgres/adapters/outbox.py): the
    store;
  - [`src/forze_kits/integrations/outbox/_relay_core.py`](../src/forze_kits/integrations/outbox/_relay_core.py):
    the relay loop;
  - [`src/forze_kits/integrations/outbox/lifecycle.py`](../src/forze_kits/integrations/outbox/lifecycle.py):
    the background relay step;
  - [`src/forze/application/contracts/outbox/ports.py`](../src/forze/application/contracts/outbox/ports.py):
    `OutboxQueryPort`;
  - [`pages/docs/reference/outbox-schema.md`](../pages/docs/reference/outbox-schema.md): the
    table and its index;
  - the outbox relay wake (`OutboxWake`, PR #546).
- **Origin:** hh-backend-v2's realtime load tests, request 7 of 7. It was held on 2026-10-13
  until they confirmed the cause, and they sent the measurements and a specification on
  2026-10-14.

---

## 1. Summary

`PostgresOutboxConfig(storage="consume")` changes what the store's relay-side methods write:

- `claim_pending` locks a batch with `FOR UPDATE SKIP LOCKED` and does not update it. It
  refuses to run outside a transaction.
- `mark_published` deletes the rows. It can append them to a delivery log in the same
  statement.
- `mark_retry` and `mark_failed` match the locked `pending` rows rather than `processing`
  ones.

The relay core runs each pass inside the port's pass scope, which in this mode is a
transaction on the store's client. A row that publishes on its first try therefore costs an
insert and a delete: no update, so no update-driven index churn. Nothing is left in
`processing` to reclaim, and nothing published is left to prune.

One row's failure still never aborts its batch. A failure of the pass itself rolls the whole
batch back, so the background step ends that tick and backs off rather than republishing it
at once.

## 2. Motivation

hh-backend-v2 relays about 300 outbox rows a second at its largest load mix (161 per poker
hand, about 790 bytes each). Each row is written three times:

| Step | Statement | Changes |
| --- | --- | --- |
| stage | `INSERT … status = 'pending'` | a new row and its index entries |
| claim | `UPDATE … SET status = 'processing', processing_at = …` | `status`, `processing_at` |
| publish | `UPDATE … SET status = 'published', published_at = …` | `status`, `published_at` |

Both updates change `status`, which every claim index forze documents includes. Neither is
therefore HOT: each inserts new entries into every index and leaves a dead heap tuple and
dead index entries behind for vacuum. forze never prunes published rows (`outbox.py:466`), so
every application writes its own retention `DELETE`, a fourth write.

What it cost them (their execution log, D-269):
- **Day-long retention:** a day of published rows grew the table to a million rows and 1 GB.
  It was autovacuumed 46 times, and every table's frames stalled in bursts about a minute
  apart: outbox lag peaked at 10.6 s, with 778 echoes over a second.
- **Pruning confirmed the cause:** pruning brought the lag peak down to 350 ms and the slow
  echoes to one.
- **Today:** they keep an hour and tune autovacuum hard (scale factor 0.02, cost limit 800).
  The table holds steady near 840,000 rows and is vacuumed about once a minute. That works,
  but every row is still written three times and deleted once.

## 3. Current state

All verified against the tree at the time of writing.

- **Staging.** `persist_rows` inserts with `ON CONFLICT (outbox_route, event_id) DO NOTHING`
  (`outbox.py:144-157`). It relies on the table's `UNIQUE (outbox_route, event_id)`
  (`outbox-schema.md:35`), which deduplicates a re-staged event for as long as its row exists.
  With no pruning, that is forever.
- **Claim.** `claim_pending` picks rows with `FOR UPDATE SKIP LOCKED`, in `created_at` order
  (or `hlc` order when that is on), and updates them to `processing` in the same statement
  (`outbox.py:195-226`). The statement runs on an autocommit connection: the row lock ends
  with it, and `processing` is what keeps a second relay away from the row.
- **Marks.** Each mark matches only rows in `processing`, and each commits on its own.
  - `mark_published` and `mark_failed` update `status`, `published_at` and `last_error`
    (`outbox.py:279-311`).
  - `mark_retry` sets the row back to `pending` and updates `attempts`, `available_at` and
    `last_error` (`outbox.py:313-348`).
  - `reclaim_stale_processing` reads and writes `processing_at` (`outbox.py:350-385`).
  - `requeue_failed` resets `processing_at` and `published_at` (`outbox.py:387-413`).
- **The relay loop** (`relay_outbox_claims`, `_relay_core.py:188-371`) claims a batch, then
  handles each row on its own:
  - a payload that will not decode is parked with `mark_failed`;
  - a publish error is rescheduled with backoff through `mark_retry` until `max_attempts`,
    then parked;
  - published ids are marked in chunks of 32 (`_MARK_CHUNK`).

  One row's failure never aborts the batch. A missing keyring is the one exception: it aborts
  the pass and leaves the rows for reclaim. The relay applies no timeout of its own to a
  publish (`relay.py:383-391`).
- **The background step** (`lifecycle.py`) runs up to `max_batches_per_tick` passes a tick,
  reclaiming on the first. When a pass raises outside `strict` mode, it logs the error and
  moves on to the next pass straight away (`lifecycle.py:402-409`). Under retain that is
  safe, because the failed rows stay in `processing` until `reclaim_stale_after`. A batch is
  `max_claim_rows` rows, 100 by default (`integration_config.py:25`).
- **The indexes.** The schema page documents `(outbox_route, status, available_at,
  created_at)` (`outbox-schema.md:39-40`). The store's docstring recommends
  `(outbox_route, status, created_at)` and its `hlc` and tenant variants
  (`outbox.py:52-62`), which leave out `available_at`.
  - The specification also lists a reclaim index on `processing_at` and a partial index on
    published rows. forze does not document either for Postgres; they are the application's
    own. Mongo's documented index does include `processing_at`.
- **The ports.**
  - `OutboxQueryPort` is a `Protocol`. The Postgres, Mongo and mock stores all subclass it,
    so a method with a default body reaches all three. A third-party store that implements
    it structurally would not get the default.
  - `OutboxAdminPort` probes `pending`, `processing` and `failed` through the claim index.
  - The DST conformance battery's delivery family claims outside any transaction and models
    a crash followed by reclaim (`forze_dst/conformance/delivery.py:121-133`).

## 4. Goals / Non-goals

**Goals**

- An insert and a delete per row that publishes on its first try, with no update.
- No `processing` state, so nothing to reclaim after a relay dies.
- No retention `DELETE` over published rows.
- The same at-least-once delivery, and the same order within a route when one relay drains it.
- Per-row failure handling unchanged: retries with backoff, parking, `requeue_failed`.
- A failed pass never republishes its batch in a tight loop.
- Optionally, "was this event's delivery committed, and when" answered from an append-only
  log that is dropped by partition.

**Non-goals**

- **Exactly-once delivery.** Consumers keep deduplicating by event id.
- **Changing the stage side.** `OutboxCommand` and `persist_rows` are untouched.
- **Other backends.** Mongo's outbox keeps the retain mode. Its transactions and write
  conflicts would need their own design.

## 5. Design

### 5.1 Configuration

```python
PostgresOutboxConfig(
    relation=("ops", "outbox"),
    storage="consume",                   # "retain" (today's, the default) | "consume"
    pass_deadline=timedelta(seconds=5),  # consume mode: when a pass stops publishing
    pass_statement_timeout=timedelta(seconds=1),  # consume mode: each claim and mark statement
    delivery_log=OutboxDeliveryLog(      # optional, consume mode only
        relation=("ops", "outbox_delivered"),
    ),
)

OutboxRelay(
    outbox_spec=...,
    publish_timeout=timedelta(seconds=2),   # new, optional: bounds one publish
)
```

Configuration refuses `delivery_log` under `storage="retain"`.

### 5.2 A pass is one transaction

`OutboxQueryPort` gains one method with a default body:

```python
def pass_scope(self) -> AbstractAsyncContextManager[OutboxPassBudget | None]:
    """Scope one relay pass: claim, publish and mark. A no-op unless the store needs it."""
    return nullcontext(None)
```

`relay_outbox_claims` enters it around a pass, and treats a store that lacks the method (one
that implements the protocol structurally) as having the no-op.
- **Postgres in consume mode.** The scope opens the client's `transaction()`. Inside a
  caller's transaction on the same client, that becomes a savepoint, which is how a
  hand-driven relay with a lease fences the pass (5.6).
- **Mongo, the mock and retain mode** keep the no-op.

**The pass deadline.** The consume scope yields a budget ending `pass_deadline` after the pass
began.
- The relay checks it before each publish. Once it has passed, the relay publishes no more
  rows, flushes its marks and lets the scope commit. The rows it did not reach were never
  changed, so they return to `pending` as they were.
- Each publish is bounded by the shorter of `publish_timeout` and the budget left. A publish
  cut short is a transient failure, retried with backoff.
- **The scope bounds the database side too, when it owns the transaction.** When the pass
  opens the root transaction, the scope sets three timeouts with `SET LOCAL`:
  - `statement_timeout` and `lock_timeout` to `pass_statement_timeout`, so a claim or mark
    blocked on a lock, such as a delivery-log insert, fails rather than waits;
  - `idle_in_transaction_session_timeout` to the deadline plus a margin.

  They end with the pass's transaction. A statement that times out fails the pass (5.3). A
  relay that stalls outright, for example on a blocked event loop, loses its session and
  rolls back rather than holding the locks. A destination on the same client runs its SQL
  under the same `statement_timeout` (§9).
- **Inside a caller's transaction the scope sets nothing.** `SET LOCAL` would outlive the
  savepoint and stay in force for the rest of the caller's transaction, including its fence
  check. It would also replace any stricter limits the caller had set. So a caller that
  fences a pass (5.6) owns its transaction's timeouts, and the docs say so. The publishing
  deadline still applies, because the relay enforces it.
- **Both durations must be positive,** and configuration refuses zero or less. In Postgres a
  zero `statement_timeout` means no limit at all.

So a pass that owns its transaction holds its locks for at most `pass_deadline`, plus its
final mark statements, each bounded by `pass_statement_timeout`. There are a few of them: the
last published chunk and one statement per distinct retry count.

A per-publish timeout alone bounds nothing: the relay marks published rows only every 32
publishes and flushes retries at the end of the pass, so a batch of slow publishes leaves the
transaction idle for their sum.

Inside the pass, consume mode changes what each method writes:

| Method | Retain (today) | Consume |
| --- | --- | --- |
| `claim_pending` | `UPDATE … SET status = 'processing'` on the picked rows | `SELECT … FOR UPDATE SKIP LOCKED` on the same predicate and order. It creates no row version and no index entries; the lock only stamps each row. It refuses outside a transaction (`require_transaction`). |
| `mark_published` | `UPDATE … SET status = 'published'` where `processing` | `DELETE … WHERE id = ANY(…) AND status = 'pending'`, and with a delivery log, `INSERT INTO <log> … SELECT` from its `RETURNING` in one statement |
| `mark_retry` | back to `pending`; attempts, availability, error; where `processing` | attempts, availability, error, where `pending` |
| `mark_failed` | `status = 'failed'`, `published_at` kept, where `processing` | `status = 'failed'` and the error, where `pending` |
| `requeue_failed` | resets `processing_at` and `published_at` as well | resets only what consume mode uses |
| `reclaim_stale_processing` | returns stale `processing` rows | the same; only rows left by retain mode can match (5.7) |

Each mark matches `pending` rather than `processing`, because a claimed row stays `pending`
under consume and the pass's row lock is what marks it as this relay's. A mark that matches
fewer rows than it was given is a defect, and the tests pin the row count.

The relay loop does not change, so per-row handling is exactly today's. A row that will not
decode is parked. A row whose publish fails or exceeds its bound gets its attempt count and
backoff, and is parked at `max_attempts`. Every published row is deleted. The whole
pass commits once.

### 5.3 A failed pass

Some failures are the pass's own rather than one row's:
- a missing keyring;
- a delivery-log insert that fails, for example with no partition for today;
- a mark statement that fails;
- the session killed by `idle_in_transaction_session_timeout`.

Each rolls the whole pass back. Every row in the batch is `pending` again at once, and that
includes rows already published and any retry or park the pass recorded.

**Under retain, the step's "log and run the next pass" is safe.** The failed rows sit in
`processing` until reclaim, so the next pass takes other rows.

**Under consume, the same loop would be a republish storm.** The next pass would take the
same rows and publish them again, up to `max_batches_per_tick` times a tick and again on
every wake. The failing rows would never back off or park, because their retry state rolls
back each time. So, under consume:
- a pass that raises ends the tick, as `strict` mode already does;
- the step backs off over consecutive failed ticks: exponential, capped, and reset by a pass
  that commits;
- a wake does not cut a backoff short.

**Duplicates.** A rolled-back pass republishes its whole batch: up to `max_claim_rows` rows,
100 by default. Today a crash republishes at most one mark chunk, 32 rows, because each mark
commits on its own. Consumers deduplicate by event id either way. An application that wants
a smaller window lowers `max_claim_rows`.

### 5.4 What the table holds

In consume mode the table holds only pending and parked rows, so it stays near the size of
the in-flight backlog.
- `published_at` is never written.
- `processing_at` is read only by reclaim, which matters only while rows from retain mode
  remain (5.7).

Whether a retry is HOT depends on the application's claim index. The schema page's index
includes `available_at`, which a retry changes, so a retry is not HOT under it. The
docstring's `(outbox_route, status, created_at)` leaves it out, and then a retry can be.
Retries are rare either way, and a happy row never updates.

`UNIQUE (outbox_route, event_id)` stays, because staging needs it, so the minimum is three
indexes: the primary key, that unique index and the claim index. The specification's "one
index" missed the unique one.

**The dedup window shrinks.** A re-staged event is now deduplicated only while its row is
pending. Once it is consumed, staging the same `event_id` again publishes it again.
Applications that prune today already have this, after their retention period. Consumers
deduplicate by event id either way.

### 5.5 The delivery log

The log is insert-only. `mark_published` writes it in the same statement as its delete:

```sql
WITH consumed AS (
    DELETE FROM ops.outbox
    WHERE id = ANY(%(ids)s::uuid[]) AND status = 'pending'
    RETURNING id, outbox_route, event_id
)
INSERT INTO ops.outbox_delivered (id, outbox_route, event_id, published_at)
SELECT id, outbox_route, event_id, %(now)s FROM consumed
```

- **What a log row proves.** A row means the event was published and its consumption
  committed. A missing row does not prove the event was never delivered. A pass that
  published and then rolled back leaves none, and the next pass's publish adds one.
- **No unique key on the event.** A re-staged event can be delivered twice (5.4), and a
  partitioned table's unique keys must include the partition column anyway. The log carries
  a plain index on `event_id`, and "was X delivered" may return more than one row.
- **Retention is the application's.** Partition by `published_at` per day and drop old
  partitions. forze never creates or drops partitions; the docs show the DDL.
- **A missing partition fails the pass.** The log is in the pass's transaction by design. The
  docs recommend a default partition, or creating partitions ahead. The backoff in 5.3 keeps
  a missing one from becoming a storm.

### 5.6 Fencing a relay

An application that drives its own relay loop, as hh-backend-v2's courier does with a lease,
fences a pass without a hook:
1. It opens a transaction on the outbox's route around the pass.
2. The pass scope nests inside it as a savepoint.
3. It checks its fence before committing.

A relay that lost its lease rolls back, and its successor finds the rows pending.

The background step runs passes itself and has no fence, as today. Fencing it would need a
pass wrapper on the step; that is left out until someone needs it.

### 5.7 Migrating a live table

1. Deploy relays in consume mode with reclaim on. Their claims take `pending` rows. Their
   reclaim still returns `processing` rows that retain mode left behind, once they pass
   `reclaim_stale_after`, and the next pass consumes them.
2. Published rows already in the table are left to the application's existing retention job,
   which can go once they are gone.
3. Once no `processing` row is left, set `reclaim_stale_after=None`. Once the retention job
   has removed every `published` row, retire the job. Only then drop `processing_at`,
   `published_at`, and any index that existed only for them. Reclaim reads `processing_at`,
   and with that column gone every pass would fail. The retention job reads `published_at`,
   so dropping it first would break the job. The docs give the
   statements in this order; the application owns its migrations.

Mixed deployments, with retain and consume relays on one route, stay at-least-once. A row
claimed under retain is in `processing`, which a consume claim never matches, until it is
marked or reclaimed.

### 5.8 Alternatives considered

- **Delete at claim, roll back on a failed publish** (the specification's sketch: `DELETE …
  RETURNING`, publish, commit). It is the same number of writes, but a rollback also undoes
  the attempt count and the backoff. A poison row that always fails its publish would then be
  retried first on every pass forever, never parked, at the head of its route. It also
  publishes a batch all-or-nothing, which today's per-row handling deliberately is not.
  Rejected in favour of locking at claim and deleting at mark.
- **Partitioning the outbox by time and dropping partitions.** It removes the retention
  delete but keeps both updates. Rejected: the updates are the churn.
- **A separate parked-rows table.** It would keep the outbox to pending rows only, at the
  cost of a move on every park and a second relation for `requeue_failed` and the probes.
  Rejected: parks are rare, and `status = 'failed'` in place keeps the admin port unchanged.
- **A `tx_route` argument on the relay instead of a pass scope.** It would also suit a
  backend whose transactions are not the store client's, but it needs the relay and the
  background step to learn the route, and the store already knows its mode. Kept as the
  fallback if the pass scope does not survive implementation (§10).

## 6. Tests

Postgres integration:
- **The write budget.** Stage N rows and relay them under consume. `pg_stat_user_tables`
  reports `n_tup_ins = N`, `n_tup_del = N` and `n_tup_upd = 0` for the outbox, and the
  destination holds N entries in order.
- **Marks match.** Each mark's row count equals the ids it was given, for published, retried
  and parked rows.
- **A relay dying mid-pass.** Kill the relay between the claim and the commit. The rows are
  still pending, and a second relay publishes them.
- **The pass deadline.**
  - Against a destination slower than the deadline allows, the pass stops at the deadline,
    commits what it published, and leaves the rows it did not reach unchanged.
  - A mark blocked on a lock fails at `pass_statement_timeout` and rolls the pass back.
  - A pass that stalls without yielding is cut off by the idle timeout, and its rows
    return.
- **Per-row failure.** One row's publish raises, or exceeds `publish_timeout`, and the rest of
  its batch is published and deleted. The failing row keeps its incremented `attempts` and
  its backoff, and at `max_attempts` it is parked, not retried forever. A row that will not
  decode is parked at once.
- **A failed pass.** Make the delivery-log insert fail. The tick ends after one pass, the next
  tick waits out the backoff, and the batch is published once per tick, not once per pass.
- **Two relays on one route.** Disjoint batches, and every row published. One relay keeps
  creation order.
- **A fence.** An outer transaction that rolls back after the pass leaves every row pending.
- **The delivery log.** Every published id is in the log with its time. A re-staged event
  delivered twice has two log rows.
- **Migration.** A table holding `pending`, stale `processing` and `published` rows is
  drained by consume relays, with `published` rows left alone. With
  `reclaim_stale_after=None` and the two columns dropped, passes succeed.
- **Refusals.** A claim outside a transaction, and `delivery_log` under retain.
- **Load.** 300 rows a second for an hour under each mode, side by side: table size, dead
  tuples and autovacuum runs.

Unit and conformance:
- the relay core enters the pass scope once per pass, and the default scope is a no-op;
- the background step under consume ends a tick on a failed pass and backs off;
- the DST delivery family gains a consume leg that claims inside the pass scope and models a
  crash as a rollback rather than a reclaim. The mock keeps retain's dedup, and the battery
  does not assert dedup after consumption.

## 7. Docs

`outbox-schema.md` gains a consume-mode section:
- what the mode writes;
- the dedup window it narrows;
- the duplicate window on a failed pass;
- the columns it leaves unused, and the order in which they can go;
- the delivery log's DDL with daily partitions and a default partition;
- the migration steps.

The relay page states that a consume pass holds one connection and the batch's row locks for
up to `pass_deadline`, and how to size the deadline against the batch size and the
destination's latency.

## 8. Out of scope

- **Mongo's outbox.** It keeps retain mode. A consume mode there would need multi-document
  transactions held across a publish, and would abort on write conflicts.
- **Fencing the background step** (5.6).
- **Managing the delivery log's partitions.** That belongs to the application's migrations or
  a partition manager.
- **Exactly-once delivery.** At-least-once stays, with the wider batch window in 5.3.

## 9. Risks

- **A connection and the batch's row locks are held for a pass.** Today each claim and mark
  is a short statement of its own. Under consume, a pass holds one pooled connection, and the
  locks on its batch, for the length of its publishes. For a Redis `XADD` per row that is
  milliseconds. `pass_deadline` and `pass_statement_timeout` bound it, with the idle timeout
  as a backstop. Concurrent relays skip the
  locked rows, so they do not wait. Accepted and documented.
- **A re-stage waits for a pass.** Staging an event whose row a pass has deleted but not yet
  committed makes the `INSERT … ON CONFLICT` wait for that pass. The pass's length bounds it.
  Documented.
- **A destination on the same Postgres client joins the pass.** The store and the destination
  share the client's context-bound connection, so the publish runs inside the pass's
  transaction. That makes the delete and the delivery atomic, but an SQL error in the
  destination aborts the whole pass (5.3). Its statements also run under the pass's
  `statement_timeout`, so a destination slower than `pass_statement_timeout` fails the pass. No destination forze ships is Postgres-backed.
  Documented.
- **Retry policies on the query port.** A resilience retry inside an aborted transaction
  always fails, and a statement timeout aborts the transaction. Under consume a failed
  statement fails the pass and 5.3 takes over. Documented.
- **The narrower dedup window** (5.4) and **the wider duplicate window** (5.3). Documented.
  Consumers deduplicate by event id regardless.
- **A long pass holds back vacuum's horizon** for its length. That is milliseconds at the
  intended batch sizes, and `pass_deadline` bounds a slow destination.

## 10. Unresolved questions

- **Whether `pass_scope` is the right seam.** Three costs came out of review:
  - a structurally implemented third-party store lacks the method, so the relay has to
    tolerate its absence (row 3);
  - port proxies wrap a synchronous method as an instant span, so tracing and DST fault
    injection see nothing of the pass;
  - the client's `transaction()` yields a connection, so the consume scope wraps it to yield
    the pass budget instead.

  Implementation decides between the scope and a relay `tx_route` (5.8), with the lean in
  row 3.
- **The default for `storage`** stays `"retain"`. Whether to recommend consume for new tables
  is a docs question once the load comparison has numbers.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Consume mode is a Postgres store setting (`storage="consume"`, default `"retain"`). The outbox ports keep their methods and meanings, and the relay's per-row loop does not change: the mode changes what each store method writes and which status its marks match. |
| 2 | `LOCKED` | A pass runs in one transaction from claim to commit. The claim locks with `FOR UPDATE SKIP LOCKED` without updating, and refuses to run outside a transaction. Without a transaction spanning the publish there is no lock to stand in for `processing`. |
| 3 | `ASSUMED` | `OutboxQueryPort.pass_scope()` has a no-op default, and the relay treats a store without it as having the no-op. The relay core enters it once per pass, and the Postgres consume store returns its client's transaction, which nests as a savepoint in a caller's. A relay `tx_route` is the fallback if the scope's costs (§10) prove real. |
| 4 | `LOCKED` | Per-row failure handling stays: retries keep their attempt count and backoff, and a row is parked at `max_attempts` or when it will not decode. Under consume each mark matches `pending`, since a claimed row is never `processing`. A published row is deleted at its mark, not at its claim. Deleting at claim and rolling back on failure would lose the attempt count and retry a poison row first on every pass, forever. |
| 5 | `ASSUMED` | A parked row stays in the table as `status = 'failed'`, so `requeue_failed` and the admin probes keep working. |
| 6 | `ASSUMED` | `UNIQUE (outbox_route, event_id)` stays, and its dedup now covers pending rows only. Documented as a narrowing. |
| 7 | `ASSUMED` | The delivery log is optional and insert-only, has no unique key on the event, and is written in the same statement as the delete, so a failure to log fails the pass. A row proves committed consumption, and a missing row does not prove the event was never delivered. forze never manages its partitions. |
| 8 | `ASSUMED` | A hand-driven relay fences by opening its own transaction around the pass, with the pass scope nested as a savepoint. The background step gets no fence. |
| 9 | `ASSUMED` | Consume mode never writes `processing_at` or `published_at`. Reclaim still reads `processing_at`, so rows left by retain mode drain without a migration step, and the column can go only once reclaim is turned off. |
| 10 | `ASSUMED` | Other backends keep retain mode only. |
| 11 | `LOCKED` | Under consume, a pass that raises ends the background step's tick, and consecutive failed ticks back off. A wake does not cut a backoff short. A rolled-back pass makes its whole batch claimable at once, so today's "log and run the next pass" would republish it up to `max_batches_per_tick` times a tick, and its retry state would never persist. |
| 12 | `ASSUMED` | `OutboxRelay` gains an optional `publish_timeout`. A publish that exceeds it is a transient failure, retried with backoff. |
| 13 | `LOCKED` | Under consume, a pass stops publishing at `pass_deadline` (default 5 s), bounds each publish by the budget left, and commits what it did; rows it did not reach are unchanged. When the pass owns its root transaction, the scope sets `statement_timeout` and `lock_timeout` to `pass_statement_timeout` (default 1 s) and `idle_in_transaction_session_timeout` to the deadline plus a margin, all with `SET LOCAL`. Inside a caller's transaction it sets nothing, because `SET LOCAL` would outlive the savepoint and override the caller's limits. Both durations must be positive. A per-publish timeout alone bounds nothing, because marks flush only every 32 publishes and retries at the pass's end. |

## 12. Phasing

1. **Consume mode:**
   - the setting and its refusals;
   - `pass_scope` and the relay core entering it;
   - the method variants and their `pending` predicates;
   - the background step's tick end and backoff;
   - `pass_deadline` and `publish_timeout`;
   - the conformance leg;
   - the integration tests in §6 apart from the load comparison;
   - the schema page.
2. **The delivery log,** with its integration tests and DDL.
3. **The load comparison** against retain mode. hh-backend-v2 runs it on their mix, and the
   numbers go in the docs.

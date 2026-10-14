# RFC 0070 — Outbox consume mode

- **Status:** 📝 Draft
- **Scope:** An opt-in storage mode for the Postgres outbox that writes a row twice, at staging
  and when it is deleted after its publish, rather than three times plus a retention delete.
  - A relay pass runs in one transaction. It locks a batch without writing it, publishes each
    row, deletes the published ones, and records retries and parks as today, then commits.
  - An optional insert-only delivery log keeps "was it delivered" answerable.

  The outbox ports keep their methods and meanings; one optional method, a pass scope, is
  added, with a no-op default. The relay core enters that scope. Staging, other backends'
  outboxes and exactly-once delivery are out of scope.
- **Related:**
  - [`src/forze_postgres/adapters/outbox.py`](../src/forze_postgres/adapters/outbox.py): the
    store;
  - [`src/forze_kits/integrations/outbox/_relay_core.py`](../src/forze_kits/integrations/outbox/_relay_core.py):
    the relay loop;
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

`PostgresOutboxConfig(storage="consume")` changes what the store's relay-side methods write.
The relay and the ports keep their contract:

- `claim_pending` locks a batch with `FOR UPDATE SKIP LOCKED` and writes nothing. It refuses
  to run outside a transaction.
- `mark_published` deletes the rows. It can append them to a delivery log in the same
  statement.
- `mark_retry` and `mark_failed` write as today: a retry updates the retry columns, and a
  park sets `failed`.

The relay core runs each pass inside the port's pass scope, which in this mode is a
transaction on the store's client. A happy row therefore costs an insert and a delete, with
no update and so no update-driven index churn. Nothing is left in `processing` to reclaim,
and nothing published is left to prune. One row's failure still never aborts its batch, as
it never does today.

## 2. Motivation

hh-backend-v2 relays about 300 outbox rows a second at its largest load mix (161 per poker
hand, about 790 bytes each). Each row is written three times:

| Step | Statement | Changes |
| --- | --- | --- |
| stage | `INSERT … status = 'pending'` | a new row and its index entries |
| claim | `UPDATE … SET status = 'processing', processing_at = …` | `status`, `processing_at` |
| publish | `UPDATE … SET status = 'published', published_at = …` | `status`, `published_at` |

Both updates change `status`, which leads the claim index forze documents. Neither is
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
  (`outbox.py:195-226`). Each statement commits on its own: the row lock ends when the claim
  does, and `processing` is what keeps a second relay away from the row.
- **Marks.**
  - `mark_published` and `mark_failed` update `status`, `published_at` and `last_error`
    (`outbox.py:279-311`).
  - `mark_retry` sets the row back to `pending` and updates `attempts`, `available_at` and
    `last_error` (`outbox.py:313-348`).
  - `reclaim_stale_processing` returns rows stranded in `processing` past a cutoff
    (`outbox.py:350-385`).
- **The relay loop** (`_relay_core.py:225-340`) claims a batch, then handles each row on its
  own:
  - a payload that will not decode is parked with `mark_failed`;
  - a publish error is rescheduled with backoff through `mark_retry` until `max_attempts`,
    then parked;
  - published ids are marked in chunks of 32.

  One row's failure never aborts the batch. A missing keyring is the one exception: it aborts
  the pass and leaves the rows for reclaim.
- **The documented index** is one claim index, `(outbox_route, status, available_at,
  created_at)` (`outbox-schema.md:39-40`), and the store's docstring gives its `hlc` and
  tenant variants (`outbox.py:52-62`).
  - The specification also lists a reclaim index on `processing_at` and a partial index on
    published rows. forze does not document either for Postgres; they are the application's
    own. Mongo's documented index does include `processing_at`.
- **The ports.**
  - `OutboxQueryPort` is a `Protocol` that the Postgres, Mongo and mock stores all subclass.
    A method with a default body therefore reaches all three.
  - `OutboxAdminPort` probes `pending`, `processing` and `failed` through the same index.

## 4. Goals / Non-goals

**Goals**

- An insert and a delete per row that publishes on its first try, with no update.
- No `processing` state, so nothing to reclaim after a relay dies.
- No retention `DELETE` over published rows.
- The same at-least-once delivery, and the same order within a route when one relay drains it.
- Per-row failure handling unchanged: retries with backoff, parking, `requeue_failed`.
- Optionally, "was this event delivered, and when" answered from an append-only log that is
  dropped by partition.

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
    delivery_log=OutboxDeliveryLog(      # optional, consume mode only
        relation=("ops", "outbox_delivered"),
    ),
)
```

Configuration refuses `delivery_log` under `storage="retain"`.

### 5.2 A pass is one transaction

`OutboxQueryPort` gains one method with a default body:

```python
def pass_scope(self) -> AbstractAsyncContextManager[None]:
    """Scope one relay pass: claim, publish and mark. A no-op unless the store needs it."""
    return nullcontext()
```

`relay_outbox_claims` enters it around a pass. The Postgres store in consume mode returns its
client's `transaction()`. Inside a caller's transaction on the same client, that becomes a
savepoint, which is how a relay with a lease fences the pass (5.5). Mongo, the mock and
retain mode keep the no-op.

Inside the pass, consume mode changes what each method writes:

| Method | Retain (today) | Consume |
| --- | --- | --- |
| `claim_pending` | `UPDATE … SET status = 'processing'` on the picked rows | `SELECT … FOR UPDATE SKIP LOCKED` on the same predicate and order; writes nothing; refuses outside a transaction (`require_transaction`) |
| `mark_published` | `UPDATE … SET status = 'published'` | `DELETE … WHERE id = ANY(…)`, and with a delivery log, `INSERT INTO <log> … SELECT` from its `RETURNING` in one statement |
| `mark_retry` | back to `pending`; attempts, availability, error | attempts, availability, error; `status` is already `pending` |
| `mark_failed` | `status = 'failed'` | the same |
| `reclaim_stale_processing` | returns stale `processing` rows | the same; only rows left by retain mode can match (5.6) |

The relay loop does not change, so per-row handling is exactly today's. A row that will not
decode is parked. A row whose publish fails gets its attempt count and backoff, and is parked
at `max_attempts`. Every published row is deleted. The whole pass commits once.

**Failure semantics:**
- **The relay dies, or the pass raises,** before commit: the transaction rolls back, and
  every row in the batch is pending again for the next pass, with nothing to reclaim. That
  includes rows it had already published, so they are published again, which is today's
  duplicate window.
- **The publish succeeds and the commit fails:** the rows are published again on the next
  pass. That is the same at-least-once duplicate as today's mark-after-publish.
- **A missing keyring** aborts the pass as today, and the rows return at once instead of
  waiting for reclaim.

### 5.3 What the table holds

In consume mode the table holds only pending and parked rows, so it stays near the size of
the in-flight backlog. The columns `processing_at` and `published_at` are never read or
written, and an application may drop them. The documented claim index is unchanged:
- a happy row never updates it;
- a retry updates `available_at`, which the index includes, so a retry is not HOT. Retries
  are rare, and that is accepted.

`UNIQUE (outbox_route, event_id)` stays, because staging needs it, so the minimum is three
indexes: the primary key, that unique index and the claim index. The specification's "one
index" missed the unique one.

**The dedup window shrinks.** A re-staged event is now deduplicated only while its row is
pending. Once it is consumed, staging the same `event_id` again publishes it again.
Applications that prune today already have this, after their retention period. Consumers
deduplicate by event id either way.

### 5.4 The delivery log

The log is insert-only, keyed by `(event_id, outbox_route)`, with `published_at`.
`mark_published` writes it in the same statement as its delete:

```sql
WITH consumed AS (
    DELETE FROM ops.outbox WHERE id = ANY(%(ids)s::uuid[])
    RETURNING id, outbox_route, event_id
)
INSERT INTO ops.outbox_delivered (id, outbox_route, event_id, published_at)
SELECT id, outbox_route, event_id, %(now)s FROM consumed
```

A failure to write the log fails the delete with it, and the pass rolls back by design.
Retention is the application's: partition the log by day and drop old partitions. forze never
creates or drops partitions. The docs show the DDL.

### 5.5 Fencing a relay

An application whose relay holds a lease, as hh-backend-v2's courier does, needs it checked
before the pass commits. No hook is needed:
1. The application opens a transaction on the outbox's route around the pass.
2. The pass scope nests inside it as a savepoint.
3. The application checks its fence before committing.

A relay that lost its lease rolls back, and its successor finds the rows pending.

### 5.6 Migrating a live table

1. Deploy relays in consume mode. Their claims take `pending` rows as today. Their reclaim
   still returns `processing` rows that retain mode left behind, once they pass
   `reclaim_stale_after`, and the next pass consumes them.
2. Published rows already in the table are left to the application's existing retention job,
   which can go once they are gone.
3. Afterwards the application may drop `processing_at`, `published_at`, and any indexes that
   existed only for them. The docs give the statements; the application owns its migrations.

Mixed deployments, with retain and consume relays on one route, stay at-least-once. A row
claimed under retain is invisible to a consume claim until it is marked or reclaimed.

### 5.7 Alternatives considered

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

## 6. Tests

Postgres integration:
- **The write budget.** Stage N rows and relay them under consume. `pg_stat_user_tables`
  reports `n_tup_ins = N`, `n_tup_del = N` and `n_tup_upd = 0` for the outbox, and the
  destination holds N entries in order.
- **A relay dying mid-pass.** Kill the relay between the claim and the commit. The rows are
  still pending, and a second relay publishes them.
- **Per-row failure.** One row's publish raises, and the rest of its batch is published and
  deleted. The failing row keeps its incremented `attempts` and its backoff, and at
  `max_attempts` it is parked, not retried forever. A row that will not decode is parked at
  once.
- **Two relays on one route.** Disjoint batches, and every row published. One relay keeps
  creation order.
- **A fence.** An outer transaction that rolls back after the pass leaves every row pending.
- **The delivery log.** Every published id is in the log with its time. A log write that
  fails rolls the pass back.
- **Migration.** A table holding `pending`, stale `processing` and `published` rows is
  drained by consume relays. `published` rows are left alone.
- **Refusals.** A claim outside a transaction, and `delivery_log` under retain.
- **Load.** 300 rows a second for an hour under each mode, side by side: table size,
  dead tuples and autovacuum runs.

Unit: the relay core enters the pass scope once per pass; the default scope is a no-op.

## 7. Docs

`outbox-schema.md` gains a consume-mode section:
- what the mode writes;
- the dedup window it narrows;
- the columns it leaves unused;
- the delivery log's DDL with daily partitions;
- the migration steps.

The relay page states that a consume pass holds one connection and the batch's row locks for
the length of its publishes.

## 8. Out of scope

- **Mongo's outbox.** It keeps retain mode. A consume mode there would need multi-document
  transactions held across a publish, and would abort on write conflicts.
- **Managing the delivery log's partitions.** That belongs to the application's migrations or
  a partition manager.
- **Exactly-once delivery.** The duplicate window is unchanged.

## 9. Risks

- **A connection and the batch's row locks are held for a pass.** Today each claim and mark
  is a short statement of its own. Under consume, a pass holds one pooled connection, and the
  locks on its batch, for the length of its publishes. For a Redis `XADD` per row that is
  milliseconds. The batch size bounds it, and a relay should cap each publish below the
  server's `idle_in_transaction_session_timeout`. Concurrent relays skip the locked rows, so
  they do not wait. Accepted and documented.
- **The narrower dedup window** (5.3). Documented. Consumers deduplicate by event id
  regardless.
- **Retries are not HOT** under the documented index, because `available_at` is in it.
  Accepted: retries are rare, and a happy row never updates.
- **A long pass holds back vacuum's horizon** for its length. It is milliseconds at the
  intended batch sizes. A slow destination makes it longer, and the publish timeout bounds
  it.

## 10. Unresolved questions

- Whether `pass_scope` is the right seam, or a `tx_route` argument on the relay. The scope
  keeps the relay's signature unchanged and puts the decision in the store, which knows its
  mode. A `tx_route` would also suit a backend whose transactions are not the store client's.
  Implementation decides, with the lean in row 3.
- The default for `storage` stays `"retain"`. Whether to recommend consume for new tables is
  a docs question once the load comparison has numbers.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Consume mode is a Postgres store setting (`storage="consume"`, default `"retain"`). The outbox ports keep their methods and meanings, and the relay loop does not change: the mode changes only what each store method writes. |
| 2 | `LOCKED` | A pass runs in one transaction from claim to commit. The claim locks with `FOR UPDATE SKIP LOCKED` and writes nothing, and it refuses to run outside a transaction. Without a transaction spanning the publish there is no lock to stand in for `processing`. |
| 3 | `ASSUMED` | `OutboxQueryPort.pass_scope()` has a no-op default. The relay core enters it once per pass, and the Postgres consume store returns its client's transaction, which nests as a savepoint in a caller's. |
| 4 | `LOCKED` | Per-row failure handling stays: retries keep their attempt count and backoff, and a row is parked at `max_attempts` or when it will not decode. A published row is deleted at its mark, not at its claim. Deleting at claim and rolling back on failure would lose the attempt count and retry a poison row first on every pass, forever. |
| 5 | `ASSUMED` | A parked row stays in the table as `status = 'failed'`, so `requeue_failed` and the admin probes keep working unchanged. |
| 6 | `ASSUMED` | `UNIQUE (outbox_route, event_id)` stays, and its dedup now covers pending rows only. Documented as a narrowing. |
| 7 | `ASSUMED` | The delivery log is optional and insert-only, written in the same statement as the delete, so a failure to log fails the pass. forze never manages its partitions. |
| 8 | `ASSUMED` | A fence is the caller's own transaction around the pass, with the pass scope nested as a savepoint. No pre-commit hook. |
| 9 | `ASSUMED` | Consume mode never reads or writes `processing_at` or `published_at`. Reclaim keeps working so that rows left by retain mode drain without a migration step. |
| 10 | `ASSUMED` | Other backends keep retain mode only. |

## 12. Phasing

1. **Consume mode:**
   - the setting and its refusals;
   - `pass_scope` and the relay core entering it;
   - the four method variants;
   - the integration tests in §6 apart from the load comparison;
   - the schema page.
2. **The delivery log,** with its integration tests and DDL.
3. **The load comparison** against retain mode. hh-backend-v2 runs it on their mix, and the
   numbers go in the docs.

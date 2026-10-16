---
title: Outbox table schema
icon: lucide/table
summary: The application-owned outbox table — columns, indexes, migrations, and the HLC ordering column
---

The outbox table is application-owned: you create and migrate it. This is the
schema the relay claims from and the columns it reads. The wiring that stages and
relays events is in the [Transactional outbox recipe](../recipes/transactional-outbox.md).

## Postgres

For Postgres (`PostgresOutboxConfig(relation=("app", "outbox"))`):

```sql
CREATE TABLE app.outbox (
    id UUID PRIMARY KEY,
    outbox_route TEXT NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    tenant_id UUID,
    execution_id UUID,
    correlation_id UUID,
    causation_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    processing_at TIMESTAMPTZ,
    last_error TEXT,
    attempts INT NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ,
    ordering_key TEXT,
    UNIQUE (outbox_route, event_id)
);

-- covers the claim predicate (route + pending + ripe, oldest first)
CREATE INDEX outbox_claim_idx
    ON app.outbox (outbox_route, status, available_at, created_at);
```

`attempts` is the durable retry counter; `available_at` schedules the next
retry (`NULL` = claimable now); `ordering_key` is the optional delivery
partition key (`NULL` = no partitioning, key falls back to the event id).
Existing tables migrate with:

```sql
ALTER TABLE app.outbox
    ADD COLUMN attempts INT NOT NULL DEFAULT 0,
    ADD COLUMN available_at TIMESTAMPTZ,
    ADD COLUMN ordering_key TEXT;
```

### Trace propagation (optional column)

`PostgresOutboxConfig(propagate_trace=True)` persists each event's W3C `traceparent` so
the relay forwards it as the `traceparent` header and the consume side links its span to
the publish span. Opt in **after** adding the nullable column (legacy / `NULL` rows simply
carry no parent); independent of `hlc_ordering`:

```sql
ALTER TABLE app.outbox ADD COLUMN traceparent TEXT;
```

### Causal ordering (Hybrid Logical Clock)

By default claims are ordered by `created_at` — assigned per flush batch, so
rows staged together share a timestamp and tie arbitrarily, and clocks on
different replicas can disagree. Set `PostgresOutboxConfig(hlc_ordering=True)`
to claim in **causal** order instead: every event carries a Hybrid Logical
Clock stamp that stays close to wall time yet always exceeds any timestamp the
process has observed (including one merged from a consumed event's `forze_hlc`
header), so a reaction sorts after its cause across replicas, and the
time-ordered `id` breaks any remaining tie. Add the column first, then opt in
(legacy `NULL`-`hlc` rows fall back to `created_at`):

```sql
ALTER TABLE app.outbox ADD COLUMN hlc BIGINT;

-- claim order becomes (hlc NULLS LAST, created_at, id); index to match
CREATE INDEX outbox_claim_hlc_idx
    ON app.outbox (outbox_route, status, available_at, hlc, created_at, id);
```

#### Restart monotonicity (clock high-water mark)

The `hlc` column keeps causal order *within* a run, but a runtime's clock lives
in memory: a restart resets it to `(0, 0)`, after which it can re-issue a stamp
at or below one it already relayed whenever wall time had regressed or a peer
merge had carried the clock ahead. Persist the clock's high-water mark so a
restart resumes above its prior emissions. The table is optional and
node-global — one clock per runtime, spanning every tenant, so it is *not*
tenant-partitioned:

```sql
CREATE TABLE app.hlc_checkpoint (
    node_key TEXT   NOT NULL,
    hlc      BIGINT NOT NULL,   -- packed HlcTimestamp: physical_ms << 16 | logical
    PRIMARY KEY (node_key)
);
```

Wire it with `PostgresDepsModule(hlc_checkpoint=PostgresHlcCheckpointConfig(relation=("app", "hlc_checkpoint")))`
and add `hlc_checkpoint_recovery_lifecycle_step()` to the runtime's lifecycle.
For an outbox route that flushes inside a transaction
(`OutboxSpec(require_transaction=True)`), the flush then advances the mark (the max
HLC among the rows it stamps) *inside that transaction* — so a committed stamp is
never durable without a mark covering it, and a rolled-back flush never advances it —
while the lifecycle step loads the mark at startup and resumes the clock. A route that
flushes standalone keeps resuming from `(0, 0)`: its mark could not advance atomically
with its rows, so no checkpoint is wired for it. Unwired entirely, every route resumes
from `(0, 0)` as before. A single shared `node_key` (the default)
records one deployment-wide mark; distinct per-replica keys avoid write
contention on one row, and recovery reads the max across all keys either way.

### Consume storage

`PostgresOutboxConfig(storage="consume")` keeps only the rows still to relay. Each relay
pass runs in one transaction, and the table holds `pending` and `failed` rows only:

| Step | `retain` (default) | `consume` |
| --- | --- | --- |
| claim | `UPDATE` to `processing` | `SELECT … FOR UPDATE SKIP LOCKED`: a row lock, no write |
| published | `UPDATE` to `published` | `DELETE` |
| retried | `UPDATE` attempts, `available_at`, back to `pending` | the same, the row stays `pending` |
| parked | `UPDATE` to `failed` | the same |

A row that publishes first time is written twice, inserted and deleted, and never
updated, so nothing published is left for a retention job to prune. Postgres only: Mongo
keeps `retain`.

- **The indexes stay three:** the primary key, `UNIQUE (outbox_route, event_id)`, which
  staging deduplicates on, and the claim index. A retry changes `available_at`, which the
  claim index above includes, so a retry is not a HOT update under it. Retries are rare,
  and a row that publishes first time never updates.
- **The dedup window narrows.** Staging an event id again is deduplicated only while its
  row is pending. Once the row is relayed and deleted, the same event id publishes again.
- **A failed pass republishes its batch.** When a mark statement fails, a statement times
  out or the session is cut off, the whole pass rolls back. Every row it claimed is pending
  again, including the ones it already published: up to `max_claim_rows` (100)
  duplicates, where `retain` republishes at most one mark chunk of 32. Lower
  `max_claim_rows` for a smaller window.
- **A failing route backs off on its own.** The background relay step ends the failed pass
  and skips that route until it has waited: one `interval` after the first failure,
  doubling with each consecutive one up to `retry_max_backoff`, and a pass that commits
  resets it. On a step with `tenants=`, only the failing tenant waits; the others keep the
  step's cadence. A wake does not cut the wait short.
- **A pass is bounded.** It stops publishing at `pass_deadline` (default 5 s) and commits
  what it did; the rows it did not reach stay pending, untouched. A publish still running at
  the deadline is cancelled and counted as a failed attempt, though it may already have
  reached the broker, so that event can arrive twice. Each statement in the pass, the reclaim
  and the claim included, and each lock wait, is limited to `pass_statement_timeout`
  (default 1 s): on a large table without the claim index above, the claim times out and
  every pass fails. The server cuts the session off once it sits idle in the pass for
  longer than the two durations together. The pass sets these with `SET LOCAL`, and only when it opens its own
  transaction. A shutdown drain stops publishing a fifth of `shutdown_drain_timeout`
  before its end, so its last pass commits instead of being cut and rolled back.
- **A pass inside your transaction sets nothing.** Run a relay in a transaction of your own,
  for example to check a lease before committing, and the pass becomes a savepoint in it:
  your transaction's timeouts apply, and its rollback returns every row.
- **Staging can wait for a pass.** Staging an event id whose row a pass has deleted but not
  yet committed waits for that pass to end.
- **A destination on the same Postgres client joins the pass.** Its statements run in the
  pass's transaction, under `pass_statement_timeout`, and an SQL error there fails the
  whole pass.
- **A retry policy on the outbox route cannot save a pass.** A statement that fails aborts
  the pass's transaction, so every retry of it fails too; the pass fails and the step
  backs off.
- **Behind pgbouncer, use session or transaction pooling.** The timeouts are set with
  `set_config(..., true)`, which ends with the transaction, so transaction pooling is safe.
  Statement pooling cannot hold a transaction across statements, so it cannot run a pass.

`processing_at` and `published_at` are never written. To move a live table over:

1. Deploy the relays with `storage="consume"`, keeping `reclaim_stale_after` set. Their
   claims take `pending` rows, and reclaim still returns the `processing` rows `retain`
   left behind for the next pass to relay.
2. Leave the `published` rows to your existing retention job.
3. Once no `processing` row is left, set `reclaim_stale_after=None`. Once the retention job
   has removed every `published` row, retire it. Only then drop the two columns, and any
   index that exists only for them:

```sql
ALTER TABLE app.outbox DROP COLUMN processing_at, DROP COLUMN published_at;
```

Reclaim reads `processing_at`, so dropping it while reclaim is on fails every pass, and
the retention job reads `published_at`. Relays in both modes on one route stay
at-least-once: a row a `retain` relay claimed is `processing`, which a `consume` claim never
matches, until it is marked or reclaimed.

## Mongo

For Mongo (`MongoOutboxConfig`), documents mirror these fields; recommended
indexes:

```javascript
db.outbox.createIndex({ outbox_route: 1, event_id: 1 }, { unique: true })
db.outbox.createIndex({ outbox_route: 1, status: 1, available_at: 1, created_at: 1 })
db.outbox.createIndex({ outbox_route: 1, status: 1, processing_at: 1 })
// the relay reads each claimed batch back by its claim token
db.outbox.createIndex({ claim_token: 1 }, { sparse: true })
```

`hlc_ordering=True` works the same on Mongo: the packed HLC is stored on each
document and the claim sorts `[(hlc, 1), (created_at, 1), (id, 1)]`. No schema
migration is needed (documents are schemaless), but add an index to match, and
note that Mongo sorts missing-`hlc` rows *first* — so during migration legacy
rows drain oldest-first, the inverse of Postgres `NULLS LAST` (both best-effort):

```javascript
db.outbox.createIndex({ outbox_route: 1, status: 1, available_at: 1, hlc: 1, created_at: 1, id: 1 })
```

#### Restart monotonicity on Mongo

The high-water mark works the same way and needs **no index at all**: the write is
keyed on the document `_id`, and recovery reads one document per node. Wire it with
`MongoDepsModule(hlc_checkpoint=MongoHlcCheckpointConfig(collection=("app", "hlc_checkpoint")))`
and the same `hlc_checkpoint_recovery_lifecycle_step()`:

```javascript
// { _id: <node_key>, hlc: <packed HlcTimestamp> } — created on first advance.
```

The advance is one `$max` upsert, which is what `GREATEST` does in the Postgres
statement: the server keeps the larger value, so concurrent writers never lower the
mark. It rides the ambient session, so inside a transaction it commits — or rolls
back — with the rows it stamps. **Mongo transactions need a replica set**, so on a
standalone server an outbox route with `require_transaction=True` cannot run at all,
and with it the checkpoint has nothing to ride; the mark is only atomic where the
flush itself is.

Two operational notes that differ from Postgres:

- **Give each replica its own `node_key` when more than one flushes concurrently.**
  Two transactions advancing one document contend, and Mongo aborts one with a write
  conflict the flush must retry; Postgres serialises the same contention on a row lock.
- **Keep the keys stable and few** — one per replica, not one per boot. Recovery reads
  every key's document, so per-process keys turn a fixed handful into an unbounded scan.

The mark is written with one atomic compare-and-set on both backends, so two replicas
advancing it concurrently keep the higher value rather than one clobbering the other. That
is asserted rather than assumed: the shared battery forces a second writer into the middle
of a held write, which a read-modify-write implementation cannot survive.

The checkpoint also **cannot be wired on a routed (per-tenant) client**, on either
backend: the mark is node-global while a routed client picks its backend from the bound
tenant, which startup recovery does not have. Both modules refuse that combination at
wiring rather than at the first read.

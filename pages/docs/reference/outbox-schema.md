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
The outbox flush then advances the mark (the max HLC among the rows it stamps)
*inside the business transaction* — so a committed stamp is never durable without
a mark covering it, and a rolled-back flush never advances it — while the
lifecycle step loads the mark at startup and resumes the clock. Unwired, the
clock resumes from `(0, 0)` as before. A single shared `node_key` (the default)
records one deployment-wide mark; distinct per-replica keys avoid write
contention on one row, and recovery reads the max across all keys either way.

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

---
title: Durable execution schema
icon: lucide/table
summary: The application-owned durable run, step and schedule tables — columns, indexes and migrations, for Postgres and Mongo
---

The durable-execution tables are application-owned: you create and migrate them. This is
the schema the run store, the step journal and the scheduler read and write. The wiring is
in [Durable execution](../data-events/durable-execution.md).

## Postgres

### Runs

For `PostgresDurableRunConfig(relation=("public", "durable_run"))`:

```sql
CREATE TABLE public.durable_run (
    run_id              text        NOT NULL,
    name                text        NOT NULL,
    status              text        NOT NULL,
    idempotency_key     text,
    coalesce_key        text,
    input               jsonb,
    output              jsonb,
    error               text,
    tenant_id           uuid,
    attempts            integer     NOT NULL DEFAULT 0,
    leased_until        timestamptz,
    available_at        timestamptz,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    cancel_requested_at timestamptz,
    cancel_refused_at   timestamptz,
    PRIMARY KEY (run_id),
    UNIQUE (idempotency_key)
);

CREATE UNIQUE INDEX ON public.durable_run (coalesce_key) WHERE status = 'pending' AND attempts = 0;
```

- `UNIQUE (idempotency_key)` makes two submits of one key converge on a single run. The
  key is stored tenant-scoped, so two tenants reusing a key on a shared table stay
  distinct runs.
- The partial unique index on `coalesce_key` admits one unclaimed `pending` run per key,
  which `enqueue(coalesce_key=...)` joins. It is stored tenant-scoped too.
- `attempts` is the fence token: each claim advances it under a row lock, and a terminal
  write is fenced against a reclaimed lease.
- `available_at` delays when a `pending` run may be claimed (`NULL`: claimable now).

Recommended indexes:

```sql
-- the recovery scan (claim_abandoned) filters on status, available_at and leased_until,
-- and orders by created_at under FOR UPDATE SKIP LOCKED
CREATE INDEX ON public.durable_run (status, created_at);
-- on a shared tagged table a bound scan also filters tenant_id
CREATE INDEX ON public.durable_run (tenant_id, status, created_at);

-- the admin listing (list_runs) orders newest first and seeks on (created_at, run_id)
CREATE INDEX ON public.durable_run (created_at DESC, run_id DESC);
-- on a shared tagged table a bound listing also filters tenant_id
CREATE INDEX ON public.durable_run (tenant_id, created_at DESC, run_id DESC);
```

Without them, the recovery scan and the listing scan and sort the whole table as it grows.
The retention purge (`purge_finished`) filters on `status` and `created_at`, oldest first, so
it uses the first index too.

### Steps

For `PostgresDurableStepConfig(relation=("public", "durable_step"))`:

```sql
CREATE TABLE public.durable_step (
    run_id     text        NOT NULL,
    step_id    text        NOT NULL,
    result     jsonb       NOT NULL,
    tenant_id  uuid,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, step_id)
);
```

The primary key is the journal's dedup key, so it needs no other index; a purge deletes a
run's steps by `run_id`, which the key leads with.

### Schedules

For `PostgresDurableScheduleConfig(relation=("public", "durable_schedule"))`:

```sql
CREATE TABLE public.durable_schedule (
    schedule_id  text        NOT NULL,
    name         text        NOT NULL,
    cron         text        NOT NULL,
    tz           text,
    input        jsonb,
    next_fire_at timestamptz NOT NULL,
    enabled      boolean     NOT NULL DEFAULT true,
    tenant_id    uuid,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    PRIMARY KEY (schedule_id)
);
```

On a shared tagged table, `schedule_id` is stored as `{tenant_id}:{schedule_id}`, so two
tenants registering one id stay distinct schedules.

Recommended indexes:

```sql
-- claim_due scans enabled schedules due now, oldest first, under FOR UPDATE SKIP LOCKED
CREATE INDEX ON public.durable_schedule (next_fire_at) WHERE enabled;
-- on a shared tagged table a bound scheduler also filters tenant_id
CREATE INDEX ON public.durable_schedule (tenant_id, next_fire_at) WHERE enabled;
```

### Migrations

**Coalescing enqueue needs a column and an index.** Only `enqueue(coalesce_key=...)` reads
them, so add both before the first such call; nothing else changes:

```sql
ALTER TABLE public.durable_run ADD COLUMN coalesce_key text;
CREATE UNIQUE INDEX CONCURRENTLY ON public.durable_run (coalesce_key)
    WHERE status = 'pending' AND attempts = 0;
```

**0.6.0 added run cancellation.** Both cancel columns are nullable with no default, so an
existing `durable_run` table migrates with no rewrite:

```sql
ALTER TABLE public.durable_run
    ADD COLUMN cancel_requested_at timestamptz,
    ADD COLUMN cancel_refused_at   timestamptz;
```

!!! warning "Migrate before you deploy"
    Starting a run, the recovery scan and every read return both columns, so against the
    old schema each one raises `UndefinedColumn` and no body starts. A new enqueue still
    inserts, so its run waits `pending` until the migration lands and the recovery scan
    claims it; a re-submit of an existing key fails like a read.

## Mongo

For `MongoDurableRunConfig`, `MongoDurableStepConfig` and `MongoDurableScheduleConfig`,
documents mirror the Postgres columns, with two differences: `tenant_id` is a string, not
a UUID, and the key is `_id`. A run stores its `run_id` there and has no `run_id` field; a
schedule stores its tenant-scoped `schedule_id` there. A run also carries `claim_token`,
which the batch claim reads back.

The run collection needs one index for correctness:

```javascript
// Two simultaneous submits of one key must converge on one run. Partial, because a run
// without a key must not collide with another.
db.durable_run.createIndex(
    { idempotency_key: 1 },
    { unique: true, partialFilterExpression: { idempotency_key: { $type: "string" } } },
)
```

Coalescing (`enqueue(coalesce_key=...)`) needs one more, so a burst of submits joins one
unclaimed run rather than racing to insert several:

```javascript
db.durable_run.createIndex(
    { coalesce_key: 1 },
    {
        unique: true,
        partialFilterExpression: {
            coalesce_key: { $type: "string" },
            status: "pending",
            attempts: 0,
        },
    },
)
```

Recommended indexes keep the recovery scan, the listing and the scheduler off collection
scans as the collections grow:

```javascript
db.durable_run.createIndex({ status: 1, created_at: 1 })
db.durable_run.createIndex({ created_at: -1, _id: -1 })
// on a shared tagged collection a bound scan and listing also filter tenant_id
db.durable_run.createIndex({ tenant_id: 1, status: 1, created_at: 1 })
db.durable_run.createIndex({ tenant_id: 1, created_at: -1, _id: -1 })
// sparse: only a run some scan has claimed carries a token
db.durable_run.createIndex({ claim_token: 1 }, { sparse: true })

db.durable_schedule.createIndex({ enabled: 1, next_fire_at: 1 })
// on a shared tagged collection a bound scheduler also filters tenant_id
db.durable_schedule.createIndex({ tenant_id: 1, enabled: 1, next_fire_at: 1 })
```

The step journal needs no index for correctness: its `_id` composes the run and the step,
so a concurrent duplicate of one step serialises on the primary key. Where runs are purged,
index `run_id`, which the purge deletes a run's steps by:

```javascript
db.durable_step.createIndex({ run_id: 1 })
```

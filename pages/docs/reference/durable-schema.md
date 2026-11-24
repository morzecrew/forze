---
title: Durable execution schema
icon: lucide/table
summary: The application-owned durable run, step and schedule tables — columns and indexes, for Postgres and Mongo
---

The durable-execution tables are application-owned: you apply their DDL with your
migration tool. This is the schema the run store, the step journal and the scheduler read
and write. The wiring is in [Durable execution](../data-events/durable-execution.md).

## Postgres

Forze declares each Postgres table once, in code. `forze schema sql` prints the statements
below for the relations your `PostgresDepsModule` names, and `forze schema diff` prints
what an existing database is missing. [Upgrading forze's tables](../running-in-prod/upgrading-forze-tables.md)
walks through both. The SQL on this page is that render, for a tenant-aware table named
`public.durable_*`, and a test keeps it so.

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
    PRIMARY KEY (run_id)
);

CREATE UNIQUE INDEX ON public.durable_run (idempotency_key);
CREATE UNIQUE INDEX ON public.durable_run (coalesce_key) WHERE status = 'pending' AND attempts = 0;
```

- The unique index on `idempotency_key` makes two submits of one key converge on a single
  run. A table created with a `UNIQUE (idempotency_key)` constraint has the same index, and
  `forze schema diff` reads it as present.
- The partial unique index on `coalesce_key` admits one unclaimed `pending` run per key,
  which `enqueue(coalesce_key=...)` joins. Only that call reads the column.
- Both keys are stored as `{tenant_id}:{key}`, or `:{key}` untagged, so no key reaches
  another tenant's run. Records return the key as the caller passed it.
- `attempts` is the fence token: each claim advances it under a row lock, and a terminal
  write is fenced against a reclaimed lease.
- `available_at` delays when a `pending` run may be claimed (`NULL`: claimable now).

Recommended indexes:

```sql
-- the recovery scan (claim_abandoned) filters on status, available_at and leased_until,
-- and on name for a worker that claims only some functions, and orders by created_at
-- under FOR UPDATE SKIP LOCKED
CREATE INDEX ON public.durable_run (status, name, created_at);
-- on a shared tagged table a bound scan also filters tenant_id
CREATE INDEX ON public.durable_run (tenant_id, status, name, created_at);

-- where runs are purged: purge_finished filters on status and updated_at (when the run
-- landed) and deletes the oldest landings first
CREATE INDEX ON public.durable_run (status, updated_at);

-- the admin listing (list_runs) orders newest first and seeks on (created_at, run_id)
CREATE INDEX ON public.durable_run (created_at DESC, run_id DESC);
-- on a shared tagged table a bound listing also filters tenant_id
CREATE INDEX ON public.durable_run (tenant_id, created_at DESC, run_id DESC);
```

Without them, the recovery scan, the purge and the listing scan and sort the whole table
as it grows. `forze schema` declares the `tenant_id` variants for a tenant-aware table on a
static relation. The store filters a bound scan by tenant whether or not the table is
tenant-aware, so if you bind tenants without `tenant_aware=True`, create the variants
yourself; the diff leaves an index it does not declare alone. A table in a per-tenant
schema needs none.

The two recovery-scan indexes replaced `(status, created_at)` and
`(tenant_id, status, created_at)` in 0.18.0. A scan of every function costs the same on
either: neither lets the claim read in `created_at` order, since it selects `pending` and
`running` rows and sorts them. On the old ones, a worker that claims only some functions
would read every pending run of the others on each scan. `forze schema diff` creates the
new indexes before it drops the old ones, under whatever names your database gave them.

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

`schedule_id` is stored as `{tenant_id}:{schedule_id}`, or `:{schedule_id}` untagged, so
two tenants registering one id stay distinct schedules and no id reaches another tenant's
schedule. Records return the id as the caller registered it.

Recommended indexes:

```sql
-- claim_due scans enabled schedules due now, oldest first, under FOR UPDATE SKIP LOCKED
CREATE INDEX ON public.durable_schedule (next_fire_at) WHERE enabled;
-- on a shared tagged table a bound scheduler also filters tenant_id
CREATE INDEX ON public.durable_schedule (tenant_id, next_fire_at) WHERE enabled;
```

!!! warning "Migrate before you deploy"
    The run store names every column but `coalesce_key` on its common paths. Against a
    table missing one, starting a run, the recovery scan or a read can raise
    `UndefinedColumn`, and no body starts. Apply `forze schema diff`'s output before the
    forze version that needs it reaches production. Only `enqueue(coalesce_key=...)` names
    `coalesce_key`, so a table without it serves every other call.

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
// a worker that claims only some functions also filters name; these do not replace the
// ones above, which a scan of every function reads in created_at order
db.durable_run.createIndex({ status: 1, name: 1, created_at: 1 })
db.durable_run.createIndex({ tenant_id: 1, status: 1, name: 1, created_at: 1 })
// where runs are purged: status and updated_at (when the run landed), oldest first
db.durable_run.createIndex({ status: 1, updated_at: 1 })
// sparse: only a run some scan has claimed carries a token
db.durable_run.createIndex({ claim_token: 1 }, { sparse: true })

db.durable_schedule.createIndex({ enabled: 1, next_fire_at: 1 })
// on a shared tagged collection a bound scheduler also filters tenant_id
db.durable_schedule.createIndex({ tenant_id: 1, enabled: 1, next_fire_at: 1 })
```

The step journal needs no index for correctness: its `_id` composes the run and the step,
so a concurrent duplicate of one step serialises on the primary key. Where runs are purged,
index `run_id`, by which the purge marks, deletes and unmarks a run's steps, and the
`purge_pending` mark, by which the next purge finds the steps of runs an interrupted one
deleted:

```javascript
db.durable_step.createIndex({ run_id: 1 })
db.durable_step.createIndex({ purge_pending: 1 }, { sparse: true })
```

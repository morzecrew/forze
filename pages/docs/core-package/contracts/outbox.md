# Outbox contracts

Transactional outbox for **integration events**: stage in the same database transaction as your writes, flush in one batch, then relay to a queue.

## `OutboxSpec`

| Field | Purpose |
|-------|---------|
| `name` | Logical route (deps key route) |
| `codec` | `RecordMappingCodec` for payload models |
| `destination` | Optional `OutboxDestination(queue_route, queue)` for `relay_outbox_to_queue` |

## Ports

### `OutboxCommandPort`

| Method | Purpose |
|--------|---------|
| `stage` / `stage_many` / `stage_event` | Buffer events (in-memory per request via `forze.application.integrations.outbox.OutboxStaging`) |
| `flush` | Persist buffered rows; returns count of **new** rows inserted |

Wire **tx `on_success`** to flush before commit:

```python
from forze.application.composition.outbox import outbox_flush_tx_on_success_factory
from forze.application.contracts.execution import OnSuccessStep

registry.patch("my.op").bind_tx().set_route("postgres").on_success(
    OnSuccessStep(id="outbox_flush", factory=outbox_flush_tx_on_success_factory(events_spec))
)
```

#### Idempotency

- Duplicate `(outbox_route, event_id)` on flush is a **no-op** (Postgres `ON CONFLICT DO NOTHING`; mock skips silently).
- `flush()` return value counts only rows actually inserted.
- Rows in `published` or `failed` still occupy the unique key—re-drive with a **new** `event_id` or operational cleanup (delete/update the row).

### `OutboxQueryPort`

| Method | Purpose |
|--------|---------|
| `claim_pending` | `FOR UPDATE SKIP LOCKED` → `processing`, sets `processing_at` |
| `reclaim_stale_processing` | Reset stuck `processing` rows to `pending` when `processing_at` is older than a cutoff |
| `mark_published` | After successful enqueue (only from `processing`) |
| `mark_failed` | On relay errors (only from `processing`) |

## Relay

```python
from datetime import timedelta

from forze.application.composition.outbox import relay_outbox_to_queue

result = await relay_outbox_to_queue(
    ctx,
    outbox_spec=events_spec,
    queue_spec=jobs_spec,
    reclaim_stale_after=timedelta(minutes=5),  # None to disable
)
# result.claimed, .published, .failed, .reclaimed
```

Delivery is **at-least-once**: enqueue and `mark_published` are not atomic. Queue consumers should deduplicate on `event_id`.

Run from a worker or cron after HTTP handlers return. See [Transactional outbox recipe](../../recipes/transactional-outbox.md).

## Postgres DDL (application-owned)

```sql
CREATE TABLE my_schema.outbox (
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
    UNIQUE (outbox_route, event_id)
);
```

Existing tables: add reclaim support with:

```sql
ALTER TABLE my_schema.outbox ADD COLUMN IF NOT EXISTS processing_at TIMESTAMPTZ;
```

Register on `PostgresDepsModule(outboxes={...})` with `PostgresOutboxConfig(relation=("my_schema", "outbox"))`. `default_processing_lease` documents a suggested `reclaim_stale_after` for workers.

## Related pages

- [Queue contracts](queue.md)
- [Operation composition](../../concepts/operation-composition.md)
- [Transactional outbox recipe](../../recipes/transactional-outbox.md)

# Outbox contracts

Transactional outbox for **integration events**: stage in the same database transaction as your writes, flush in one batch, then relay to a queue.

## `OutboxSpec`

| Field | Purpose |
|-------|---------|
| `name` | Logical route (deps key route) |
| `codec` | `ModelCodec` for payload models |
| `destination` | Optional `OutboxDestination` (`queue`, `stream`, or `pubsub`) for relay helpers |

## Ports

### `OutboxCommandPort`

| Method | Purpose |
|--------|---------|
| `stage` / `stage_many` / `stage_event` | Buffer events (in-memory per request via `forze.application.integrations.outbox.OutboxStaging`) |
| `flush` | Persist buffered rows; returns count of **new** rows inserted |

Wire **tx `on_success`** to flush before commit:

```python
from forze_kits.integrations.outbox import outbox_flush_tx_on_success_factory
from forze.application.contracts.execution import OnSuccessStep

registry.patch("my.op").bind_tx().set_route("postgres").on_success(
    OnSuccessStep(id="outbox_flush", factory=outbox_flush_tx_on_success_factory(events_spec))
)
```

Use `.set_route("mongo")` with `MongoTxScopeKey` when flushing via `MongoDepsModule`.

#### Idempotency

- Duplicate `(outbox_route, event_id)` on flush is a **no-op** (Postgres `ON CONFLICT DO NOTHING`; Mongo unique index; mock skips silently).
- `flush()` return value counts only rows actually inserted.
- Rows in `published` or `failed` still occupy the unique key—re-drive with `requeue_failed`, a **new** `event_id`, or operational cleanup.

### `OutboxQueryPort`

| Method | Purpose |
|--------|---------|
| `claim_pending` | Claim batch → `processing`, sets `processing_at` |
| `reclaim_stale_processing` | Reset stuck `processing` rows to `pending` when `processing_at` is older than a cutoff |
| `mark_published` | After successful enqueue (only from `processing`) |
| `mark_failed` | On relay errors (only from `processing`) |
| `requeue_failed` | Reset `failed` rows to `pending` for re-drive |

## `OutboxDestination`

Discriminated relay target on :class:`OutboxSpec`:

| `kind` | Factory | Relay function |
|--------|---------|----------------|
| `queue` | `OutboxDestination.queue(route=..., channel=...)` | `relay_outbox_to_queue` |
| `stream` | `OutboxDestination.stream(route=..., channel=...)` | `relay_outbox_to_stream` |
| `pubsub` | `OutboxDestination.pubsub(route=..., channel=...)` | `relay_outbox_to_pubsub` |

- `route` must match the registered spec name (`QueueSpec.name`, `StreamSpec.name`, or `PubSubSpec.name`).
- `channel` is the logical queue name, stream name, or pub/sub topic.

Use :func:`~forze_kits.integrations.outbox.relay_outbox` to dispatch from `outbox_spec.destination.kind`.

## Relay

```python
from datetime import timedelta

from forze_kits.integrations.outbox import relay_outbox, relay_outbox_to_queue

result = await relay_outbox_to_queue(
    ctx,
    outbox_spec=events_spec,
    queue_spec=jobs_spec,
    reclaim_stale_after=timedelta(minutes=5),  # None to disable
)
# result.claimed, .published, .failed, .reclaimed

# Or dispatch from OutboxSpec.destination:
result = await relay_outbox(
    ctx,
    outbox_spec=events_spec,
    queue_spec=jobs_spec,
    reclaim_stale_after=timedelta(minutes=5),
)
```

Delivery is **at-least-once**: each claim is published with `type=event_type` and `key=str(event_id)` when the backend supports metadata. Publish and `mark_published` are not atomic.

For notifications after relay, see [Transactional notifications](../../recipes/transactional-notifications.md).

Optional in-process polling (long-running apps only):

```python
from forze_kits.integrations.outbox import outbox_relay_background_lifecycle_step
```

Run from a worker, cron, or background lifecycle step. See [Transactional outbox recipe](../../recipes/transactional-outbox.md).

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

Register on `PostgresDepsModule(outboxes={...})` with `PostgresOutboxConfig(relation=("my_schema", "outbox"))`.

## Mongo collection (application-owned)

Register on `MongoDepsModule(outboxes={...})` with `MongoOutboxConfig(collection=("my_db", "outbox"))`. **Transactions require a replica set.**

Recommended indexes:

```javascript
db.outbox.createIndex({ outbox_route: 1, event_id: 1 }, { unique: true })
db.outbox.createIndex({ outbox_route: 1, status: 1, created_at: 1 })
db.outbox.createIndex({ outbox_route: 1, status: 1, processing_at: 1 })
```

Document fields mirror the Postgres table (`id`, `outbox_route`, `event_id`, envelope ids, `payload`, `status`, timestamps, `last_error`).

## Related pages

- [Queue contracts](queue.md)
- [Mongo integration](../../integrations/mongo.md)
- [Operation composition](../../concepts/operation-composition.md)
- [Transactional outbox recipe](../../recipes/transactional-outbox.md)

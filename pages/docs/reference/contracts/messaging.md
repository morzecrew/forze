---
title: Queue, outbox & inbox ports
icon: lucide/inbox
summary: Methods on the queue, outbox, and inbox contracts
---

## Queue

The queue contract has no short `ctx.*` accessor — resolve the ports by dep key:

```python
from forze.application.contracts.queue import QueueQueryDepKey, QueueCommandDepKey

w = ctx.deps.resolve_configurable(ctx, QueueCommandDepKey, spec, route=spec.name)
r = ctx.deps.resolve_configurable(ctx, QueueQueryDepKey, spec, route=spec.name)
```

### Command port

| Method | Signature |
|--------|-----------|
| `enqueue` | `enqueue(queue, payload, *, type=None, key=None, enqueued_at=None, delay=None, not_before=None)` → message id |
| `enqueue_many` | `enqueue_many(queue, payloads, *, type=None, key=None, enqueued_at=None, delay=None, not_before=None)` |

`delay` (a `timedelta`) and `not_before` (a tz-aware `datetime`) are mutually
exclusive — see [Scheduled & delayed jobs](../../recipes/scheduled-queue-jobs.md).

### Query port

| Method | Signature | Notes |
|--------|-----------|-------|
| `receive` | `receive(queue, *, limit=None, timeout=None)` | a batch of `QueueMessage` |
| `consume` | `consume(queue, *, timeout=None)` | async generator until `timeout` |
| `ack` | `ack(queue, ids)` | acknowledge; returns count |
| `nack` | `nack(queue, ids, *, requeue=True)` | negative-ack, optionally requeue |

## Outbox

`ctx.outbox.command(spec)` stages and flushes; `ctx.outbox.query(spec)` drives the
relay. See [Transactional outbox](../../recipes/transactional-outbox.md).

### Command port

| Method | Signature | Notes |
|--------|-----------|-------|
| `stage` | `stage(event_type, payload, *, event_id=None, occurred_at=None)` | buffer one event |
| `stage_many` | `stage_many(events, *, event_ids=None)` | buffer `(event_type, payload)` pairs |
| `stage_event` | `stage_event(event)` | buffer a built `IntegrationEvent` |
| `flush` | `flush()` | persist buffered events; returns rows inserted |

### Query port

| Method | Signature | Notes |
|--------|-----------|-------|
| `claim_pending` | `claim_pending(*, limit=None)` | claim a batch for relay; skips rows whose `available_at` is in the future |
| `mark_published` | `mark_published(ids)` | mark relayed |
| `mark_failed` | `mark_failed(ids, *, error=None)` | mark terminally failed (operator re-drives) |
| `mark_retry` | `mark_retry(ids, *, attempts, available_at, error=None)` | reschedule for a future retry with the durable attempt counter |
| `reclaim_stale_processing` | `reclaim_stale_processing(*, older_than)` | reset stuck rows to pending |
| `requeue_failed` | `requeue_failed(ids)` | re-drive failed rows; resets `attempts` to 0 |

(Most apps use the `relay_outbox_to_queue` kit rather than these directly.)
Delivery is at-least-once and ordering is not preserved across
failures/retries — consumers key on `event_id` and tolerate reordering.

## Inbox

`ctx.inbox(spec)` returns an `InboxPort` with a single method — the consumer-side
exactly-once primitive:

| Method | Signature | Notes |
|--------|-----------|-------|
| `mark_if_unseen` | `mark_if_unseen(inbox, message_id)` | `True` if newly recorded (process), `False` if already seen (skip) |

Call it inside the handler's transaction so the dedup mark and the handler's
writes commit together. In practice use the `process_with_inbox` kit, which does
exactly that — see [Events & sagas](../../in-depth/events-sagas.md).

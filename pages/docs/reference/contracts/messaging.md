---
title: Queue, outbox & inbox
icon: lucide/inbox
summary: The queue, outbox, and inbox contracts — their specs and methods
---

The three messaging contracts that carry events between services: the **queue**
(produce / consume), the **outbox** (stage events in a transaction, relay them
at-least-once), and the **inbox** (consumer-side exactly-once dedup). How they compose is
[Events & sagas](../../data-events/events-sagas.md).

## Queue

The queue contract has no short `ctx.*` accessor — resolve the ports by dep key:

```python
from forze.application.contracts.queue import QueueQueryDepKey, QueueCommandDepKey

w = ctx.deps.resolve_configurable(ctx, QueueCommandDepKey, spec, route=spec.name)
r = ctx.deps.resolve_configurable(ctx, QueueQueryDepKey, spec, route=spec.name)
```

`QueueSpec` carries the payload `codec` and an `encryption` tier (`none` / `end_to_end`,
where `end_to_end` seals the payload through the broker).

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

`OutboxSpec` fields:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | route name |
| `codec` | `ModelCodec` | required | staged integration-event payload codec |
| `destination` | `OutboxDestination \| None` | `None` | default relay target — `.queue` / `.stream` / `.pubsub(route, channel)` |
| `encryption` | `OutboxEncryptionTier` | `"none"` | whole-payload tier: `none` · `at_rest` (relay decrypts before publish) · `end_to_end` (consumer decrypts) — see [encryption](../../identity-tenancy-enc/encryption.md) |

### Command port

| Method | Signature | Notes |
|--------|-----------|-------|
| `stage` | `stage(event_type, payload, *, event_id=None, occurred_at=None, ordering_key=None)` | buffer one event; `ordering_key` partitions delivery on capable transports |
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
Delivery is at-least-once and ordering is not guaranteed across
failures/retries — a retrying row never stalls later rows of its
`ordering_key`. Staging an `ordering_key` makes same-key events partition
together (SQS FIFO `MessageGroupId`, stream partition key) and relay in
`created_at` order on the happy path; consumers still dedupe on `event_id`
(the `forze_event_id` header) and tolerate reordering.

## Inbox

`ctx.inbox(spec)` returns an `InboxPort` with a single method — the consumer-side
exactly-once primitive (`InboxSpec` carries a dedup-window `ttl`, default 7 days):

| Method | Signature | Notes |
|--------|-----------|-------|
| `mark_if_unseen` | `mark_if_unseen(inbox, message_id)` | `True` if newly recorded (process), `False` if already seen (skip) |

Call it inside the handler's transaction so the dedup mark and the handler's
writes commit together. In practice use the `process_with_inbox` kit, which does
exactly that — see [Events & sagas](../../data-events/events-sagas.md).

## Implemented by

| Contract | Backend | Integration |
|----------|---------|-------------|
| Queue | RabbitMQ, SQS | [RabbitMQ](../../integrations/rabbitmq.md) · [SQS](../../integrations/sqs.md) |
| Outbox | Postgres, Mongo (store) → any transport | [Postgres](../../integrations/postgres.md) · [Mongo](../../integrations/mongo.md) |
| Inbox | Postgres, Mongo | [Postgres](../../integrations/postgres.md) · [Mongo](../../integrations/mongo.md) |

The table layout (with the optional HLC ordering column) is the
[outbox schema](../outbox-schema.md).

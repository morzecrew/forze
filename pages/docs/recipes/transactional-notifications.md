---
title: Transactional notifications
icon: lucide/mail
summary: Reliable email / push / webhook — staged with the write, routed to a sender by the notify kit
---

A "welcome email" that fires on signup shouldn't send if the signup rolled back,
and shouldn't be lost if it committed. That's the [outbox](transactional-outbox.md)
guarantee again — plus a small kit that **routes** each relayed event to the
right notification and hands it to a **sender**.

The runnable version lives at `examples/recipes/notifications/` (mock — no broker
or SMTP needed).

## Stage the notification

The producer stages an event exactly like the outbox recipe — the notification is
just the integration event's purpose:

```python
--8<-- "recipes/notifications/app.py:event"
```

## Route events to notifications

A `NotificationRouter` registers a mapper per event type, then `freeze()`s into an
immutable `FrozenNotificationRouter` the consumer resolves against. Registration and
resolution are separate — the routing table is fixed once frozen, so it can't change
under a running consumer. The mapper receives the integration event, so it reads the
payload:

```python
--8<-- "recipes/notifications/app.py:router"
```

`EmailNotification`, `PushNotification`, and `WebhookNotification` are the shipped
shapes.

## Provide senders

`NotificationSenders` is a protocol — any object with `send_email` / `send_push`
/ `send_webhook` satisfies it. Real senders wrap SMTP, FCM, or an HTTP client;
here it just records:

```python
--8<-- "recipes/notifications/app.py:senders"
```

## Consume and dispatch

`OutboxRelay(...).to_queue(...)` publishes the staged events to the queue; a
`QueueConsumer` then drains that queue, routing each message through the frozen router to
the matching sender. Going through the consumer (rather than a hand-rolled receive/ack
loop) gives **inbox dedup** — an at-least-once redelivery won't re-send — and poison
parking, for free. In production wire the consumer as a background step with
`notification_consumer_lifecycle_step`; the example drains once:

```python
--8<-- "recipes/notifications/app.py:consume"
```

## Notes

- The dedup key is the relayed event's deterministic id (`forze_event_id` header,
  else `key`, else `<queue>:<message.id>`), so a redelivered message is processed
  once even though delivery is at-least-once.
- `notification_queue_consumer_handler` adapts `process_notification_message` (which
  takes a **`QueueMessage`**, not a raw payload) to the consumer's handler signature.
- Unmapped event types are skipped by default (`skip_unmapped=True`).
- The producer and consumer are decoupled: they can be different processes, and
  the consumer is just a queue worker.

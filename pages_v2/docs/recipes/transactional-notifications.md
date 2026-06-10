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

A `NotificationRouter` maps each event type to the notifications it should
produce. The mapper receives the integration event, so it reads the payload:

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

The consumer relays the staged events to the queue, then feeds each message to
`process_notification_message`, which resolves it through the router and calls the
matching sender:

```python
--8<-- "recipes/notifications/app.py:consume"
```

## Notes

- `process_notification_message` takes a **`QueueMessage`** (not a raw payload) —
  the event type and id come from the relayed message's `type` and `key`.
- Unmapped event types are skipped by default (`skip_unmapped=True`).
- The producer and consumer are decoupled: they can be different processes, and
  the consumer is just a queue worker.

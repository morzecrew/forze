# Transactional notifications

Send email, push, or webhooks reliably using the **transactional outbox**, **queue relay**, and **`forze_kits.integrations.notify`**—without a core `NotificationPort`.

## Checklist

1. Define integration event payloads and `OutboxSpec` with `OutboxDestination.queue(...)`.
2. Register outbox store + `QueueSpec` on deps modules.
3. Patch mutating operations with `outbox_flush_tx_on_success_factory`.
4. Run `relay_outbox_to_queue` (or `relay_outbox`) from a worker.
5. Consume the queue with `process_notification_message` and app `NotificationSenders`.

## Event and outbox

```python
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.base.serialization import PydanticModelCodec

class ProjectCreated(BaseModel):
    project_id: str

events_spec = OutboxSpec(
    name="events",
    codec=PydanticModelCodec(ProjectCreated),
    destination=OutboxDestination.queue(route="notifications", channel="notifications"),
)
notifications_spec = QueueSpec(name="notifications", codec=events_spec.codec)
```

Stage in a handler (same transaction as writes):

```python
await ctx.outbox.command(events_spec).stage(
    "project.created",
    ProjectCreated(project_id=str(project.id)),
)
```

Wire flush on the transaction route (see [Transactional outbox](transactional-outbox.md)).

## Relay

```python
from forze_kits.integrations.outbox import relay_outbox_to_queue

await relay_outbox_to_queue(
    ctx,
    outbox_spec=events_spec,
    queue_spec=notifications_spec,
)
```

## Routing and senders

```python
from forze_kits.integrations.notify import (
    EmailNotification,
    NotificationRouter,
    NotificationSenders,
    process_notification_message,
)

router = NotificationRouter()
router.register(
    "project.created",
    lambda event: [
        EmailNotification(
            to="owner@example.com",
            subject="Project created",
            body=f"Project {event.payload.project_id} is ready.",
        )
    ],
)

class AppNotificationSenders(NotificationSenders):
    async def send_email(self, notification: EmailNotification) -> None:
        ...  # SendGrid, SMTP, etc.

    async def send_push(self, notification) -> None:
        ...

    async def send_webhook(self, notification) -> None:
        ...

senders = AppNotificationSenders()
```

## Worker

```python
async for message in ctx.deps.resolve_configurable(
    ctx, QueueQueryDepKey, notifications_spec, route=notifications_spec.name
).consume("notifications"):
    await process_notification_message(message, router=router, senders=senders)
    await query.ack("notifications", [message.id])
```

Implement **idempotent** senders or deduplicate on `message.key` (`event_id` from outbox relay) because delivery is at-least-once.

## Live fan-out (optional)

For WebSocket or SSE subscribers, either:

- **Second outbox** with `OutboxDestination.pubsub(...)` and `relay_outbox_to_pubsub`, or
- Publish from the notification worker after successful send (`PubSubCommandPort.publish`).

Prefer **queue workers** for email/push/webhook retries; use **pub/sub** or **streams** for broadcast or ordered logs (see [Stream contracts](../core-package/contracts/stream.md)).

## Related

- [Transactional outbox](transactional-outbox.md)
- [Outbox contracts](../core-package/contracts/outbox.md)
- [Queue contracts](../core-package/contracts/queue.md)
- [Kits reference](../reference/kits.md)

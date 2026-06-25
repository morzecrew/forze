"""Recipe: transactional notifications — stage a notification, relay, route to a sender.

Reliable email/push/webhook: the notification intent is staged in the outbox with
the business write, relayed to a queue, and a consumer routes each message to the
right sender. Builds on the transactional-outbox recipe. Mock-runnable.

Run it:  uv run python -m examples.recipes.notifications.app
Exercised by tests/unit/test_examples/test_notifications.py.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

from pydantic import BaseModel

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.consumer import QueueConsumer
from forze_kits.integrations.notify import (
    EmailNotification,
    NotificationRouter,
    notification_queue_consumer_handler,
)
from forze_kits.integrations.outbox import OutboxRelay
from forze_mock import MockDepsModule


# --8<-- [start:event]
class UserRegistered(BaseModel):
    email: str


NOTIFY_EVENTS = OutboxSpec(
    name="notify-events",
    codec=PydanticModelCodec(UserRegistered),
    destination=OutboxDestination.queue(route="notifications", channel="notifications"),
)
NOTIFICATIONS = QueueSpec(
    name="notifications", codec=PydanticModelCodec(UserRegistered)
)
# --8<-- [end:event]

# Dedup store for the consumer: a redelivered message is marked seen and skipped,
# so an at-least-once queue cannot send the same notification twice.
NOTIFY_INBOX = InboxSpec(name="notify-inbox")


# --8<-- [start:router]
# Map each integration event type to the notifications it should produce, then freeze:
# registration happens once at wiring time; the consumer holds an immutable resolver.
router = (
    NotificationRouter()
    .register(
        "user.registered",
        lambda event: [
            EmailNotification(
                to=event.payload.email, subject="Welcome", body="Thanks for joining!"
            )
        ],
    )
    .freeze()
)
# --8<-- [end:router]


# --8<-- [start:senders]
class RecordingSenders:
    """A NotificationSenders implementation — here it just records what it sent."""

    def __init__(self) -> None:
        self.emails: list[EmailNotification] = []

    async def send_email(self, notification: EmailNotification) -> None:
        self.emails.append(notification)

    async def send_push(self, notification: object) -> None: ...
    async def send_webhook(self, notification: object) -> None: ...


# --8<-- [end:senders]


async def stage_welcome(ctx: ExecutionContext, email: str) -> None:
    outbox = ctx.outbox.command(NOTIFY_EVENTS)

    await outbox.stage("user.registered", UserRegistered(email=email), event_id=uuid4())
    await outbox.flush()


# --8<-- [start:consume]
async def deliver_notifications(
    ctx: ExecutionContext,
    senders: RecordingSenders,
) -> int:
    # Relay staged events to the queue, then let a QueueConsumer drain it. The consumer
    # deduplicates redeliveries through the inbox (so an at-least-once queue cannot
    # double-send) and parks poison messages — instead of a hand-rolled receive/ack loop
    # that would re-send on every redelivery. In production this is a background
    # lifecycle step (notification_consumer_lifecycle_step); here we drain once.
    await OutboxRelay(outbox_spec=NOTIFY_EVENTS).to_queue(ctx, NOTIFICATIONS)

    consumer = QueueConsumer(
        queue="notifications",
        queue_spec=NOTIFICATIONS,
        handler=notification_queue_consumer_handler(
            router=router,
            senders=senders,  # pyright: ignore[reportArgumentType]
        ),
        inbox_spec=NOTIFY_INBOX,
        tx_route="mock",
    )

    # A finite idle timeout ends the drain once the queue goes quiet.
    await consumer.run(ctx, timeout=timedelta(milliseconds=250))

    # Report notifications actually dispatched (one message can map to several), not the
    # number of queue messages processed.
    return len(senders.emails)


# --8<-- [end:consume]


async def main() -> None:
    ctx = ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve()
    )
    senders = RecordingSenders()
    await stage_welcome(ctx, "ada@example.com")
    sent = await deliver_notifications(ctx, senders)
    print(f"sent {sent} notification(s) to {[e.to for e in senders.emails]}")


if __name__ == "__main__":
    asyncio.run(main())

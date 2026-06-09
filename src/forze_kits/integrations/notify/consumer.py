"""Consume queue messages produced by outbox relay and dispatch notifications."""

from __future__ import annotations

from uuid import UUID, uuid4

from forze.application.contracts.outbox import IntegrationEvent
from forze.application.contracts.queue import QueueMessage
from .dispatch import dispatch_notification
from .routing import NotificationRouter
from .senders import NotificationSenders

# ----------------------- #


def integration_event_from_queue_message[M](
    message: QueueMessage[M],
) -> IntegrationEvent[M]:
    """Build an :class:`IntegrationEvent` from a relayed queue message.

    Outbox relay sets ``message.type`` to the staged ``event_type`` and
    ``message.key`` to ``str(event_id)`` when the backend supports it.
    """

    event_id: UUID
    if message.key is not None:
        try:
            event_id = UUID(message.key)
        except ValueError:
            event_id = uuid4()
    else:
        event_id = uuid4()

    return IntegrationEvent(
        event_type=message.type or "",
        payload=message.payload,
        event_id=event_id,
    )


async def process_notification_message[M](
    message: QueueMessage[M],
    *,
    router: NotificationRouter,
    senders: NotificationSenders,
    skip_unmapped: bool = True,
) -> int:
    """Map a queue message to notifications and dispatch them.

    Returns the number of commands dispatched. Delivery is **at-least-once** when
    the queue redelivers: implement idempotent senders or deduplicate on
    :attr:`~forze.application.contracts.outbox.IntegrationEvent.event_id`.

    When *skip_unmapped* is ``True`` (default), unknown ``event_type`` values yield
    ``0`` without error. Set ``False`` to raise via
    :meth:`NotificationRouter.resolve_or_raise`.
    """

    event = integration_event_from_queue_message(message)

    if skip_unmapped:
        commands = router.resolve(event)
    else:
        commands = router.resolve_or_raise(event)

    for command in commands:
        await dispatch_notification(command, senders)

    return len(commands)

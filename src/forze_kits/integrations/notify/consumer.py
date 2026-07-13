"""Consume relayed queue messages and dispatch the resulting notifications."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from forze.application.contracts.queue import QueueMessage

from .dispatch import dispatch_notification
from .events import integration_event_from_queue_message
from .routing import FrozenNotificationRouter
from .senders import NotificationSenders

# ----------------------- #


async def process_notification_message[M](
    message: QueueMessage[M],
    *,
    router: FrozenNotificationRouter,
    senders: NotificationSenders,
    skip_unmapped: bool = True,
) -> int:
    """Map a queue message to notifications and dispatch them.

    Returns the number of commands dispatched. Delivery is **at-least-once** when
    the queue redelivers: run this through a
    :class:`~forze_kits.integrations.consumer.QueueConsumer` (see
    :func:`notification_queue_consumer_handler`) so the inbox deduplicates on the
    deterministic :attr:`~forze.application.contracts.outbox.IntegrationEvent.event_id`,
    or make the senders idempotent.

    When *skip_unmapped* is ``True`` (default), unknown ``event_type`` values yield
    ``0`` without error. Set ``False`` to raise via
    :meth:`~forze_kits.integrations.notify.routing.FrozenNotificationRouter.resolve_or_raise`.
    """

    event = integration_event_from_queue_message(message)

    commands = router.resolve(event) if skip_unmapped else router.resolve_or_raise(event)

    for command in commands:
        await dispatch_notification(command, senders)

    return len(commands)


# ....................... #


def notification_queue_consumer_handler[M](
    *,
    router: FrozenNotificationRouter,
    senders: NotificationSenders,
    skip_unmapped: bool = True,
) -> Callable[[QueueMessage[M]], Awaitable[None]]:
    """Adapt :func:`process_notification_message` to a ``QueueConsumer`` handler.

    The consumer's handler returns ``None``; ``process_notification_message`` returns the
    dispatch count, so this discards it. Running notifications through the consumer (rather
    than a bespoke receive/ack loop) gives them inbox dedup — a redelivered message no
    longer re-sends — and the poison-parking ladder for free.
    """

    async def _handle(message: QueueMessage[M]) -> None:
        await process_notification_message(
            message, router=router, senders=senders, skip_unmapped=skip_unmapped
        )

    return _handle

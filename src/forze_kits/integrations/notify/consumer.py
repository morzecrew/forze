"""Consume queue messages produced by outbox relay and dispatch notifications."""

from __future__ import annotations

from uuid import UUID

from forze.application.contracts.outbox import IntegrationEvent
from forze.application.contracts.queue import QueueMessage
from forze.base.exceptions import exc
from forze.base.primitives import uuid4
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

    ``event_id`` derivation is **deterministic** so redeliveries of the same
    message always produce the same id (the dedup contract documented on
    :func:`process_notification_message` relies on it):

    1. a valid UUID ``message.key`` is used verbatim;
    2. otherwise the id is derived from the stable broker identity
       ``"<queue>:<message.id>"``;
    3. a message with neither (empty ``message.id``) raises
       :func:`exc.precondition` — a random id would silently break dedup.
    """

    event_id: UUID
    if message.key is not None:
        try:
            event_id = UUID(message.key)
        except ValueError:
            event_id = _event_id_from_message_identity(message)
    else:
        event_id = _event_id_from_message_identity(message)

    return IntegrationEvent(
        event_type=message.type or "",
        payload=message.payload,
        event_id=event_id,
    )


def _event_id_from_message_identity[M](message: QueueMessage[M]) -> UUID:
    """Derive a deterministic event id from the broker-assigned message identity.

    Guards empty ids explicitly: :func:`forze.base.primitives.uuid4` falls back
    to a *random* UUID for falsy input, which would defeat deduplication.
    """

    if not message.id:
        raise exc.precondition(
            "Cannot derive a deterministic event_id: queue message has neither "
            "a valid UUID key nor a broker message id",
            details={"queue": message.queue, "type": message.type},
        )

    return uuid4(f"{message.queue}:{message.id}")


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

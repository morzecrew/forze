"""Consume queue messages produced by outbox relay and dispatch notifications."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import IntegrationEvent
from forze.application.contracts.queue import QueueMessage, QueueSpec
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, uuid4
from .dispatch import dispatch_notification
from .routing import NotificationRouter
from .senders import NotificationSenders

# ----------------------- #


def _uuid_or_none(value: str | None) -> UUID | None:
    if not value:
        return None

    try:
        return UUID(value)
    except ValueError:
        return None


# ....................... #


def integration_event_from_queue_message[M](
    message: QueueMessage[M],
) -> IntegrationEvent[M]:
    """Build an :class:`IntegrationEvent` from a relayed queue message.

    Outbox relay sets ``message.type`` to the staged ``event_type``, carries
    the event id in the ``forze_event_id`` header, and sets ``message.key``
    to the staged *ordering key* (falling back to ``str(event_id)``).

    ``event_id`` derivation is **deterministic** so redeliveries of the same
    message always produce the same id (the dedup contract documented on
    :func:`process_notification_message` relies on it):

    1. a valid UUID ``forze_event_id`` header is used verbatim — it outranks
       ``key`` because a UUID-shaped *ordering key* in ``key`` would otherwise
       masquerade as the event id and collapse different events;
    2. otherwise a valid UUID ``message.key`` (legacy relays put the event id
       there);
    3. otherwise the id is derived from the stable broker identity
       ``"<queue>:<message.id>"``;
    4. a message with none of these (empty ``message.id``) raises
       :func:`exc.precondition` — a random id would silently break dedup.
    """

    event_id = (
        _uuid_or_none(message.headers.get(HEADER_EVENT_ID))
        or _uuid_or_none(message.key)
        or _event_id_from_message_identity(message)
    )

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


# ....................... #


def notification_queue_consumer_handler[M](
    *,
    router: NotificationRouter,
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


# ....................... #


def notification_consumer_lifecycle_step(
    *,
    queue: str,
    queue_spec: QueueSpec[Any],
    inbox_spec: InboxSpec,
    tx_route: StrKey,
    router: NotificationRouter,
    senders: NotificationSenders,
    skip_unmapped: bool = True,
    bind_tenant_from_headers: bool = False,
    max_deliveries: int | None = None,
    retry_policy: StrKey | None = None,
    restart_backoff: timedelta = timedelta(seconds=5),
    step_id: StrKey | None = None,
) -> LifecycleStep:
    """Background lifecycle step that consumes a notification queue via ``QueueConsumer``.

    Notifications inherit the consumer's inbox dedup and poison parking. The dedup key is
    the same deterministic event id :func:`integration_event_from_queue_message` derives
    (the ``forze_event_id`` header the relay sets, else ``message.key``, else
    ``"<queue>:<message.id>"``), so an at-least-once redelivery cannot double-send.

    All ``QueueConsumer`` knobs (*max_deliveries*, *retry_policy*,
    *bind_tenant_from_headers*, ...) pass through with the same defaults and caveats.
    """

    # Local import: the consumer integration imports broadly; importing it at module load
    # would widen this module's import graph (and risk a cycle) for a rarely-hot path.
    from forze_kits.integrations.consumer import (
        queue_consumer_background_lifecycle_step,
    )

    return queue_consumer_background_lifecycle_step(
        queue=queue,
        queue_spec=queue_spec,
        handler=notification_queue_consumer_handler(
            router=router, senders=senders, skip_unmapped=skip_unmapped
        ),
        inbox_spec=inbox_spec,
        tx_route=tx_route,
        message_id=lambda m: str(integration_event_from_queue_message(m).event_id),
        bind_tenant_from_headers=bind_tenant_from_headers,
        max_deliveries=max_deliveries,
        retry_policy=retry_policy,
        restart_backoff=restart_backoff,
        step_id=step_id if step_id is not None else f"notification_consumer:{queue}",
    )

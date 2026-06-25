"""Reconstruct an :class:`IntegrationEvent` from a relayed queue message."""

from __future__ import annotations

from uuid import UUID

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.outbox import IntegrationEvent
from forze.application.contracts.queue import QueueMessage
from forze.base.exceptions import exc
from forze.base.primitives import uuid4

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
    :func:`~forze_kits.integrations.notify.consumer.process_notification_message`
    relies on it):

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


# ....................... #


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

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

    1. a valid UUID ``forze_event_id`` header is used verbatim;
    2. otherwise the id is derived from the stable broker identity
       ``"<queue>:<message.id>"``;
    3. a message with neither (empty ``message.id``) raises
       :func:`exc.precondition` — a random id would silently break dedup.

    ``message.key`` is never used: it is the *ordering key* — a grouping
    token every event of one aggregate shares — so a UUID-shaped key would
    masquerade as the event id and collapse **different** events into one,
    silently dropping notifications.
    """

    event_id = _uuid_or_none(
        message.headers.get(HEADER_EVENT_ID)
    ) or _event_id_from_message_identity(message)

    if not message.type:
        # A relayed event always carries its staged ``event_type``; a typeless message is
        # malformed. Fail closed rather than mapping it to an empty type that silently
        # resolves to no notifications and acks as a success.
        raise exc.precondition(
            "Cannot map a queue message with no type to a notification event",
            details={"queue": message.queue},
        )

    return IntegrationEvent(
        event_type=message.type,
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
            "a valid UUID event-id header nor a broker message id",
            details={"queue": message.queue, "type": message.type},
        )

    return uuid4(f"{message.queue}:{message.id}")

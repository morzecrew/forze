from datetime import datetime
from types import MappingProxyType
from typing import Final, Mapping, final

import attrs

# ----------------------- #

_EMPTY_HEADERS: Final[Mapping[str, str]] = MappingProxyType({})

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PubSubMessage[M]:
    """Message as read from or written to a pubsub backend."""

    topic: str
    """Logical topic or channel."""

    payload: M
    """Structured payload carried by the message."""

    type: str | None = None
    """Optional message type or category."""

    published_at: datetime | None = None
    """Optional timestamp associated with the message."""

    key: str | None = None
    """Optional partitioning key for the message."""

    headers: Mapping[str, str] = _EMPTY_HEADERS
    """String-to-string transport metadata carried alongside the payload.

    Propagated best-effort via the backend's native metadata channel; not
    part of the payload contract. See
    :mod:`forze.application.contracts.envelope` for the well-known keys.
    """

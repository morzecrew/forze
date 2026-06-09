"""Port for consumer-side message deduplication (inbox)."""

from collections.abc import Awaitable
from typing import Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class InboxPort(Protocol):
    """Marks messages processed for exactly-once-effect consumption.

    Implementations atomically record a message id under an inbox route and report
    whether it was newly recorded.
    """

    def mark_if_unseen(self, inbox: str, message_id: str) -> Awaitable[bool]:
        """Atomically mark *message_id* under *inbox*.

        :returns: ``True`` if newly recorded (process the message), ``False`` if it
            was already seen (skip). For exactly-once effect, call this inside the
            same transaction as the message handler.
        """
        ...  # pragma: no cover

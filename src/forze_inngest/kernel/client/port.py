"""Structural protocol for Inngest clients."""

from typing import TYPE_CHECKING, Protocol
from collections.abc import Awaitable

if TYPE_CHECKING:
    import inngest

# ----------------------- #


class InngestClientPort(Protocol):
    """Operations implemented by :class:`~forze_inngest.kernel.client.client.InngestClient`."""

    @property
    def native(self) -> "inngest.Inngest":
        """Underlying Inngest SDK client (for registration and framework serve)."""
        ...  # pragma: no cover

    def send(
        self,
        events: "inngest.Event | list[inngest.Event]",
    ) -> Awaitable[list[str]]:
        """Send one or more events; returns Inngest event ids."""
        ...  # pragma: no cover

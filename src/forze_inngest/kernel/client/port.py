"""Structural protocol for Inngest clients."""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import inngest

# ----------------------- #


class InngestClientPort(Protocol):
    """Operations implemented by :class:`~forze_inngest.kernel.client.client.InngestClient`."""

    @property
    def native(self) -> "inngest.Inngest":
        """Underlying Inngest SDK client (for registration and framework serve)."""
        ...  # pragma: no cover

    async def send(
        self,
        events: "inngest.Event | list[inngest.Event]",
    ) -> list[str]:
        """Send one or more events; returns Inngest event ids."""
        ...  # pragma: no cover

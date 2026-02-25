"""Actor context port for tracking the current caller."""

from typing import Protocol, runtime_checkable
from uuid import UUID

# ----------------------- #
#! TODO: model for actor context


@runtime_checkable
class ActorContextPort(Protocol):
    """Access to the current actor identity (e.g. authenticated user)."""

    def get(self) -> UUID:
        """Return the current actor identifier.

        Implementations should raise if no actor is currently bound.
        """
        ...

    def set(self, actor_id: UUID) -> None:
        """Bind the current actor identifier for the ambient context."""
        ...

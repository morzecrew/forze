"""Actor context port for tracking the current caller.

Provides :class:`ActorContextPort` for ambient actor identity (e.g. authenticated
user). Used by mapping steps such as :class:`CreatorIdStep` to inject
``creator_id``.
"""

from typing import Protocol, runtime_checkable
from uuid import UUID

# ----------------------- #


@runtime_checkable
class ActorContextPort(Protocol):
    """Access to the current actor identity in the ambient context.

    Implementations typically use context variables or request-scoped storage.
    Used when mapping DTOs to inject ``creator_id`` or for audit trails.
    """

    def get(self) -> UUID:
        """Return the current actor identifier.

        Implementations should raise if no actor is currently bound.
        """
        ...

    def set(self, actor_id: UUID) -> None:
        """Bind the current actor identifier for the ambient context."""
        ...

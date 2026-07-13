from collections.abc import Awaitable
from typing import Protocol
from uuid import UUID

# ----------------------- #


class PrincipalDeactivationPort(Protocol):  # pragma: no cover
    """Deactivate a principal for the application: policy, sessions, and credentials."""

    def deactivate(self, principal_id: UUID) -> Awaitable[None]:
        """Idempotently disable authentication and authorization for ``principal_id``."""
        ...

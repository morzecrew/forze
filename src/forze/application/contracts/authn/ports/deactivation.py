from typing import Awaitable, Protocol
from uuid import UUID

# ----------------------- #


class PrincipalDeactivationPort(Protocol):  # pragma: no cover
    """Deactivate a principal for the application: policy, sessions, and credentials."""

    def deactivate(self, principal_id: UUID) -> Awaitable[None]:  # noqa: F841
        """Idempotently disable authentication and authorization for ``principal_id``."""
        ...

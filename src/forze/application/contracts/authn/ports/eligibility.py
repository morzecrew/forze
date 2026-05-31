from typing import Awaitable, Protocol
from uuid import UUID

# ----------------------- #


class PrincipalEligibilityPort(Protocol):  # pragma: no cover
    """Gate authentication and credential lifecycle by policy principal state."""

    def require_authentication_allowed(
        self,
        principal_id: UUID,  # noqa: F841
    ) -> Awaitable[None]:
        """Raise when ``principal_id`` is missing or inactive for authentication."""
        ...

from collections.abc import Awaitable
from typing import Protocol
from uuid import UUID

# ----------------------- #


class PrincipalEligibilityPort(Protocol):  # pragma: no cover
    """Gate authentication and credential lifecycle by policy principal state."""

    def require_authentication_allowed(
        self,
        principal_id: UUID,
        *,
        for_update: bool = False,
    ) -> Awaitable[None]:
        """Raise when ``principal_id`` is missing or inactive for authentication.

        ``for_update`` reads the principal with a row lock, held until the caller's
        transaction ends. Issuing a session passes it, so on a backend whose locking read
        excludes a concurrent update (Postgres) a deactivation either waits for the session
        and revokes it, or commits first and the issue is refused. Outside a transaction,
        and in a port with no row to lock, the read is plain.
        """
        ...

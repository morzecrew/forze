from typing import Awaitable, Protocol
from uuid import UUID

from .value_objects import TenantIdentity

# ----------------------- #


class TenantResolverPort(Protocol):
    """Port for resolving the tenant identity."""

    def resolve_from_principal(
        self,
        principal_id: UUID,
    ) -> Awaitable[TenantIdentity | None]:
        """Resolve the tenant identity from the principal ID."""
        ...


# ....................... #


class TenantProviderPort(Protocol):
    """Port for providing the tenant ID."""

    def __call__(self) -> TenantIdentity | None:
        """Provide the tenant ID."""
        ...

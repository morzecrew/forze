from typing import Awaitable, Protocol
from uuid import UUID

from .value_objects import PrincipalKind, PrincipalRef

# ----------------------- #


class PrincipalRegistryPort(Protocol):
    """Port for registering and resolving policy principals (anchors for authz)."""

    def create_principal(self, kind: PrincipalKind) -> Awaitable[PrincipalRef]:
        """Create a new principal row and return its stable reference."""
        ...  # pragma: no cover

    def get_principal(self, principal_id: UUID) -> Awaitable[PrincipalRef | None]:
        """Return the principal reference if known."""
        ...  # pragma: no cover

    def deactivate_principal(self, principal_id: UUID) -> Awaitable[None]:
        """Mark the principal inactive for policy purposes (exact semantics adapter-defined)."""
        ...  # pragma: no cover


# ....................... #


class RoleAssignmentPort(Protocol):
    """Port for attaching and listing role bindings on principals."""

    def assign_role(self, principal_id: UUID, role: str) -> Awaitable[None]:
        """Grant a role name to the principal."""
        ...  # pragma: no cover

    def revoke_role(self, principal_id: UUID, role: str) -> Awaitable[None]:
        """Revoke a role name from the principal."""
        ...  # pragma: no cover

    def list_roles(self, principal_id: UUID) -> Awaitable[set[str]]:
        """Enumerate role names assigned to the principal."""
        ...  # pragma: no cover


# ....................... #


class AuthorizationPort(Protocol):
    """Port for runtime permission evaluation given a resolved principal."""

    def permits(self, principal_id: UUID, permission: str) -> Awaitable[bool]:
        """Whether the principal satisfies the permission in the tenant scope."""
        ...  # pragma: no cover

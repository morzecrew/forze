from typing import Any, Awaitable, Mapping, Protocol
from uuid import UUID

from .types import PrincipalKind
from .value_objects import EffectiveGrants, PrincipalRef

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


class EffectiveGrantsPort(Protocol):
    """Port for resolving effective grants for a principal."""

    def resolve_effective_grants(
        self,
        principal: PrincipalRef | UUID,
    ) -> Awaitable[EffectiveGrants]:
        """Resolve effective grants for a principal."""
        ...  # pragma: no cover


# ....................... #


class RoleAssignmentPort(Protocol):
    """Port for attaching and listing role bindings on principals."""

    def assign_role(
        self,
        principal: PrincipalRef | UUID,
        role: str,  # noqa: F841
    ) -> Awaitable[None]:
        """Grant a role name to the principal."""
        ...  # pragma: no cover

    def revoke_role(
        self,
        principal: PrincipalRef | UUID,
        role: str,  # noqa: F841
    ) -> Awaitable[None]:
        """Revoke a role name from the principal."""
        ...  # pragma: no cover

    def list_roles(
        self,
        principal: PrincipalRef | UUID,
    ) -> Awaitable[frozenset[str]]:
        """Enumerate role names assigned to the principal."""
        ...  # pragma: no cover


# ....................... #


class AuthzPort(Protocol):
    """Port for runtime permission evaluation given a resolved principal."""

    def permits(
        self,
        principal: PrincipalRef | UUID,
        permission: str,  # noqa: F841
        *,
        resource: str | None = None,  # noqa: F841
        context: Mapping[str, Any] | None = None,  # noqa: F841
    ) -> Awaitable[bool]:
        """Whether the principal satisfies the permission."""
        ...  # pragma: no cover

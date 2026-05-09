from typing import Any, Awaitable, Mapping, Protocol
from uuid import UUID

from .types import PrincipalKind
from .value_objects import EffectiveGrants, PrincipalRef, RoleRef

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
        *,
        tenant_id: UUID | None = None,
    ) -> Awaitable[EffectiveGrants]:
        """Resolve effective grants for a principal in an optional tenant partition.

        :param tenant_id: Explicit tenant scope for the call. When ``principal`` is a
            :class:`~forze.application.contracts.authz.value_objects.PrincipalRef` with
            :attr:`~forze.application.contracts.authz.value_objects.PrincipalRef.tenant_id`
            set, both must agree or :class:`~forze.base.errors.CoreError` is raised.
        """
        ...  # pragma: no cover


# ....................... #


class RoleAssignmentPort(Protocol):
    """Port for attaching and listing role bindings on principals."""

    def assign_role(
        self,
        principal: PrincipalRef | UUID,
        role_key: str,  # noqa: F841
        *,
        tenant_id: UUID | None = None,
    ) -> Awaitable[None]:
        """Grant a role (catalog ``role_key``) to the principal.

        :param tenant_id: Optional tenant scope; merged with ``PrincipalRef.tenant_id`` when present.
        """
        ...  # pragma: no cover

    def revoke_role(
        self,
        principal: PrincipalRef | UUID,
        role_key: str,  # noqa: F841
        *,
        tenant_id: UUID | None = None,
    ) -> Awaitable[None]:
        """Revoke a role (catalog ``role_key``) from the principal.

        :param tenant_id: Optional tenant scope; merged with ``PrincipalRef.tenant_id`` when present.
        """
        ...  # pragma: no cover

    def list_roles(
        self,
        principal: PrincipalRef | UUID,
        *,
        tenant_id: UUID | None = None,
    ) -> Awaitable[frozenset[RoleRef]]:
        """Enumerate roles assigned via principal-role bindings (including via groups).

        :param tenant_id: Optional tenant scope; merged with ``PrincipalRef.tenant_id`` when present.
        """
        ...  # pragma: no cover


# ....................... #


class AuthzPort(Protocol):
    """Port for runtime permission evaluation given a resolved principal."""

    def permits(
        self,
        principal: PrincipalRef | UUID,
        permission_key: str,  # noqa: F841
        *,
        tenant_id: UUID | None = None,
        resource: str | None = None,  # noqa: F841
        context: Mapping[str, Any] | None = None,  # noqa: F841
    ) -> Awaitable[bool]:
        """Whether the principal satisfies the catalog permission (``permission_key``).

        :param tenant_id: Optional tenant scope; merged with ``PrincipalRef.tenant_id`` when present.
        """
        ...  # pragma: no cover

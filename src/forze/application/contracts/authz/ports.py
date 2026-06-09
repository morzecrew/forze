"""Authz port protocols."""

from typing import Awaitable, Protocol
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity

from .types import PrincipalKind
from .value_objects import (
    AuthzDecision,
    AuthzDocumentScope,
    AuthzDocumentScopeRequest,
    AuthzRequest,
    AuthzScope,
    AuthzSensitiveAccessRequest,
    AuthzSubject,
    EffectiveGrants,
    PrincipalRef,
    RoleRef,
)

# ----------------------- #


class AuthzDecisionPort(Protocol):
    """Port for operation-level allow/deny decisions."""

    def authorize(self, request: AuthzRequest) -> Awaitable[AuthzDecision]:
        """Evaluate whether ``request`` is permitted."""
        ...  # pragma: no cover


# ....................... #


class AuthzScopePort(Protocol):
    """Port for deriving data-access constraints."""

    def scope_document(
        self,
        request: AuthzDocumentScopeRequest,
    ) -> Awaitable[AuthzDocumentScope]:
        """Return query constraints for a document list/search/read path."""
        ...  # pragma: no cover

    def authorize_sensitive_resource(
        self,
        request: AuthzSensitiveAccessRequest,
    ) -> Awaitable[bool]:
        """Whether the subject may access the given resource instance."""
        ...  # pragma: no cover


# ....................... #


class GrantQueryPort(Protocol):
    """Port for resolving effective grants for a subject."""

    def resolve_effective_grants(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> Awaitable[EffectiveGrants]:
        """Resolve effective grants for a subject in an optional policy partition."""
        ...  # pragma: no cover


# ....................... #


class DelegationPort(Protocol):
    """Port for checking whether one principal may act on behalf of another.

    Answers the *pairwise* question ``may_act(actor, subject)`` — distinct from the
    least-privilege intersection (which asks whether both are independently permitted the
    action). Consulted by the authz before-hook only when
    :attr:`~forze.application.contracts.authz.specs.AuthzSpec.enforce_delegation_grant` is set.
    """

    def may_act(
        self,
        actor_id: UUID,
        subject_id: UUID,
        *,
        scope: AuthzScope | None = None,
    ) -> Awaitable[bool]:
        """Whether ``actor_id`` holds a grant to act on behalf of ``subject_id``."""
        ...  # pragma: no cover


# ....................... #


class DelegationGrantPort(Protocol):
    """Port for attaching and listing delegation (``may_act``) grants between principals."""

    def grant_delegation(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> Awaitable[None]:
        """Grant ``actor`` the right to act on behalf of ``subject`` (idempotent)."""
        ...  # pragma: no cover

    def revoke_delegation(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> Awaitable[None]:
        """Revoke ``actor``'s right to act on behalf of ``subject``."""
        ...  # pragma: no cover

    def list_delegators(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> Awaitable[frozenset[UUID]]:
        """Enumerate the subject principal ids ``actor`` may act on behalf of."""
        ...  # pragma: no cover


# ....................... #


class PrincipalRegistryPort(Protocol):
    """Port for registering and resolving policy principals."""

    def ensure_principal(
        self,
        principal_id: UUID,
        kind: PrincipalKind,
        *,
        is_active: bool = True,
    ) -> Awaitable[PrincipalRef]:
        """Create or return the policy principal for ``principal_id`` (idempotent)."""
        ...  # pragma: no cover

    def create_principal(self, kind: PrincipalKind) -> Awaitable[PrincipalRef]:
        """Create a new principal row and return its stable reference."""
        ...  # pragma: no cover

    def get_principal(self, principal_id: UUID) -> Awaitable[PrincipalRef | None]:
        """Return the principal reference if known."""
        ...  # pragma: no cover

    def deactivate_principal(self, principal_id: UUID) -> Awaitable[None]:
        """Mark the principal inactive for policy purposes."""
        ...  # pragma: no cover


# ....................... #


class RoleAssignmentPort(Protocol):
    """Port for attaching and listing role bindings on principals."""

    def assign_role(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        role_key: str,  # noqa: F841
        *,
        scope: AuthzScope | None = None,
    ) -> Awaitable[None]:
        """Grant a role (catalog ``role_key``) to the subject."""
        ...  # pragma: no cover

    def revoke_role(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        role_key: str,  # noqa: F841
        *,
        scope: AuthzScope | None = None,
    ) -> Awaitable[None]:
        """Revoke a role (catalog ``role_key``) from the subject."""
        ...  # pragma: no cover

    def list_roles(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> Awaitable[frozenset[RoleRef]]:
        """Enumerate roles assigned to the subject."""
        ...  # pragma: no cover

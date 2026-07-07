"""In-memory authz port stubs."""

from __future__ import annotations

from typing import TypedDict, cast, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz.helpers import subject_for_grant_query
from forze.application.contracts.authz.ports import (
    AuthzDecisionPort,
    AuthzScopePort,
    DelegationGrantPort,
    DelegationPort,
    GrantQueryPort,
    PrincipalRegistryPort,
    RoleAssignmentPort,
)
from forze.application.contracts.authz.types import PrincipalKind
from forze.application.contracts.authz.value_objects import (
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
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState

# ----------------------- #


class _AuthzRouteStore(TypedDict, total=False):
    """Per-route authz scratch store kept on the mock identity plane."""

    principals: dict[UUID, str]
    """Principal id → :class:`PrincipalKind` value."""

    inactive: list[UUID]
    """Deactivated principal ids."""

    delegations: set[tuple[str, str]]
    """Pairwise ``(actor, subject)`` delegation grants."""

    grants: set[tuple[str, str, str | None]]
    """Seeded ``(principal, permission_key, tenant_id | None)`` permission grants."""


# ....................... #


def _route_store(state: MockState, route: str) -> _AuthzRouteStore:
    identity = state.identity
    authz = identity.setdefault("authz", {})
    return authz.setdefault(route, {})


@final
@attrs.define(slots=True, kw_only=True)
class MockPrincipalRegistryPort(PrincipalRegistryPort):
    state: MockState
    route: str = "main"

    async def ensure_principal(
        self,
        principal_id: UUID,
        kind: PrincipalKind,
        *,
        is_active: bool = True,
    ) -> PrincipalRef:
        _ = is_active
        return PrincipalRef(principal_id=principal_id, kind=kind)

    async def create_principal(self, kind: PrincipalKind) -> PrincipalRef:
        return PrincipalRef(principal_id=uuid7(), kind=kind)

    async def get_principal(self, principal_id: UUID) -> PrincipalRef | None:
        store = _route_store(self.state, self.route)
        principals = store.setdefault("principals", {})
        if principal_id not in principals:
            return None
        return PrincipalRef(
            principal_id=principal_id,
            kind=cast(PrincipalKind, principals[principal_id]),
        )

    async def deactivate_principal(self, principal_id: UUID) -> None:
        store = _route_store(self.state, self.route)
        store.setdefault("inactive", []).append(principal_id)


@final
@attrs.define(slots=True, kw_only=True)
class MockRoleAssignmentPort(RoleAssignmentPort):
    async def assign_role(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        role_key: str,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        _ = subject, role_key, scope

    async def revoke_role(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        role_key: str,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        _ = subject, role_key, scope

    async def list_roles(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> frozenset[RoleRef]:
        _ = subject, scope
        return frozenset()


@final
@attrs.define(slots=True, kw_only=True)
class MockGrantQueryPort(GrantQueryPort):
    async def resolve_effective_grants(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> EffectiveGrants:
        _ = subject, scope
        return EffectiveGrants(resolved_at=utcnow())


def _delegation_store(state: MockState, route: str) -> set[tuple[str, str]]:
    return _route_store(state, route).setdefault("delegations", set())


@final
@attrs.define(slots=True, kw_only=True)
class MockDelegationGrantPort(DelegationGrantPort):
    """In-memory pairwise (actor → subject) delegation grants kept on mock state."""

    state: MockState
    route: str = "main"

    async def grant_delegation(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        _ = scope
        _delegation_store(self.state, self.route).add(
            (str(subject_for_grant_query(actor)), str(subject_for_grant_query(subject)))
        )

    async def revoke_delegation(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        _ = scope
        _delegation_store(self.state, self.route).discard(
            (str(subject_for_grant_query(actor)), str(subject_for_grant_query(subject)))
        )

    async def list_delegators(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> frozenset[UUID]:
        _ = scope
        actor_id = str(subject_for_grant_query(actor))
        return frozenset(
            UUID(subject)
            for granted_actor, subject in _delegation_store(self.state, self.route)
            if granted_actor == actor_id
        )


@final
@attrs.define(slots=True, kw_only=True)
class MockDelegationPort(DelegationPort):
    """Check pairwise delegation grants recorded by :class:`MockDelegationGrantPort`.

    Deny-unless-granted by default (proper enforcement semantics); set
    ``allow_by_default=True`` for a permissive stub.
    """

    state: MockState
    route: str = "main"
    allow_by_default: bool = False

    async def may_act(
        self,
        actor_id: UUID,
        subject_id: UUID,
        *,
        scope: AuthzScope | None = None,
    ) -> bool:
        _ = scope

        if (str(actor_id), str(subject_id)) in _delegation_store(
            self.state, self.route
        ):
            return True

        return self.allow_by_default


def _grant_store(state: MockState, route: str) -> set[tuple[str, str, str | None]]:
    """Seeded permission grants: ``(principal_id, permission_key, tenant_id | None)``."""

    return _route_store(state, route).setdefault("grants", set())


@final
@attrs.define(slots=True, kw_only=True)
class MockAuthzDecisionPort(AuthzDecisionPort):
    """Grant-aware authz decision stub.

    Mirrors the real catalog-backed evaluation at mock fidelity: a request is
    allowed when ``request.action`` matches a permission key seeded for the
    subject via :meth:`seed_grant`. Grants are tenant-scoped — a grant seeded
    with a ``tenant_id`` only matches requests whose policy scope carries that
    tenant; a grant seeded without one is global and matches any scope.

    Back-compat fallback: with no bound state, or **no grants seeded at all** on
    the route, the port behaves as the original constant stub — deny-by-default
    (proper enforcement semantics, consistent with :class:`MockDelegationPort`);
    set ``allow_by_default=True`` explicitly for a permissive stub when a test's
    purpose isn't authz.
    """

    state: MockState | None = None
    route: str = "main"
    allow_by_default: bool = False

    # ....................... #

    def seed_grant(
        self,
        principal_id: UUID,
        permission: str,
        tenant_id: UUID | None = None,
    ) -> None:
        """Seed a permission grant for ``principal_id`` (optionally tenant-scoped)."""

        if self.state is None:
            raise exc.configuration(
                "MockAuthzDecisionPort.seed_grant requires a bound MockState",
            )

        with self.state.lock:
            _grant_store(self.state, self.route).add(
                (
                    str(principal_id),
                    permission,
                    str(tenant_id) if tenant_id is not None else None,
                )
            )

    # ....................... #

    async def authorize(self, request: AuthzRequest) -> AuthzDecision:
        if self.state is None:
            return AuthzDecision(allowed=self.allow_by_default)

        with self.state.lock:
            grants = _grant_store(self.state, self.route)

            if not grants:
                return AuthzDecision(allowed=self.allow_by_default)

            principal_id = str(request.subject.principal_id)
            tenant_id = (
                str(request.scope.tenant_id)
                if request.scope.tenant_id is not None
                else None
            )

            matched = (principal_id, request.action, None) in grants or (
                tenant_id is not None
                and (principal_id, request.action, tenant_id) in grants
            )

        if matched:
            return AuthzDecision(
                allowed=True,
                matched_permission_key=request.action,
            )

        return AuthzDecision(
            allowed=False,
            reason=f"No grant for permission {request.action!r}",
        )


@final
@attrs.define(slots=True, kw_only=True)
class MockAuthzScopePort(AuthzScopePort):
    """Authz scoping stub.

    ``authorize_sensitive_resource`` is deny-by-default; set
    ``allow_sensitive_by_default=True`` explicitly for a permissive stub.
    """

    allow_sensitive_by_default: bool = False

    async def scope_document(
        self,
        request: AuthzDocumentScopeRequest,
    ) -> AuthzDocumentScope:
        _ = request
        return AuthzDocumentScope()

    async def authorize_sensitive_resource(
        self,
        request: AuthzSensitiveAccessRequest,
    ) -> bool:
        _ = request
        return self.allow_sensitive_by_default

"""In-memory authz port stubs."""

from __future__ import annotations

from typing import cast, final
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
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState

# ----------------------- #


def _route_store(state: MockState, route: str) -> dict[str, object]:
    identity = state.identity
    authz = identity.setdefault("authz", {})
    assert isinstance(authz, dict)  # nosec: B101
    store = authz.setdefault(route, {})  # type: ignore[assignment]
    assert isinstance(store, dict)  # nosec: B101
    return store  # type: ignore[return-value]


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
        assert isinstance(principals, dict)  # nosec: B101
        if principal_id not in principals:
            return None
        kind = principals[principal_id]  # type: ignore[index]
        assert isinstance(kind, str)  # nosec: B101
        return PrincipalRef(
            principal_id=principal_id,
            kind=cast(PrincipalKind, kind),
        )

    async def deactivate_principal(self, principal_id: UUID) -> None:
        store = _route_store(self.state, self.route)
        inactive = store.setdefault("inactive", [])
        assert isinstance(inactive, list)  # nosec: B101
        inactive.append(principal_id)  # type: ignore[arg-type]


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
    store = _route_store(state, route)
    delegations = store.setdefault("delegations", set())  # type: ignore[assignment]
    assert isinstance(delegations, set)  # nosec: B101
    return delegations  # type: ignore[return-value]


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


@final
@attrs.define(slots=True, kw_only=True)
class MockAuthzDecisionPort(AuthzDecisionPort):
    """Constant authz decision stub.

    Deny-by-default (proper enforcement semantics, consistent with
    :class:`MockDelegationPort`); set ``allow_by_default=True`` explicitly for a
    permissive stub when a test's purpose isn't authz.
    """

    allow_by_default: bool = False

    async def authorize(self, request: AuthzRequest) -> AuthzDecision:
        _ = request
        return AuthzDecision(allowed=self.allow_by_default)


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

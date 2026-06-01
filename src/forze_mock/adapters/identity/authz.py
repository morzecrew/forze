"""In-memory authz port stubs."""

from __future__ import annotations

from typing import cast, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz.ports import (
    AuthzDecisionPort,
    AuthzScopePort,
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


@final
@attrs.define(slots=True, kw_only=True)
class MockAuthzDecisionPort(AuthzDecisionPort):
    allow_by_default: bool = True

    async def authorize(self, request: AuthzRequest) -> AuthzDecision:
        _ = request
        return AuthzDecision(allowed=self.allow_by_default)


@final
@attrs.define(slots=True, kw_only=True)
class MockAuthzScopePort(AuthzScopePort):
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
        return True

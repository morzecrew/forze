"""A delegated call on a scope-guarded route never reaches past any principal in its chain.

The before-hook guard already decides each actor separately. The scope wrap and the sensitive
resource check must too: an agent acting for a user may see only what the user *and* the agent
could each see, so a denied agent is refused and an agent's narrower row scope narrows the page.
"""

from __future__ import annotations

from contextlib import ExitStack
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzDocumentScope,
    AuthzScope,
    AuthzSensitiveAccessRequest,
    AuthzSpec,
    EffectiveGrants,
    PermissionRef,
    subject_from_authn,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import Deps, InvocationMetadata
from forze.application.hooks.authz import AuthzDocumentScopeWrap
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO
from forze_identity.authz.adapters.scoping import AuthzScopeAdapter
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal
from forze_identity.authz.services.grants import AuthzGrantResolver
from forze_identity.authz.services.policy import AuthzPolicyService
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

# ----------------------- #

USER = AuthnIdentity(principal_id=uuid4())
AGENT = AuthnIdentity(principal_id=uuid4())
AS_AGENT_FOR_USER = AuthnIdentity(principal_id=USER.principal_id, actor=AGENT)
INNER = AuthnIdentity(principal_id=uuid4())
# INNER acts for AGENT, which acts for USER.
THREE_HOPS = AuthnIdentity(
    principal_id=USER.principal_id,
    actor=AuthnIdentity(principal_id=AGENT.principal_id, actor=INNER),
)


class _ListArgs(BaseDTO):
    filters: object = None


class _ScopeByPrincipal:
    """A scope port answering per principal, recording whom it was asked about."""

    def __init__(self, scopes: dict[UUID, AuthzDocumentScope]) -> None:
        self.scopes = scopes
        self.asked: list[UUID] = []

    async def scope_document(self, request: Any) -> AuthzDocumentScope:
        self.asked.append(request.subject.principal_id)
        return self.scopes.get(request.subject.principal_id, AuthzDocumentScope())


class _Delegation:
    def __init__(self, granted: bool) -> None:
        self.granted = granted
        self.asked: list[tuple[UUID, UUID]] = []

    async def may_act(self, actor_id: UUID, subject_id: UUID, *, scope: Any = None) -> bool:
        _ = scope
        self.asked.append((actor_id, subject_id))
        return self.granted


async def _list(
    identity: AuthnIdentity,
    port: _ScopeByPrincipal,
    *,
    spec: AuthzSpec | None = None,
    delegation: _Delegation | None = None,
) -> list[Any]:
    ctx = context_from_deps(Deps())
    seen: list[Any] = []

    async def _next(args: Any) -> Any:
        seen.append(args.filters)
        return None

    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ExitStack() as stack:
        stack.enter_context(patch.object(ctx.authz, "scope", return_value=port))

        if delegation is not None:
            stack.enter_context(patch.object(ctx.authz, "delegation", return_value=delegation))

        stack.enter_context(ctx.inv_ctx.bind(metadata=metadata, authn=identity))
        wrap = AuthzDocumentScopeWrap(
            spec=spec or AuthzSpec(name="main"), document_name="orders", operation="find_many"
        )(ctx)
        await wrap(_next, _ListArgs())

    return seen


# ....................... #


class TestTheScopeWrap:
    async def test_an_undelegated_call_asks_about_its_principal_once(self) -> None:
        port = _ScopeByPrincipal({})

        await _list(USER, port)

        assert port.asked == [USER.principal_id]

    async def test_a_denied_agent_is_refused_although_the_user_may_list(self) -> None:
        port = _ScopeByPrincipal({AGENT.principal_id: AuthzDocumentScope(deny_all=True)})

        with pytest.raises(CoreException) as caught:
            await _list(AS_AGENT_FOR_USER, port)

        assert caught.value.kind is ExceptionKind.AUTHORIZATION
        assert caught.value.code == "delegate_denied"

    async def test_an_agents_row_scope_narrows_the_users(self) -> None:
        mine: Any = {"$values": {"owner_id": str(USER.principal_id)}}
        team: Any = {"$values": {"team": "support"}}
        port = _ScopeByPrincipal(
            {
                USER.principal_id: AuthzDocumentScope(filters=mine),
                AGENT.principal_id: AuthzDocumentScope(filters=team),
            }
        )

        [filters] = await _list(AS_AGENT_FOR_USER, port)

        assert filters == {"$and": [mine, team]}
        assert port.asked == [USER.principal_id, AGENT.principal_id]

    async def test_every_hop_of_a_chain_is_asked(self) -> None:
        inner = AuthnIdentity(principal_id=uuid4())
        chain = AuthnIdentity(
            principal_id=USER.principal_id,
            actor=AuthnIdentity(principal_id=AGENT.principal_id, actor=inner),
        )
        port = _ScopeByPrincipal({inner.principal_id: AuthzDocumentScope(deny_all=True)})

        with pytest.raises(CoreException) as caught:
            await _list(chain, port)

        assert caught.value.code == "delegate_denied"
        assert port.asked == [USER.principal_id, AGENT.principal_id, inner.principal_id]

    @pytest.mark.parametrize("granted", [True, False])
    async def test_an_enforced_delegation_grant_is_checked(self, granted: bool) -> None:
        port = _ScopeByPrincipal({})
        spec = AuthzSpec(name="main", enforce_delegation_grant=True)

        if granted:
            delegation = _Delegation(True)
            await _list(AS_AGENT_FOR_USER, port, spec=spec, delegation=delegation)

            # The agent is asked whether it may act for the user, not the other way round.
            assert delegation.asked == [(AGENT.principal_id, USER.principal_id)]
            return

        with pytest.raises(CoreException) as caught:
            await _list(AS_AGENT_FOR_USER, port, spec=spec, delegation=_Delegation(False))

        assert caught.value.code == "delegation_not_granted"

    async def test_each_hop_asks_for_the_principal_it_acts_for(self) -> None:
        delegation = _Delegation(True)
        spec = AuthzSpec(name="main", enforce_delegation_grant=True)

        await _list(THREE_HOPS, _ScopeByPrincipal({}), spec=spec, delegation=delegation)

        assert delegation.asked == [
            (AGENT.principal_id, USER.principal_id),
            (INNER.principal_id, AGENT.principal_id),
        ]

    def test_an_enforced_grant_with_no_port_fails_when_the_hook_is_built(self) -> None:
        # Never open at runtime: the route cannot check what it declared it would.
        ctx = context_from_deps(Deps())
        spec = AuthzSpec(name="main", enforce_delegation_grant=True)

        with patch.object(ctx.authz, "scope", return_value=_ScopeByPrincipal({})):
            with pytest.raises(CoreException):
                AuthzDocumentScopeWrap(spec=spec, document_name="orders", operation="find_many")(
                    ctx
                )


# ....................... #


def _adapter(
    grants_by_principal: dict[UUID, EffectiveGrants],
    *,
    spec: AuthzSpec | None = None,
    delegation: _Delegation | None = None,
) -> AuthzScopeAdapter:
    now = datetime.now(tz=UTC)

    async def _find(*_: Any, **__: Any) -> ReadPolicyPrincipal:
        return ReadPolicyPrincipal(
            id=uuid4(), rev=1, created_at=now, last_update_at=now, kind="user", is_active=True
        )

    principal_qry = MagicMock()
    principal_qry.spec = DocumentSpec(name="policy_principals", read=ReadPolicyPrincipal)
    principal_qry.find = AsyncMock(side_effect=_find)

    async def _grants(pid: UUID, **_: Any) -> EffectiveGrants:
        return grants_by_principal.get(pid, EffectiveGrants())

    resolver = MagicMock(spec=AuthzGrantResolver)
    resolver.resolve_effective_grants = AsyncMock(side_effect=_grants)

    return AuthzScopeAdapter(
        spec=spec or AuthzSpec(name="main"),
        principal_qry=principal_qry,
        resolver=resolver,
        policy=AuthzPolicyService(),
        delegation=delegation,
    )


def _holding(key: str) -> EffectiveGrants:
    return EffectiveGrants(
        permissions=frozenset({PermissionRef(permission_id=uuid4(), permission_key=key)})
    )


class TestTheSensitiveResourceCheck:
    def _request(self, identity: AuthnIdentity) -> AuthzSensitiveAccessRequest:
        return AuthzSensitiveAccessRequest(
            subject=subject_from_authn(identity),
            scope=AuthzScope(),
            resource_type="invoice",
            resource_id=uuid4(),
            action="invoice.read",
        )

    async def test_a_delegated_read_needs_every_principal(self) -> None:
        adapter = _adapter({USER.principal_id: _holding("invoice.read")})

        assert await adapter.authorize_sensitive_resource(self._request(USER))
        assert not await adapter.authorize_sensitive_resource(self._request(AS_AGENT_FOR_USER))

    async def test_a_delegated_read_passes_when_both_hold_it(self) -> None:
        adapter = _adapter(
            {
                USER.principal_id: _holding("invoice.read"),
                AGENT.principal_id: _holding("invoice.read"),
            }
        )

        assert await adapter.authorize_sensitive_resource(self._request(AS_AGENT_FOR_USER))

    @pytest.mark.parametrize("granted", [True, False])
    async def test_an_enforced_delegation_grant_is_checked(self, granted: bool) -> None:
        both = {
            USER.principal_id: _holding("invoice.read"),
            AGENT.principal_id: _holding("invoice.read"),
        }
        delegation = _Delegation(granted)
        adapter = _adapter(
            both, spec=AuthzSpec(name="main", enforce_delegation_grant=True), delegation=delegation
        )

        assert (
            await adapter.authorize_sensitive_resource(self._request(AS_AGENT_FOR_USER)) is granted
        )
        assert delegation.asked == [(AGENT.principal_id, USER.principal_id)]

    async def test_an_inner_actor_needs_access_too(self) -> None:
        both = {
            USER.principal_id: _holding("invoice.read"),
            AGENT.principal_id: _holding("invoice.read"),
        }

        assert not await _adapter(both).authorize_sensitive_resource(self._request(THREE_HOPS))

    async def test_each_hop_asks_for_the_principal_it_acts_for(self) -> None:
        every = {
            principal.principal_id: _holding("invoice.read") for principal in (USER, AGENT, INNER)
        }
        delegation = _Delegation(True)
        adapter = _adapter(
            every, spec=AuthzSpec(name="main", enforce_delegation_grant=True), delegation=delegation
        )

        assert await adapter.authorize_sensitive_resource(self._request(THREE_HOPS))
        assert delegation.asked == [
            (AGENT.principal_id, USER.principal_id),
            (INNER.principal_id, AGENT.principal_id),
        ]

    def test_an_enforced_grant_with_no_port_is_refused_when_built(self) -> None:
        # Never open: the spec says a delegated call needs a recorded grant.
        with pytest.raises(CoreException):
            _adapter({}, spec=AuthzSpec(name="main", enforce_delegation_grant=True))

    def test_the_wired_adapter_carries_the_delegation_port_when_enforced(self) -> None:
        from forze.testing import context_from_modules
        from forze_identity.authz.execution.deps.configs import build_authz_shared_services
        from forze_identity.authz.execution.deps.deps import ConfigurableAuthzScope
        from forze_mock import MockDepsModule

        ctx = context_from_modules(MockDepsModule())
        build = ConfigurableAuthzScope(shared=build_authz_shared_services())

        enforced = build(ctx, AuthzSpec(name="main", enforce_delegation_grant=True))
        relaxed = build(ctx, AuthzSpec(name="main"))

        assert enforced.delegation is not None  # type: ignore[attr-defined]
        assert relaxed.delegation is None  # type: ignore[attr-defined]

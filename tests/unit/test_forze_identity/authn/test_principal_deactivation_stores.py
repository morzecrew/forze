"""Cascaded deactivation touches only the credential stores the authn module wires.

A deployment that never enabled API keys (or passwords) has no document route for that store,
so a deactivation that always resolved both failed outright. The stores are decided per module,
not per route: a key any route of the module issues must still be revoked, or a deactivated
principal keeps a working credential.

Runs against routed mock documents, wiring only the stores a deployment would have, so a
resolution of an unwired store fails here as it does on a real backend.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity, AuthnSpec
from forze.application.contracts.authz import PrincipalRegistryDepKey
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.adapters.credential_deactivation import (
    AuthnCredentialDeactivationHelper,
)
from forze_identity.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    session_spec,
)
from forze_identity.authn.domain.models.account import (
    CreateApiKeyAccountCmd,
    CreatePasswordAccountCmd,
)
from forze_identity.authn.services import PasswordConfig
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import CreatePolicyPrincipalCmd
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.execution.factories import ConfigurableMockDocument
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE)
KERNEL = AuthnKernelConfig(
    access_token_secret=b"k" * 32,
    refresh_token_pepper=b"p" * 32,
    api_key_pepper=b"a" * 32,
    password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
)


def _ctx(*stores: Any, **module: Any) -> tuple[Any, MagicMock]:
    """A context wiring the session store plus *stores*, and a principal registry stub."""

    mock = MockDepsModule()
    document = ConfigurableMockDocument(module=mock)
    names = {spec.name for spec in (session_spec, policy_principal_spec, *stores)}
    registry = MagicMock()
    registry.deactivate_principal = AsyncMock()

    deps = AuthnDepsModule(
        kernel=KERNEL,
        authz_route=ROUTE,
        principal_deactivation={ROUTE},
        token_lifecycle={ROUTE},
        **module,
    )().merge(
        Deps.routed(
            {
                DocumentQueryDepKey: dict.fromkeys(names, document),
                DocumentCommandDepKey: dict.fromkeys(names, document),
                PrincipalRegistryDepKey: {ROUTE: lambda _ctx, _spec: registry},
            }
        ).merge(Deps.plain({MockStateDepKey: mock.state}))
    )

    return context_from_deps(deps), registry


class TestTheStoresDeactivationTouches:
    async def test_a_token_only_module_deactivates_without_credential_stores(self) -> None:
        ctx, registry = _ctx(authn={ROUTE: {"token"}})
        principal = uuid4()

        await ctx.authn.principal_deactivation(SPEC).deactivate(principal)

        registry.deactivate_principal.assert_awaited_once_with(principal)

    async def test_it_still_revokes_the_principals_sessions(self) -> None:
        ctx, _ = _ctx(authn={ROUTE: {"token"}})
        principal = (
            await ctx.doc.command(policy_principal_spec).create(
                CreatePolicyPrincipalCmd(kind="user")
            )
        ).id
        await ctx.authn.token_lifecycle(SPEC).issue_tokens(AuthnIdentity(principal_id=principal))

        await ctx.authn.principal_deactivation(SPEC).deactivate(principal)

        sessions = await ctx.doc.query(session_spec).find_many(
            filters={"$values": {"principal_id": principal}}
        )
        assert sessions.hits and all(row.revoked_at is not None for row in sessions.hits)

    @pytest.mark.parametrize(
        "module",
        [
            pytest.param({"authn": {ROUTE: {"token"}, "keys": {"api_key"}}}, id="another-route"),
            pytest.param(
                {"authn": {ROUTE: {"token"}}, "api_key_lifecycle": {ROUTE}}, id="lifecycle"
            ),
        ],
    )
    async def test_api_keys_any_route_issues_are_revoked(self, module: Any) -> None:
        # Deactivating through a token route must not leave the principal a working key.
        ctx, _ = _ctx(api_key_account_spec, **module)
        principal = uuid4()
        await ctx.doc.command(api_key_account_spec).create(
            CreateApiKeyAccountCmd(principal_id=principal, key_hash="h")
        )

        await ctx.authn.principal_deactivation(SPEC).deactivate(principal)

        keys = await ctx.doc.query(api_key_account_spec).find_many(
            filters={"$values": {"principal_id": principal}}
        )
        assert [row.is_active for row in keys.hits] == [False]

    @pytest.mark.parametrize(
        "module",
        [
            pytest.param({"authn": {ROUTE: {"token"}, "login": {"password"}}}, id="another-route"),
            pytest.param({"authn": {ROUTE: {"token"}}, "password_lifecycle": {ROUTE}}, id="lifecycle"),
        ],
    )
    async def test_password_accounts_any_route_uses_are_closed(self, module: Any) -> None:
        ctx, _ = _ctx(password_account_spec, **module)
        principal = uuid4()
        await ctx.doc.command(password_account_spec).create(
            CreatePasswordAccountCmd(principal_id=principal, username="someone", password_hash="h")
        )

        await ctx.authn.principal_deactivation(SPEC).deactivate(principal)

        accounts = await ctx.doc.query(password_account_spec).find_many(
            filters={"$values": {"principal_id": principal}}
        )
        assert [row.is_active for row in accounts.hits] == [False]


def test_half_a_store_is_refused() -> None:
    # One port of a store without the other would skip its leg and leave credentials active.
    with pytest.raises(CoreException, match="both ports"):
        AuthnCredentialDeactivationHelper(ak_qry=MagicMock())

"""Cascaded deactivation closes every credential store the application wires, and no other.

A deployment that never enabled API keys (or passwords) has no document route for that store,
so a deactivation that always resolved both failed outright. The stores are decided from the
composed dependencies, not from one module's routes: a key any route of any module issues must
still be revoked, or a deactivated principal keeps a working credential.

Runs against mock documents wired only for the stores a deployment would have, so resolving an
unwired store fails here as it does on a real backend.
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
from forze.base.exceptions import CoreException, ExceptionKind
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


def _ctx(
    *stores: Any,
    others: tuple[dict[str, Any], ...] = (),
    plain: bool = False,
    query_only: tuple[Any, ...] = (),
    command_only: tuple[Any, ...] = (),
    **module: Any,
) -> tuple[Any, MagicMock]:
    """A context wiring the session store plus *stores*, and a principal registry stub.

    *others* are further authn modules composed beside the deactivating one; *plain* wires
    every document through the plain fallback instead of per-spec routes; *query_only* and
    *command_only* wire one port of a store without the other.
    """

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
    )()

    for other in others:
        deps = deps.merge(AuthnDepsModule(kernel=KERNEL, **other)())

    documents = (
        Deps.plain({DocumentQueryDepKey: document, DocumentCommandDepKey: document})
        if plain
        else Deps.routed(
            {
                DocumentQueryDepKey: dict.fromkeys(
                    names | {spec.name for spec in query_only}, document
                ),
                DocumentCommandDepKey: dict.fromkeys(
                    names | {spec.name for spec in command_only}, document
                ),
            }
        )
    )
    deps = (
        deps.merge(documents)
        .merge(Deps.routed({PrincipalRegistryDepKey: {ROUTE: lambda _ctx, _spec: registry}}))
        .merge(Deps.plain({MockStateDepKey: mock.state}))
    )

    return context_from_deps(deps), registry


async def _active_keys(ctx: Any, principal: Any) -> list[bool]:
    keys = await ctx.doc.query(api_key_account_spec).find_many(
        filters={"$values": {"principal_id": principal}}
    )

    return [row.is_active for row in keys.hits]


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


class TestTheStoresAreTheApplications:
    async def test_a_key_another_module_issues_is_revoked(self) -> None:
        # Apps compose several authn modules; the deactivating one does not own the key store.
        ctx, _ = _ctx(
            api_key_account_spec,
            authn={ROUTE: {"token"}},
            others=({"authn": {"svc": {"api_key"}}, "api_key_lifecycle": {"svc"}},),
        )
        principal = uuid4()
        await ctx.doc.command(api_key_account_spec).create(
            CreateApiKeyAccountCmd(principal_id=principal, key_hash="h")
        )

        await ctx.authn.principal_deactivation(SPEC).deactivate(principal)

        assert await _active_keys(ctx, principal) == [False]

    async def test_a_key_route_with_no_document_store_deactivates(self) -> None:
        # A verifier override (file or environment keys) keeps no API-key accounts.
        ctx, registry = _ctx(
            authn={ROUTE: {"token", "api_key"}},
            api_key_verifiers={ROUTE: lambda _ctx, _spec: object()},
        )
        principal = uuid4()

        await ctx.authn.principal_deactivation(SPEC).deactivate(principal)

        registry.deactivate_principal.assert_awaited_once_with(principal)

    async def test_a_store_wired_through_the_plain_fallback_is_closed(self) -> None:
        ctx, _ = _ctx(authn={ROUTE: {"token"}}, plain=True)
        principal = uuid4()
        await ctx.doc.command(api_key_account_spec).create(
            CreateApiKeyAccountCmd(principal_id=principal, key_hash="h")
        )

        await ctx.authn.principal_deactivation(SPEC).deactivate(principal)

        assert await _active_keys(ctx, principal) == [False]

    @pytest.mark.parametrize(
        "half",
        [
            pytest.param({"query_only": (api_key_account_spec,)}, id="query-only"),
            pytest.param({"command_only": (api_key_account_spec,)}, id="command-only"),
        ],
    )
    async def test_a_store_wired_by_one_port_is_refused(self, half: Any) -> None:
        # Keys deactivation could see but not revoke, or revoke but not find: refuse rather
        # than leave them working.
        ctx, _ = _ctx(authn={ROUTE: {"token"}}, **half)

        with pytest.raises(CoreException, match="authn_api_key_accounts") as caught:
            await ctx.authn.principal_deactivation(SPEC).deactivate(uuid4())

        assert caught.value.kind is ExceptionKind.CONFIGURATION


def test_half_a_store_is_refused() -> None:
    # One port of a store without the other would skip its leg and leave credentials active.
    with pytest.raises(CoreException, match="both ports"):
        AuthnCredentialDeactivationHelper(ak_qry=MagicMock())

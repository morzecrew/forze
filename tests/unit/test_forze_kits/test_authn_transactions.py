"""The authn kit's revoking operations commit whole, and reuse detection survives the rollback.

Runs the kit's operations over two stacks: the identity plane's real adapters on mock documents,
and the mock token lifecycle. Both run on the mock's journal transaction manager, which rolls
documents and identity state back with a failed transaction, as a database would.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity, AuthnSpec, IssuedTokens
from forze.application.contracts.authz import PrincipalRegistryDepKey
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.execution import BeforeStep
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.adapters.credential_deactivation import (
    AuthnCredentialDeactivationHelper,
)
from forze_identity.authn.application.specs import api_key_account_spec, session_spec
from forze_identity.authn.services import PasswordConfig
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import CreatePolicyPrincipalCmd
from forze_identity.authz.execution.deps.deps import ConfigurablePrincipalRegistry
from forze_kits.aggregates.authn import (
    AuthnKernelOp,
    AuthnRefreshRequestDTO,
    DeactivatePrincipalRequestDTO,
    build_authn_registry,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.execution.factories import ConfigurableMockDocument, mock_journal_txmanager
from tests.support.execution_context import context_from_deps, context_from_modules

pytest.importorskip("jwt")
pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token"}))
NS = SPEC.default_namespace
KERNEL = AuthnKernelConfig(
    access_token_secret=b"k" * 32,
    refresh_token_pepper=b"p" * 32,
    api_key_pepper=b"a" * 32,
    password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
)


async def _allow(args: Any) -> None:
    _ = args


def _registry(tx_route: str | None = "default") -> Any:
    return build_authn_registry(
        SPEC,
        admin_guards=(BeforeStep(id="app.admin", factory=lambda ctx: _allow),),
        trust_admin_guards=True,
        tx_route=tx_route,
    ).freeze()


def _real_ctx() -> ExecutionContext:
    """The identity plane's adapters over mock documents and the journal transaction manager."""

    mock = MockDepsModule()
    document = ConfigurableMockDocument(module=mock)
    names = {spec.name for spec in (session_spec, policy_principal_spec, api_key_account_spec)}
    deps = (
        AuthnDepsModule(
            kernel=KERNEL,
            authn={ROUTE: frozenset({"token", "api_key"})},
            authz_route=ROUTE,
            principal_deactivation={ROUTE},
            token_lifecycle={ROUTE},
        )()
        .merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: dict.fromkeys(names, document),
                    DocumentCommandDepKey: dict.fromkeys(names, document),
                    PrincipalRegistryDepKey: {ROUTE: ConfigurablePrincipalRegistry()},
                }
            )
        )
        .merge(
            Deps.plain(
                {
                    MockStateDepKey: mock.state,
                    TransactionManagerDepKey: mock_journal_txmanager,
                }
            )
        )
    )

    return context_from_deps(deps)


def _mock_ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule())


async def _principal(ctx: ExecutionContext) -> UUID:
    if ctx.deps.exists(PrincipalRegistryDepKey, route=ROUTE):
        created = await ctx.doc.command(policy_principal_spec).create(
            CreatePolicyPrincipalCmd(kind="user")
        )
        return created.id

    return uuid4()


def _refresh_token(issued: IssuedTokens) -> str:
    if issued.refresh is None:
        raise AssertionError("the lifecycle issued no refresh token")

    return issued.refresh.token.token


async def _refresh(ctx: ExecutionContext, registry: Any, token: str) -> Any:
    return await run_operation(
        registry,
        NS.key(AuthnKernelOp.REFRESH_TOKENS),
        AuthnRefreshRequestDTO(refresh_token=token),
        ctx,
    )


# ----------------------- #


@pytest.mark.parametrize("build", [_real_ctx, _mock_ctx], ids=["identity", "mock"])
@pytest.mark.parametrize("tx_route", ["default", None], ids=["tx", "no-tx"])
async def test_a_reused_refresh_token_revokes_its_family(
    build: Callable[[], ExecutionContext],
    tx_route: str | None,
) -> None:
    # The lifecycle revokes the family and then raises, and the raise rolls its own revocation
    # back. The live session of the family must still be revoked once the refresh has failed.
    ctx = build()
    registry = _registry(tx_route)
    principal = await _principal(ctx)
    first = await ctx.authn.token_lifecycle(SPEC).issue_tokens(
        AuthnIdentity(principal_id=principal)
    )
    second = await _refresh(ctx, registry, _refresh_token(first))

    with pytest.raises(CoreException) as caught:
        await _refresh(ctx, registry, _refresh_token(first))

    # Reuse renders as the uniform refusal, the same as any other bad refresh token.
    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert caught.value.summary == "Invalid refresh token"
    assert caught.value.code == "core.authentication"
    assert caught.value.details is None

    with pytest.raises(CoreException, match="Invalid refresh token"):
        await _refresh(ctx, registry, second.refresh_token)


async def test_a_failed_deactivation_leaves_the_principal_and_its_sessions_as_they_were(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _real_ctx()
    principal = await _principal(ctx)
    issued = await ctx.authn.token_lifecycle(SPEC).issue_tokens(
        AuthnIdentity(principal_id=principal)
    )

    # The credential leg runs last, after the principal and its sessions were written.
    async def fail(*_: Any) -> None:
        raise exc.infrastructure("credential store unavailable")

    monkeypatch.setattr(AuthnCredentialDeactivationHelper, "deactivate_all", fail)

    with (
        ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=uuid4())),
        pytest.raises(CoreException, match="credential store unavailable"),
    ):
        await run_operation(
            _registry(),
            NS.key(AuthnKernelOp.DEACTIVATE_PRINCIPAL),
            DeactivatePrincipalRequestDTO(principal_id=principal),
            ctx,
        )

    row = await ctx.doc.query(policy_principal_spec).get(principal)
    sessions = await ctx.doc.query(session_spec).find_many(
        filters={"$values": {"principal_id": principal}}
    )

    assert row.is_active
    assert [session.revoked_at for session in sessions.hits] == [None]

    # And the session still refreshes: nothing of the failed deactivation committed.
    await _refresh(ctx, _registry(), _refresh_token(issued))


async def test_deactivating_a_principal_on_the_mock_revokes_its_sessions() -> None:
    ctx = _mock_ctx()
    principal = uuid4()
    issued = await ctx.authn.token_lifecycle(SPEC).issue_tokens(
        AuthnIdentity(principal_id=principal)
    )

    with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=uuid4())):
        await run_operation(
            _registry(),
            NS.key(AuthnKernelOp.DEACTIVATE_PRINCIPAL),
            DeactivatePrincipalRequestDTO(principal_id=principal),
            ctx,
        )

    with pytest.raises(CoreException, match="Invalid refresh token"):
        await _refresh(ctx, _registry(), _refresh_token(issued))

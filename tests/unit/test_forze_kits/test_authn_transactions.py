"""The authn kit's revoking operations commit whole, and reuse detection survives the rollback.

Runs the kit's operations over two stacks: the identity plane's real adapters on mock documents,
and the mock token lifecycle. Both run on the mock's journal transaction manager, which rolls
documents and identity state back with a failed transaction, as a database would.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import (
    AuthnIdentity,
    AuthnSpec,
    IssuedTokens,
    PasswordCredentials,
)
from forze.application.contracts.authz import PrincipalRegistryDepKey
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.execution import BeforeStep
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.adapters import _utils as authn_utils
from forze_identity.authn.adapters import password_lifecycle, password_reset, token_lifecycle
from forze_identity.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    password_reset_spec,
    session_spec,
)
from forze_identity.authn.domain.models.account import CreateApiKeyAccountCmd
from forze_identity.authn.services import PasswordConfig
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import CreatePolicyPrincipalCmd
from forze_identity.authz.execution.deps.deps import ConfigurablePrincipalRegistry
from forze_kits.aggregates.authn import (
    AuthnChangePasswordRequestDTO,
    AuthnKernelOp,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnResetPasswordDTO,
    DeactivatePrincipalRequestDTO,
    build_authn_registry,
)
from forze_kits.aggregates.tenancy import TenancyKernelOp, build_tenancy_registry
from forze_kits.aggregates.tenancy.dto import TenantSwitchRequestDTO
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters.document import MockDocumentAdapter
from forze_mock.adapters.identity import seed_password_account
from forze_mock.adapters.identity.authn import MockTokenLifecyclePort
from forze_mock.execution.factories import ConfigurableMockDocument, mock_journal_txmanager
from tests.support.execution_context import context_from_deps, context_from_modules

pytest.importorskip("jwt")
pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token", "password"}))
NS = SPEC.default_namespace
KERNEL = AuthnKernelConfig(
    access_token_secret=b"k" * 32,
    refresh_token_pepper=b"p" * 32,
    api_key_pepper=b"a" * 32,
    reset_token_pepper=b"r" * 32,
    password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
)


async def _allow(args: Any) -> None:
    _ = args


def _registry(**kwargs: Any) -> Any:
    """The kit's registry; ``tx_route`` is the library default unless passed."""

    return build_authn_registry(
        SPEC,
        admin_guards=(BeforeStep(id="app.admin", factory=lambda ctx: _allow),),
        trust_admin_guards=True,
        **kwargs,
    ).freeze()


def _real_ctx(*, tx: bool = True) -> ExecutionContext:
    """The identity plane's adapters over mock documents and the journal transaction manager."""

    mock = MockDepsModule()
    document = ConfigurableMockDocument(module=mock)
    names = {
        spec.name
        for spec in (
            session_spec,
            policy_principal_spec,
            api_key_account_spec,
            password_account_spec,
            password_reset_spec,
        )
    }
    deps = (
        AuthnDepsModule(
            kernel=KERNEL,
            authn={ROUTE: frozenset({"token", "api_key", "password"})},
            authz_route=ROUTE,
            principal_deactivation={ROUTE},
            token_lifecycle={ROUTE},
            password_lifecycle={ROUTE},
            password_account_provisioning={ROUTE},
            password_reset={ROUTE},
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
        .merge(Deps.plain({MockStateDepKey: mock.state}))
    )

    if tx:
        deps = deps.merge(Deps.plain({TransactionManagerDepKey: mock_journal_txmanager}))

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
    registry = _registry(tx_route=tx_route)
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
    await ctx.doc.command(api_key_account_spec).create(
        CreateApiKeyAccountCmd(principal_id=principal, key_hash="h")
    )

    # The session leg runs last, after the principal and its credentials were written.
    async def fail(*_: Any) -> None:
        raise exc.infrastructure("session store unavailable")

    monkeypatch.setattr(token_lifecycle, "revoke_sessions_matching", fail)

    with (
        ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=uuid4())),
        pytest.raises(CoreException, match="session store unavailable"),
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

    keys = await ctx.doc.query(api_key_account_spec).find_many(
        filters={"$values": {"principal_id": principal}}
    )

    assert row.is_active
    assert [key.is_active for key in keys.hits] == [True]
    assert [session.revoked_at for session in sessions.hits] == [None]

    # And the session still refreshes: nothing of the failed deactivation committed.
    await _refresh(ctx, _registry(), _refresh_token(issued))


@pytest.mark.parametrize("flow", ["change", "reset"])
async def test_a_failed_password_flow_keeps_the_old_password(
    monkeypatch: pytest.MonkeyPatch,
    flow: str,
) -> None:
    ctx = _real_ctx()
    principal = await _principal(ctx)
    await ctx.authn.password_account_provisioning(SPEC).register_with_password(
        principal,
        PasswordCredentials(login="alice", password="pw-1"),
    )
    issued = await ctx.authn.password_reset(SPEC).request_reset("alice")
    accounts = ctx.doc.query(password_account_spec)
    filters: Any = {"$values": {"principal_id": principal}}
    before = await accounts.find(filters)

    # Logging out everywhere runs after the new hash was written (and a reset consumed).
    async def fail(*_: Any) -> None:
        raise exc.infrastructure("session store unavailable")

    monkeypatch.setattr(password_lifecycle, "revoke_sessions_matching", fail)
    monkeypatch.setattr(password_reset, "revoke_sessions_matching", fail)

    if flow == "change":
        op, args = (
            AuthnKernelOp.CHANGE_PASSWORD,
            AuthnChangePasswordRequestDTO(current_password="pw-1", new_password="pw-2"),
        )
    else:
        assert issued is not None
        op, args = (
            AuthnKernelOp.RESET_PASSWORD,
            AuthnResetPasswordDTO(token=issued.token, new_password="pw-2"),
        )

    with (
        ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=principal)),
        pytest.raises(CoreException, match="session store unavailable"),
    ):
        await run_operation(_registry(), NS.key(op), args, ctx)

    after = await accounts.find(filters)
    resets = await ctx.doc.query(password_reset_spec).find_many(filters=filters)

    assert before is not None and after is not None
    assert after.password_hash == before.password_hash
    assert [row.used_at for row in resets.hits] == [None]


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


@pytest.mark.parametrize("flow", ["login", "refresh", "switch_tenant", "logout"])
async def test_every_session_operation_calls_its_lifecycle_inside_a_transaction(
    monkeypatch: pytest.MonkeyPatch,
    flow: str,
) -> None:
    # The eligibility lock is held only as long as the issuing transaction, so a flow that
    # issues outside one does not serialize with a deactivation. Logout revokes inside one.
    ctx = _mock_ctx()
    principal = uuid4()
    identity = AuthnIdentity(principal_id=principal)
    depths: list[int] = []

    for name in ("issue_tokens", "refresh_tokens", "revoke_tokens"):
        original = getattr(MockTokenLifecyclePort, name)

        async def spy(self: Any, *args: Any, _original: Any = original, **kwargs: Any) -> Any:
            depths.append(ctx.tx_ctx.depth())
            return await _original(self, *args, **kwargs)

        monkeypatch.setattr(MockTokenLifecyclePort, name, spy)

    if flow == "login":
        seed_password_account(
            ctx.deps.provide(MockStateDepKey), login="alice", password="pw", principal_id=principal
        )
        await run_operation(
            _registry(),
            NS.key(AuthnKernelOp.PASSWORD_LOGIN),
            AuthnLoginRequestDTO(login="alice", password="pw"),
            ctx,
        )

    elif flow == "refresh":
        issued = await ctx.authn.token_lifecycle(SPEC).issue_tokens(identity)
        depths.clear()
        await _refresh(ctx, _registry(), _refresh_token(issued))

    elif flow == "logout":
        with ctx.inv_ctx.bind_identity(authn=identity):
            await run_operation(_registry(), NS.key(AuthnKernelOp.LOGOUT), None, ctx)

    else:
        manager = ctx.tenancy.require_manager(ROUTE)
        tenant = await manager.provision_tenant()
        await manager.attach_principal(principal, tenant.tenant_id)

        with ctx.inv_ctx.bind_identity(authn=identity):
            await run_operation(
                build_tenancy_registry(SPEC).freeze(),
                NS.key(TenancyKernelOp.SWITCH_TENANT),
                TenantSwitchRequestDTO(id=tenant.tenant_id),
                ctx,
            )

    assert depths == [1]


async def test_the_wired_eligibility_locks_the_principal_only_inside_a_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The deps module gives the policy-principal adapter its transaction probe: a backend
    # refuses a locking read outside a transaction, so outside one the read is plain.
    ctx = _real_ctx()
    principal = await _principal(ctx)
    locks: list[object] = []
    original = MockDocumentAdapter.find

    async def spy(self: Any, filters: Any, *, for_update: Any = False) -> Any:
        if self.spec.name == policy_principal_spec.name:
            locks.append(for_update)

        return await original(self, filters, for_update=for_update)

    monkeypatch.setattr(MockDocumentAdapter, "find", spy)
    issued = await ctx.authn.token_lifecycle(SPEC).issue_tokens(
        AuthnIdentity(principal_id=principal)
    )
    await _refresh(ctx, _registry(), _refresh_token(issued))

    assert locks == [False, True]


async def test_a_refresh_inside_an_open_transaction_is_refused() -> None:
    # Both of its scopes would be savepoints, and the reuse revocation would roll back with
    # the enclosing transaction.
    ctx = _mock_ctx()
    issued = await ctx.authn.token_lifecycle(SPEC).issue_tokens(AuthnIdentity(principal_id=uuid4()))

    async with ctx.tx_ctx.scope("default"):
        with pytest.raises(CoreException) as caught:
            await _refresh(ctx, _registry(), _refresh_token(issued))

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert caught.value.code == "authn_refresh_in_transaction"

    # Outside one, the same token refreshes: nothing was read or written.
    await _refresh(ctx, _registry(), _refresh_token(issued))


async def test_a_registry_without_a_transaction_manager_fails_when_built() -> None:
    ctx = _real_ctx(tx=False)

    with pytest.raises(CoreException) as caught:
        _registry().resolve(NS.key(AuthnKernelOp.LOGOUT), ctx)

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert "transaction_manager" in caught.value.summary


async def test_logout_revokes_more_live_sessions_than_one_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authn_utils, "_REVOKE_BATCH", 2)
    ctx = _real_ctx()
    principal = await _principal(ctx)
    identity = AuthnIdentity(principal_id=principal)

    for _ in range(5):
        await ctx.authn.token_lifecycle(SPEC).issue_tokens(identity)

    with ctx.inv_ctx.bind_identity(authn=identity):
        await run_operation(_registry(), NS.key(AuthnKernelOp.LOGOUT), None, ctx)

    sessions = await ctx.doc.query(session_spec).find_many(
        filters={"$values": {"principal_id": principal}}
    )

    assert len(sessions.hits) == 5
    assert all(session.revoked_at is not None for session in sessions.hits)


def _server_reported() -> CoreException:
    """A conflict translated from a driver error, as a deadlock or a write conflict is."""

    error = exc.concurrency("Deadlock detected. Please retry.")
    error.__cause__ = RuntimeError("deadlock detected")
    return error


@pytest.mark.parametrize(
    ("error", "calls"),
    [
        pytest.param(exc.precondition("stale", code="revision_mismatch"), 4, id="mismatch"),
        pytest.param(exc.concurrency("Failed to update records"), 4, id="no-row-matched"),
        pytest.param(_server_reported(), 1, id="server-reported"),
        pytest.param(exc.precondition("other", code="something_else"), 1, id="other"),
    ],
)
async def test_a_revocation_retries_a_stale_batch_only_so_often(
    error: CoreException,
    calls: int,
) -> None:
    session = MagicMock(id=uuid4(), rev=1)
    qry = MagicMock()
    qry.find_many = AsyncMock(return_value=MagicMock(hits=[session]))
    cmd = MagicMock()
    # A bounded script: a retry with no cap fails here at once, rather than looping and
    # recording calls until the process runs out of memory.
    cmd.update_many = AsyncMock(
        side_effect=[error] * 5 + [AssertionError("the revocation retried without a cap")]
    )

    with pytest.raises(CoreException) as caught:
        await authn_utils.revoke_sessions_matching(qry, cmd, {"principal_id": uuid4()})

    assert caught.value is error
    assert cmd.update_many.await_count == calls


@pytest.mark.parametrize("failures", [1, 2], ids=["in-line", "in-line-and-first-retry"])
async def test_a_reuse_revocation_that_conflicts_still_revokes_the_family(
    monkeypatch: pytest.MonkeyPatch,
    failures: int,
) -> None:
    # A concurrent refresh can make the revocation conflict, and on Mongo a conflict aborts
    # the transaction, so no retry inside it can recover. The reuse must still be reported,
    # and the family revoked in a fresh transaction.
    ctx = _real_ctx()
    registry = _registry()
    principal = await _principal(ctx)
    first = await ctx.authn.token_lifecycle(SPEC).issue_tokens(
        AuthnIdentity(principal_id=principal)
    )
    second = await _refresh(ctx, registry, _refresh_token(first))
    original = token_lifecycle.revoke_sessions_matching
    left = [failures]

    async def conflicting(*args: Any, **kwargs: Any) -> None:
        if left[0]:
            left[0] -= 1
            raise exc.concurrency("Write conflict during transaction. Please retry.")

        await original(*args, **kwargs)

    monkeypatch.setattr(token_lifecycle, "revoke_sessions_matching", conflicting)

    with pytest.raises(CoreException) as caught:
        await _refresh(ctx, registry, _refresh_token(first))

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert caught.value.summary == "Invalid refresh token"

    with pytest.raises(CoreException, match="Invalid refresh token"):
        await _refresh(ctx, registry, second.refresh_token)

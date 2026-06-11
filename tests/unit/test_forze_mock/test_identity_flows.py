"""Real identity flows against pure :class:`~forze_mock.execution.MockDepsModule` deps.

The proof that the mock plane runs the framework's own authn/authz machinery —
no AsyncMocks, no identity-plane imports in ``src``:

- the kits authn registry (``build_authn_registry``) executes password login,
  refresh (with rotation + reuse → family revocation), change-password
  ("log out everywhere"), and logout against the mock token/password lifecycles;
- access tokens issued by the mock lifecycle verify through the core
  :class:`~forze.application.integrations.authn.AuthnOrchestrator` resolved from
  ``AuthnDepKey`` and resolve back to the seeded principal;
- operations guarded by :class:`AuthzBeforeAuthorize` are denied/allowed by the
  grant-aware :class:`MockAuthzDecisionPort` (tenant-scoped included).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnIdentity,
    AuthnSpec,
    RefreshTokenCredentials,
)
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.authz.value_objects import (
    AuthzRequest,
    AuthzScope,
    AuthzSubject,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext, InvocationMetadata
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze.application.integrations.authn import AuthnOrchestrator
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import FrozenTimeSource, bind_time_source, str_key_selector
from forze_kits.aggregates.authn import (
    AuthnChangePasswordRequestDTO,
    AuthnKernelOp,
    AuthnLoginRequestDTO,
    AuthnPasswordResetAckDTO,
    AuthnRefreshRequestDTO,
    AuthnRequestPasswordResetDTO,
    AuthnResetPasswordDTO,
    AuthnTokenResponseDTO,
    build_authn_registry,
)
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters.identity import (
    MockAuthzDecisionPort,
    MockPasswordResetPort,
    MockTokenLifecyclePort,
    seed_password_account,
)
from tests.support.execution_context import context_from_modules

# ----------------------- #

AUTHN_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"password", "token"}))
AUTHZ_SPEC = AuthzSpec(name="main")


def _metadata() -> InvocationMetadata:
    return InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())


def _orchestrator(ctx: ExecutionContext) -> AuthnOrchestrator:
    port = ctx.authn.authn(AUTHN_SPEC)
    assert isinstance(port, AuthnOrchestrator)
    return port


async def _verify_access(ctx: ExecutionContext, token: str) -> AuthnIdentity:
    result = await _orchestrator(ctx).authenticate_with_token(
        AccessTokenCredentials(token=token),
    )
    return result.identity


def _assert_access_dead(token: str) -> Any:
    _ = token
    return pytest.raises(CoreException, match="Invalid token")


# ----------------------- #
# Orchestrator from the core path


def test_orchestrator_lives_in_core_and_identity_reexports_it() -> None:
    from forze.application.integrations.authn import (
        AuthnOrchestrator as core_orchestrator,
    )
    from forze_identity.authn import AuthnOrchestrator as facade_orchestrator

    assert core_orchestrator is facade_orchestrator
    assert core_orchestrator.__module__ == (
        "forze.application.integrations.authn.orchestrator"
    )


async def test_authn_dep_key_resolves_core_orchestrator() -> None:
    ctx = context_from_modules(MockDepsModule())

    assert isinstance(_orchestrator(ctx), AuthnOrchestrator)


# ----------------------- #
# Full identity flow through the kits authn registry


class TestIdentityFlows:
    """password login → verify → refresh/reuse → change password → logout."""

    def _env(self) -> tuple[MockDepsModule, ExecutionContext, Any]:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        reg = build_authn_registry(AUTHN_SPEC).freeze()
        return mod, ctx, reg

    # ....................... #

    async def _login(
        self,
        reg: Any,
        ctx: ExecutionContext,
        *,
        login: str = "alice",
        password: str,
    ) -> AuthnTokenResponseDTO:
        return await run_operation(
            reg,
            AUTHN_SPEC.default_namespace.key(AuthnKernelOp.PASSWORD_LOGIN),
            AuthnLoginRequestDTO(login=login, password=password),
            ctx,
        )

    # ....................... #

    async def test_full_password_login_refresh_change_logout_flow(self) -> None:
        mod, ctx, reg = self._env()
        principal_id = uuid4()
        seed_password_account(
            mod.state,
            login="alice",
            password="pw-1",
            principal_id=principal_id,
        )
        ns = AUTHN_SPEC.default_namespace
        identity = AuthnIdentity(principal_id=principal_id)

        # 1. Password login issues an opaque token pair via the mock lifecycle.
        first = await self._login(reg, ctx, password="pw-1")
        assert first.access_token is not None
        assert first.refresh_token is not None
        assert first.access_token.startswith("mock_access_")
        assert first.refresh_token.startswith("mock_refresh_")
        assert first.access_expires_in is not None
        assert first.refresh_expires_in is not None

        # 2. The issued access token verifies through the core orchestrator and
        #    resolves back to the seeded principal.
        verified = await _verify_access(ctx, first.access_token)
        assert verified.principal_id == principal_id

        # 3. Refresh rotates: the old refresh token becomes single-use.
        second = await run_operation(
            reg,
            ns.key(AuthnKernelOp.REFRESH_TOKENS),
            AuthnRefreshRequestDTO(refresh_token=first.refresh_token),
            ctx,
        )
        assert second.access_token != first.access_token
        assert second.refresh_token != first.refresh_token
        assert (await _verify_access(ctx, second.access_token)).principal_id == (
            principal_id
        )

        # Rotation binds access tokens to sessions: the rotated session's access
        # token stops verifying (session-bound real-verifier semantics).
        with _assert_access_dead(first.access_token):
            await _verify_access(ctx, first.access_token)

        # 4. Reuse of the rotated refresh token revokes the whole family.
        with pytest.raises(CoreException, match="Invalid refresh token") as ei:
            await run_operation(
                reg,
                ns.key(AuthnKernelOp.REFRESH_TOKENS),
                AuthnRefreshRequestDTO(refresh_token=first.refresh_token),
                ctx,
            )
        assert ei.value.kind is ExceptionKind.AUTHENTICATION

        with _assert_access_dead(second.access_token):
            await _verify_access(ctx, second.access_token)

        with pytest.raises(CoreException, match="Invalid refresh token"):
            await run_operation(
                reg,
                ns.key(AuthnKernelOp.REFRESH_TOKENS),
                AuthnRefreshRequestDTO(refresh_token=second.refresh_token),
                ctx,
            )

        # 5. Change password (round-5 semantics): re-authenticates with the
        #    current password, then revokes every session of the principal.
        third = await self._login(reg, ctx, password="pw-1")

        with ctx.inv_ctx.bind(metadata=_metadata(), authn=identity):
            await run_operation(
                reg,
                ns.key(AuthnKernelOp.CHANGE_PASSWORD),
                AuthnChangePasswordRequestDTO(
                    current_password="pw-1",
                    new_password="pw-2",
                ),
                ctx,
            )

        with _assert_access_dead(third.access_token):
            await _verify_access(ctx, third.access_token)

        with pytest.raises(CoreException, match="Invalid login or password"):
            await self._login(reg, ctx, password="pw-1")

        # 6. Logout revokes the fresh session; access verification fails after.
        fourth = await self._login(reg, ctx, password="pw-2")
        assert (await _verify_access(ctx, fourth.access_token)).principal_id == (
            principal_id
        )

        with ctx.inv_ctx.bind(metadata=_metadata(), authn=identity):
            await run_operation(reg, ns.key(AuthnKernelOp.LOGOUT), None, ctx)

        with _assert_access_dead(fourth.access_token):
            await _verify_access(ctx, fourth.access_token)

    # ....................... #

    async def test_change_password_rejects_wrong_current_password(self) -> None:
        mod, ctx, reg = self._env()
        principal_id = uuid4()
        seed_password_account(
            mod.state,
            login="alice",
            password="pw-1",
            principal_id=principal_id,
        )

        with ctx.inv_ctx.bind(
            metadata=_metadata(),
            authn=AuthnIdentity(principal_id=principal_id),
        ):
            with pytest.raises(CoreException) as ei:
                await run_operation(
                    reg,
                    AUTHN_SPEC.default_namespace.key(AuthnKernelOp.CHANGE_PASSWORD),
                    AuthnChangePasswordRequestDTO(
                        current_password="wrong",
                        new_password="pw-2",
                    ),
                    ctx,
                )

        assert ei.value.kind is ExceptionKind.AUTHENTICATION
        assert ei.value.code == "invalid_credentials"

    # ....................... #

    async def test_change_password_unknown_principal_uniform_error(self) -> None:
        _mod, ctx, reg = self._env()

        with ctx.inv_ctx.bind(
            metadata=_metadata(),
            authn=AuthnIdentity(principal_id=uuid4()),
        ):
            with pytest.raises(CoreException, match="Password account not found"):
                await run_operation(
                    reg,
                    AUTHN_SPEC.default_namespace.key(AuthnKernelOp.CHANGE_PASSWORD),
                    AuthnChangePasswordRequestDTO(
                        current_password="x",
                        new_password="y",
                    ),
                    ctx,
                )


# ----------------------- #
# Token lifecycle details


class TestMockTokenLifecycle:
    async def test_empty_refresh_token_is_required(self) -> None:
        lifecycle = MockTokenLifecyclePort(state=MockState())

        with pytest.raises(CoreException, match="Refresh token is required"):
            await lifecycle.refresh_tokens(RefreshTokenCredentials(token=""))

    # ....................... #

    async def test_expired_refresh_token_rejected(self) -> None:
        state = MockState()
        lifecycle = MockTokenLifecyclePort(
            state=state,
            refresh_expires_in=timedelta(seconds=-1),
        )
        tokens = await lifecycle.issue_tokens(AuthnIdentity(principal_id=uuid4()))
        assert tokens.refresh is not None

        with pytest.raises(CoreException, match="Refresh token expired"):
            await lifecycle.refresh_tokens(tokens.refresh.token)

    # ....................... #

    async def test_expired_access_token_fails_verification(self) -> None:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        lifecycle = MockTokenLifecyclePort(
            state=mod.state,
            access_expires_in=timedelta(seconds=-1),
        )
        tokens = await lifecycle.issue_tokens(AuthnIdentity(principal_id=uuid4()))

        with _assert_access_dead(tokens.access.token.token):
            await _verify_access(ctx, tokens.access.token.token)

    # ....................... #

    async def test_revoke_tokens_kills_all_principal_sessions(self) -> None:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        lifecycle = MockTokenLifecyclePort(state=mod.state)
        identity = AuthnIdentity(principal_id=uuid4())

        one = await lifecycle.issue_tokens(identity)
        two = await lifecycle.issue_tokens(identity)

        await lifecycle.revoke_tokens(identity)

        for bundle in (one, two):
            with _assert_access_dead(bundle.access.token.token):
                await _verify_access(ctx, bundle.access.token.token)

            assert bundle.refresh is not None
            with pytest.raises(CoreException, match="Invalid refresh token"):
                await lifecycle.refresh_tokens(bundle.refresh.token)

    # ....................... #

    async def test_seeded_static_tokens_still_verify(self) -> None:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        store = mod.state.identity["authn"].setdefault("main", {})
        store.setdefault("tokens", {})["static-token"] = {"subject": "static-user"}

        # Lifecycle-issued tokens coexist with seeded static ones.
        lifecycle = MockTokenLifecyclePort(state=mod.state)
        await lifecycle.issue_tokens(AuthnIdentity(principal_id=uuid4()))

        result = await _orchestrator(ctx).authenticate_with_token(
            AccessTokenCredentials(token="static-token"),
        )
        assert result.identity is not None

    # ....................... #

    async def test_tenant_bound_issue_round_trips_tenant_hint(self) -> None:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        lifecycle = MockTokenLifecyclePort(state=mod.state)
        tenant_id = uuid4()

        tokens = await lifecycle.issue_tokens(
            AuthnIdentity(principal_id=uuid4()),
            tenant_id=tenant_id,
        )

        result = await _orchestrator(ctx).authenticate_with_token(
            AccessTokenCredentials(token=tokens.access.token.token),
        )
        assert result.issuer_tenant_hint == str(tenant_id)

    # ....................... #

    async def test_sessions_participate_in_strict_transactions(self) -> None:
        """Sessions are identity-plane stores: a strict rollback discards them."""

        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))
        lifecycle = ctx.authn.token_lifecycle(AUTHN_SPEC)
        token: str | None = None

        with pytest.raises(RuntimeError, match="boom"):
            async with ctx.tx_ctx.scope("mock"):
                tokens = await lifecycle.issue_tokens(
                    AuthnIdentity(principal_id=uuid4()),
                )
                token = tokens.access.token.token
                raise RuntimeError("boom")

        assert token is not None
        with _assert_access_dead(token):
            await _verify_access(ctx, token)

    # ....................... #

    async def test_issue_inside_read_only_strict_root_raises(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))
        lifecycle = ctx.authn.token_lifecycle(AUTHN_SPEC)

        with pytest.raises(CoreException) as ei:
            async with ctx.tx_ctx.scope("mock", read_only=True):
                await lifecycle.issue_tokens(AuthnIdentity(principal_id=uuid4()))

        assert ei.value.code == "read_only_tx"


# ----------------------- #
# Self-service password reset


def _reset_tokens(state: MockState) -> dict[str, Any]:
    return state.identity["authn"]["main"].get("password_resets", {})  # type: ignore[index,union-attr,return-value]


class TestMockPasswordReset:
    """request → out-of-band token → confirm, against pure mock deps."""

    async def test_full_reset_flow_through_the_kits_registry(self) -> None:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        reg = build_authn_registry(AUTHN_SPEC).freeze()
        ns = AUTHN_SPEC.default_namespace
        principal_id = uuid4()
        seed_password_account(
            mod.state,
            login="alice",
            password="pw-1",
            principal_id=principal_id,
        )

        # An open session that the reset must kill.
        login = await run_operation(
            reg,
            ns.key(AuthnKernelOp.PASSWORD_LOGIN),
            AuthnLoginRequestDTO(login="alice", password="pw-1"),
            ctx,
        )
        assert login.access_token is not None

        # 1. Known and unknown logins get the byte-identical uniform ack.
        known_ack = await run_operation(
            reg,
            ns.key(AuthnKernelOp.REQUEST_PASSWORD_RESET),
            AuthnRequestPasswordResetDTO(login="alice"),
            ctx,
        )
        unknown_ack = await run_operation(
            reg,
            ns.key(AuthnKernelOp.REQUEST_PASSWORD_RESET),
            AuthnRequestPasswordResetDTO(login="nobody"),
            ctx,
        )
        assert isinstance(known_ack, AuthnPasswordResetAckDTO)
        assert known_ack == unknown_ack

        # 2. The token reached mock state (the out-of-band channel), not the ack.
        tokens = [
            token
            for token, record in _reset_tokens(mod.state).items()
            if record["used_at"] is None
        ]
        assert len(tokens) == 1
        token = tokens[0]
        assert token.startswith("mock_reset_")
        assert token not in str(known_ack.model_dump())

        # 3. Confirm sets the new password and consumes the token.
        await run_operation(
            reg,
            ns.key(AuthnKernelOp.RESET_PASSWORD),
            AuthnResetPasswordDTO(token=token, new_password="pw-2"),
            ctx,
        )

        # 4. Sessions revoked ("log out everywhere").
        with _assert_access_dead(login.access_token):
            await _verify_access(ctx, login.access_token)

        # 5. Single use: the consumed token is uniformly rejected.
        with pytest.raises(CoreException, match="Invalid or expired reset token"):
            await run_operation(
                reg,
                ns.key(AuthnKernelOp.RESET_PASSWORD),
                AuthnResetPasswordDTO(token=token, new_password="pw-3"),
                ctx,
            )

        # 6. Old password dead; the new one logs in and verifies.
        with pytest.raises(CoreException, match="Invalid login or password"):
            await run_operation(
                reg,
                ns.key(AuthnKernelOp.PASSWORD_LOGIN),
                AuthnLoginRequestDTO(login="alice", password="pw-1"),
                ctx,
            )

        fresh = await run_operation(
            reg,
            ns.key(AuthnKernelOp.PASSWORD_LOGIN),
            AuthnLoginRequestDTO(login="alice", password="pw-2"),
            ctx,
        )
        assert fresh.access_token is not None
        verified = await _verify_access(ctx, fresh.access_token)
        assert verified.principal_id == principal_id

    # ....................... #

    async def test_unknown_login_returns_none_at_the_port(self) -> None:
        port = MockPasswordResetPort(state=MockState())

        assert await port.request_reset("nobody") is None
        assert await port.request_reset("") is None

    # ....................... #

    async def test_new_request_supersedes_the_outstanding_reset(self) -> None:
        state = MockState()
        seed_password_account(
            state,
            login="alice",
            password="pw-1",
            principal_id=uuid4(),
        )
        port = MockPasswordResetPort(state=state)

        first = await port.request_reset("alice")
        second = await port.request_reset("alice")
        assert first is not None and second is not None
        assert first.token != second.token

        # Single active reset: the superseded token is uniformly rejected …
        with pytest.raises(CoreException, match="Invalid or expired reset token"):
            await port.reset_password(first.token, "pw-2")

        # … while the fresh one works.
        await port.reset_password(second.token, "pw-2")

    # ....................... #

    async def test_ttl_expiry_via_frozen_time(self) -> None:
        state = MockState()
        seed_password_account(
            state,
            login="alice",
            password="pw-1",
            principal_id=uuid4(),
        )
        port = MockPasswordResetPort(state=state)

        issued_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)

        with bind_time_source(FrozenTimeSource(issued_at)):
            issued = await port.request_reset("alice")

        assert issued is not None
        assert issued.expires_at == issued_at + timedelta(hours=1)

        # One second past expiry: the uniform error, not a special "expired" one.
        with bind_time_source(
            FrozenTimeSource(issued_at + timedelta(hours=1, seconds=1)),
        ):
            with pytest.raises(CoreException, match="Invalid or expired reset token") as ei:
                await port.reset_password(issued.token, "pw-2")

        assert ei.value.kind is ExceptionKind.AUTHENTICATION

        # Inside the window the same token would have worked.
        with bind_time_source(FrozenTimeSource(issued_at + timedelta(minutes=59))):
            await port.reset_password(issued.token, "pw-2")

    # ....................... #

    async def test_garbage_and_empty_tokens_uniformly_rejected(self) -> None:
        port = MockPasswordResetPort(state=MockState())

        for bad in ("", "nope", "mock_reset_unknown"):
            with pytest.raises(CoreException, match="Invalid or expired reset token"):
                await port.reset_password(bad, "pw-2")

    # ....................... #

    async def test_request_inside_read_only_strict_root_raises(self) -> None:
        state = MockState()
        seed_password_account(
            state,
            login="alice",
            password="pw-1",
            principal_id=uuid4(),
        )
        ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))
        port = ctx.authn.password_reset(AUTHN_SPEC)

        with pytest.raises(CoreException) as ei:
            async with ctx.tx_ctx.scope("mock", read_only=True):
                await port.request_reset("alice")

        assert ei.value.code == "read_only_tx"


# ----------------------- #
# Grant-aware authz decision


def _request(
    principal_id: UUID,
    action: str,
    *,
    tenant_id: UUID | None = None,
) -> AuthzRequest:
    return AuthzRequest(
        subject=AuthzSubject(principal_id=principal_id),
        action=action,
        scope=AuthzScope(tenant_id=tenant_id),
    )


class TestGrantAwareAuthzDecision:
    async def test_no_grants_seeded_falls_back_to_constant(self) -> None:
        state = MockState()
        principal_id = uuid4()

        deny = MockAuthzDecisionPort(state=state)
        allow = MockAuthzDecisionPort(state=state, allow_by_default=True)

        assert (await deny.authorize(_request(principal_id, "doc.read"))).allowed is (
            False
        )
        assert (await allow.authorize(_request(principal_id, "doc.read"))).allowed is (
            True
        )

    # ....................... #

    async def test_seeded_grants_switch_to_evaluation(self) -> None:
        state = MockState()
        port = MockAuthzDecisionPort(state=state, allow_by_default=True)
        granted = uuid4()
        other = uuid4()

        port.seed_grant(granted, "doc.read")

        allowed = await port.authorize(_request(granted, "doc.read"))
        assert allowed.allowed is True
        assert allowed.matched_permission_key == "doc.read"

        # Once grants exist the constant no longer applies — even permissive
        # stubs evaluate (no grant for the action / the other principal).
        denied_action = await port.authorize(_request(granted, "doc.write"))
        assert denied_action.allowed is False
        assert denied_action.reason == "No grant for permission 'doc.write'"

        denied_subject = await port.authorize(_request(other, "doc.read"))
        assert denied_subject.allowed is False

    # ....................... #

    async def test_tenant_scoped_grant_rules(self) -> None:
        state = MockState()
        port = MockAuthzDecisionPort(state=state)
        principal_id = uuid4()
        tenant_id = uuid4()

        port.seed_grant(principal_id, "doc.write", tenant_id=tenant_id)

        in_tenant = await port.authorize(
            _request(principal_id, "doc.write", tenant_id=tenant_id),
        )
        assert in_tenant.allowed is True

        out_of_tenant = await port.authorize(_request(principal_id, "doc.write"))
        assert out_of_tenant.allowed is False

        wrong_tenant = await port.authorize(
            _request(principal_id, "doc.write", tenant_id=uuid4()),
        )
        assert wrong_tenant.allowed is False

        # A global grant (no tenant) matches any scope.
        port.seed_grant(principal_id, "doc.read")
        assert (
            await port.authorize(
                _request(principal_id, "doc.read", tenant_id=tenant_id),
            )
        ).allowed is True

    # ....................... #

    async def test_seed_grant_requires_bound_state(self) -> None:
        with pytest.raises(CoreException, match="requires a bound MockState"):
            MockAuthzDecisionPort().seed_grant(uuid4(), "doc.read")

    # ....................... #

    def test_stateless_port_keeps_constant_behavior(self) -> None:
        # Construction shape of the pre-grant rounds keeps working.
        assert MockAuthzDecisionPort().allow_by_default is False
        assert MockAuthzDecisionPort(allow_by_default=True).allow_by_default is True


# ----------------------- #
# Authz-guarded operations against MockDepsModule


def _guarded_registry(action: str) -> Any:
    async def _handler(_args: Any) -> str:
        return "ok"

    return (
        OperationRegistry(handlers={"doc.read": lambda _ctx: _handler})
        .patch(str_key_selector.exact("doc.read"))
        .bind_outer()
        .before(
            AuthzBeforeAuthorize(spec=AUTHZ_SPEC, action=action).to_step(
                step_id="authz_guard",
                requires=(),
            ),
        )
        .finish(deep=True)
        .freeze()
    )


class TestAuthzGuardedOperations:
    async def test_guarded_op_denied_without_grant_allowed_with_one(self) -> None:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        reg = _guarded_registry("doc.read")
        principal_id = uuid4()
        identity = AuthnIdentity(principal_id=principal_id)

        with ctx.inv_ctx.bind(metadata=_metadata(), authn=identity):
            run = reg.resolve("doc.read", ctx)

            with pytest.raises(CoreException) as ei:
                await run(None)
            assert ei.value.kind is ExceptionKind.AUTHORIZATION
            assert ei.value.code == "permission_denied"

            MockAuthzDecisionPort(state=mod.state).seed_grant(
                principal_id,
                "doc.read",
            )

            assert await run(None) == "ok"

    # ....................... #

    async def test_guarded_op_tenant_scoped_grant(self) -> None:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        reg = _guarded_registry("doc.read")
        principal_id = uuid4()
        tenant_id = uuid4()
        identity = AuthnIdentity(principal_id=principal_id)
        tenant = TenantIdentity(tenant_id=tenant_id)

        MockAuthzDecisionPort(state=mod.state).seed_grant(
            principal_id,
            "doc.read",
            tenant_id=tenant_id,
        )

        # Without the tenant bound, the tenant-scoped grant does not apply.
        with ctx.inv_ctx.bind(metadata=_metadata(), authn=identity):
            run = reg.resolve("doc.read", ctx)

            with pytest.raises(CoreException) as ei:
                await run(None)
            assert ei.value.kind is ExceptionKind.AUTHORIZATION

        # With the tenant bound, the policy scope matches the seeded grant.
        with ctx.inv_ctx.bind(metadata=_metadata(), authn=identity, tenant=tenant):
            run = reg.resolve("doc.read", ctx)
            assert await run(None) == "ok"

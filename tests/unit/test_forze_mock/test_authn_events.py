"""Authn event emission matrix + login lockout against pure mock deps.

Runs the framework's own flows (kits authn registry + core orchestrator) over
:class:`MockDepsModule` with ``authn_events=True`` and asserts the recorded
:class:`AuthnEvent` stream on ``state.authn_events`` — including the privacy
contract (events carry the login digest, never the raw login) and the
fixed-window lockout behavior over the mock counter (frozen-time driven).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.authn import (
    AuthnEventKind,
    AuthnIdentity,
    AuthnSpec,
    login_digest,
)
from forze.application.execution import ExecutionContext, InvocationMetadata
from forze.application.execution.operations import run_operation
from forze.application.integrations.authn import LockoutConfig
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze_kits.aggregates.authn import (
    AuthnChangePasswordRequestDTO,
    AuthnKernelOp,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnRequestPasswordResetDTO,
    AuthnResetPasswordDTO,
    build_authn_registry,
)
from forze_mock import MockDepsModule
from forze_mock.adapters.identity import seed_password_account
from tests.support.execution_context import context_from_modules

# ----------------------- #

AUTHN_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"password", "token"}))
T0 = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)


def _metadata() -> InvocationMetadata:
    return InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())


def _env(
    **module_kwargs: Any,
) -> tuple[MockDepsModule, ExecutionContext, Any]:
    mod = MockDepsModule(authn_events=True, **module_kwargs)
    ctx = context_from_modules(mod)
    reg = build_authn_registry(AUTHN_SPEC).freeze()
    return mod, ctx, reg


def _kinds(mod: MockDepsModule) -> list[AuthnEventKind]:
    return [event.kind for event in mod.state.authn_events]


async def _login(
    reg: Any,
    ctx: ExecutionContext,
    *,
    login: str = "alice",
    password: str,
) -> Any:
    return await run_operation(
        reg,
        AUTHN_SPEC.default_namespace.key(AuthnKernelOp.PASSWORD_LOGIN),
        AuthnLoginRequestDTO(login=login, password=password),
        ctx,
    )


# ----------------------- #
# Emission matrix per flow


class TestEmissionMatrix:
    async def test_login_success_and_failure(self) -> None:
        mod, ctx, reg = _env()
        seed_password_account(
            mod.state, login="alice", password="pw-1", principal_id=uuid4()
        )

        await _login(reg, ctx, password="pw-1")

        with pytest.raises(CoreException):
            await _login(reg, ctx, password="wrong")

        assert _kinds(mod) == [
            AuthnEventKind.LOGIN_SUCCEEDED,
            AuthnEventKind.LOGIN_FAILED,
        ]

        succeeded, failed = mod.state.authn_events
        assert succeeded.principal_id is not None
        assert failed.principal_id is None
        for event in mod.state.authn_events:
            assert event.route == "main"
            assert event.login_digest == login_digest("alice")
            assert event.login_digest != "alice"

    async def test_refresh_reuse_and_logout(self) -> None:
        mod, ctx, reg = _env()
        principal_id = uuid4()
        seed_password_account(
            mod.state, login="alice", password="pw-1", principal_id=principal_id
        )
        ns = AUTHN_SPEC.default_namespace

        first = await _login(reg, ctx, password="pw-1")

        # Rotation → TOKEN_REFRESHED.
        await run_operation(
            reg,
            ns.key(AuthnKernelOp.REFRESH_TOKENS),
            AuthnRefreshRequestDTO(refresh_token=first.refresh_token),
            ctx,
        )

        # Presenting the rotated token again → REFRESH_REUSE_DETECTED.
        with pytest.raises(CoreException, match="Invalid refresh token"):
            await run_operation(
                reg,
                ns.key(AuthnKernelOp.REFRESH_TOKENS),
                AuthnRefreshRequestDTO(refresh_token=first.refresh_token),
                ctx,
            )

        # Logout → LOGOUT.
        with ctx.inv_ctx.bind(
            metadata=_metadata(),
            authn=AuthnIdentity(principal_id=principal_id),
        ):
            await run_operation(reg, ns.key(AuthnKernelOp.LOGOUT), None, ctx)

        kinds = _kinds(mod)
        assert kinds[0] is AuthnEventKind.LOGIN_SUCCEEDED
        assert AuthnEventKind.TOKEN_REFRESHED in kinds
        assert AuthnEventKind.REFRESH_REUSE_DETECTED in kinds
        assert kinds[-1] is AuthnEventKind.LOGOUT

        refreshed = next(
            event
            for event in mod.state.authn_events
            if event.kind is AuthnEventKind.TOKEN_REFRESHED
        )
        reuse = next(
            event
            for event in mod.state.authn_events
            if event.kind is AuthnEventKind.REFRESH_REUSE_DETECTED
        )
        assert refreshed.principal_id == principal_id
        assert reuse.principal_id == principal_id

    async def test_change_password(self) -> None:
        mod, ctx, reg = _env()
        principal_id = uuid4()
        seed_password_account(
            mod.state, login="alice", password="pw-1", principal_id=principal_id
        )

        with ctx.inv_ctx.bind(
            metadata=_metadata(),
            authn=AuthnIdentity(principal_id=principal_id),
        ):
            await run_operation(
                reg,
                AUTHN_SPEC.default_namespace.key(AuthnKernelOp.CHANGE_PASSWORD),
                AuthnChangePasswordRequestDTO(
                    current_password="pw-1",
                    new_password="pw-2",
                ),
                ctx,
            )

        assert _kinds(mod) == [AuthnEventKind.PASSWORD_CHANGED]
        assert mod.state.authn_events[0].principal_id == principal_id

    async def test_reset_requested_only_on_actual_issuance_and_completed(self) -> None:
        mod, ctx, reg = _env()
        principal_id = uuid4()
        seed_password_account(
            mod.state, login="alice", password="pw-1", principal_id=principal_id
        )
        ns = AUTHN_SPEC.default_namespace

        # Unknown login: the caller still gets the uniform ack, but no token was
        # issued — so nothing is recorded.
        await run_operation(
            reg,
            ns.key(AuthnKernelOp.REQUEST_PASSWORD_RESET),
            AuthnRequestPasswordResetDTO(login="nobody"),
            ctx,
        )
        assert _kinds(mod) == []

        # Known login: PASSWORD_RESET_REQUESTED with the login digest.
        await run_operation(
            reg,
            ns.key(AuthnKernelOp.REQUEST_PASSWORD_RESET),
            AuthnRequestPasswordResetDTO(login="alice"),
            ctx,
        )
        assert _kinds(mod) == [AuthnEventKind.PASSWORD_RESET_REQUESTED]
        requested = mod.state.authn_events[0]
        assert requested.principal_id == principal_id
        assert requested.login_digest == login_digest("alice")
        assert requested.login_digest != "alice"

        # Consume the token → PASSWORD_RESET_COMPLETED.
        token = next(
            token
            for token, record in mod.state.identity["authn"]["main"][
                "password_resets"
            ].items()
            if record["used_at"] is None
        )
        await run_operation(
            reg,
            ns.key(AuthnKernelOp.RESET_PASSWORD),
            AuthnResetPasswordDTO(token=token, new_password="pw-2"),
            ctx,
        )

        assert _kinds(mod) == [
            AuthnEventKind.PASSWORD_RESET_REQUESTED,
            AuthnEventKind.PASSWORD_RESET_COMPLETED,
        ]
        assert mod.state.authn_events[-1].principal_id == principal_id

    async def test_principal_deactivation(self) -> None:
        mod, ctx, _reg = _env()
        principal_id = uuid4()

        port = ctx.authn.principal_deactivation(AUTHN_SPEC)
        await port.deactivate(principal_id)

        assert _kinds(mod) == [AuthnEventKind.PRINCIPAL_DEACTIVATED]
        assert mod.state.authn_events[0].principal_id == principal_id

    async def test_raw_login_never_reaches_the_event_stream(self) -> None:
        mod, ctx, reg = _env()
        seed_password_account(
            mod.state, login="Alice@Example.com", password="pw-1", principal_id=uuid4()
        )

        await _login(reg, ctx, login="Alice@Example.com", password="pw-1")

        (event,) = mod.state.authn_events
        assert event.login_digest == login_digest("alice@example.com")
        assert "alice" not in (event.login_digest or "")
        assert "Alice@Example.com" not in str(event)

    async def test_events_off_by_default(self) -> None:
        mod = MockDepsModule()
        ctx = context_from_modules(mod)
        reg = build_authn_registry(AUTHN_SPEC).freeze()
        seed_password_account(
            mod.state, login="alice", password="pw-1", principal_id=uuid4()
        )

        await _login(reg, ctx, password="pw-1")

        assert mod.state.authn_events == []


# ----------------------- #
# Full-flow login lockout (fixed window, frozen time)


class TestLoginLockoutFullFlow:
    async def test_wrong_password_locks_then_window_advance_unlocks(self) -> None:
        mod, ctx, reg = _env(
            lockout=LockoutConfig(threshold=5, window=timedelta(minutes=15)),
        )
        principal_id = uuid4()
        seed_password_account(
            mod.state, login="alice", password="pw-1", principal_id=principal_id
        )

        with bind_time_source(FrozenTimeSource(T0)):
            # Five wrong passwords: each one is the uniform authentication
            # failure (the lockout gate never preempts attempts below the
            # threshold).
            for _ in range(5):
                with pytest.raises(CoreException) as ei:
                    await _login(reg, ctx, password="wrong")
                assert ei.value.kind is ExceptionKind.AUTHENTICATION

            # Locked: even the correct password is throttled (429-mapped kind),
            # before password verification.
            with pytest.raises(CoreException) as ei:
                await _login(reg, ctx, password="pw-1")

            assert ei.value.kind is ExceptionKind.THROTTLED
            assert ei.value.code == "login_locked"

        # Window rollover unlocks; the correct login succeeds and is recorded.
        with bind_time_source(FrozenTimeSource(T0 + timedelta(minutes=15))):
            result = await _login(reg, ctx, password="pw-1")
            assert result.access_token is not None

        kinds = _kinds(mod)
        assert kinds == (
            [AuthnEventKind.LOGIN_FAILED] * 5
            + [AuthnEventKind.LOGIN_LOCKED, AuthnEventKind.LOGIN_SUCCEEDED]
        )

    async def test_nonexistent_login_locks_identically(self) -> None:
        """No-enumeration: a login that maps to no account locks the same way."""

        mod, ctx, reg = _env(
            lockout=LockoutConfig(threshold=2, window=timedelta(minutes=15)),
        )

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(2):
                with pytest.raises(CoreException) as ei:
                    await _login(reg, ctx, login="nobody", password="x")
                assert ei.value.kind is ExceptionKind.AUTHENTICATION

            with pytest.raises(CoreException) as ei:
                await _login(reg, ctx, login="nobody", password="x")

            assert ei.value.kind is ExceptionKind.THROTTLED
            assert ei.value.code == "login_locked"

        # Counter keys carry the digest, never the raw login string.
        suffixes = [suffix for (_ns, suffix) in mod.state.counters]
        assert suffixes
        assert all("nobody" not in (suffix or "") for suffix in suffixes)
        assert any(login_digest("nobody") in (suffix or "") for suffix in suffixes)

    async def test_success_resets_the_window(self) -> None:
        mod, ctx, reg = _env(
            lockout=LockoutConfig(threshold=2, window=timedelta(minutes=15)),
        )
        seed_password_account(
            mod.state, login="alice", password="pw-1", principal_id=uuid4()
        )

        with bind_time_source(FrozenTimeSource(T0)):
            with pytest.raises(CoreException):
                await _login(reg, ctx, password="wrong")

            await _login(reg, ctx, password="pw-1")  # resets the bucket

            # Two fresh failures are needed again before the lock engages.
            for _ in range(2):
                with pytest.raises(CoreException) as ei:
                    await _login(reg, ctx, password="wrong")
                assert ei.value.kind is ExceptionKind.AUTHENTICATION

            with pytest.raises(CoreException) as ei:
                await _login(reg, ctx, password="pw-1")
            assert ei.value.kind is ExceptionKind.THROTTLED

    async def test_lockout_off_by_default(self) -> None:
        mod, ctx, reg = _env()
        seed_password_account(
            mod.state, login="alice", password="pw-1", principal_id=uuid4()
        )

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(10):
                with pytest.raises(CoreException) as ei:
                    await _login(reg, ctx, password="wrong")
                assert ei.value.kind is ExceptionKind.AUTHENTICATION

            result = await _login(reg, ctx, password="pw-1")
            assert result.access_token is not None

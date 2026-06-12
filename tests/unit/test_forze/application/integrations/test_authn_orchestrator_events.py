"""Orchestrator password-path emission + lockout wiring (events optional, best-effort)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import (
    AuthnEvent,
    AuthnEventEmitter,
    AuthnEventKind,
    AuthnEventSink,
    AuthnIdentity,
    PasswordCredentials,
    VerifiedAssertion,
    login_digest,
)
from forze.application.contracts.counter import CounterPort
from forze.application.integrations.authn import (
    AuthnOrchestrator,
    LockoutConfig,
    LoginLockoutGuard,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import FrozenTimeSource, bind_time_source

pytestmark = pytest.mark.unit

# ----------------------- #

T0 = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
PRINCIPAL = uuid4()
GOOD = PasswordCredentials(login="alice", password="pw-1")
BAD = PasswordCredentials(login="alice", password="nope")


class _RecordingSink(AuthnEventSink):
    def __init__(self) -> None:
        self.events: list[AuthnEvent] = []

    async def record(self, event: AuthnEvent) -> None:
        self.events.append(event)


class _RaisingSink(AuthnEventSink):
    async def record(self, event: AuthnEvent) -> None:
        raise RuntimeError("sink down")


class _SpyVerifier:
    """Stub password verifier mirroring the uniform-failure contract."""

    def __init__(self, *, fail_with: CoreException | None = None) -> None:
        self.calls = 0
        self.fail_with = fail_with

    async def verify_password(self, credentials: PasswordCredentials) -> VerifiedAssertion:
        self.calls += 1

        if self.fail_with is not None:
            raise self.fail_with

        if credentials.password != "pw-1":
            raise exc.authentication(
                "Invalid login or password",
                code="invalid_credentials",
            )

        return VerifiedAssertion(issuer="test", subject=str(PRINCIPAL))


class _Resolver:
    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        return AuthnIdentity(principal_id=UUID(assertion.subject))


class _Eligibility:
    async def require_authentication_allowed(self, principal_id: UUID) -> None:
        _ = principal_id


class _MemoryCounter(CounterPort):
    def __init__(self) -> None:
        self.values: dict[str | None, int] = {}

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = self.values.get(suffix, 0) + by
        return self.values[suffix]

    async def incr_batch(self, size: int = 2, *, suffix: str | None = None) -> list[int]:
        prev = self.values.get(suffix, 0)
        self.values[suffix] = prev + size
        return list(range(prev + 1, prev + size + 1))

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = self.values.get(suffix, 0) - by
        return self.values[suffix]

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = value
        return value


def _orchestrator(
    *,
    verifier: _SpyVerifier | None = None,
    sink: AuthnEventSink | None = None,
    lockout: LoginLockoutGuard | None = None,
) -> tuple[AuthnOrchestrator, _SpyVerifier]:
    verifier = verifier or _SpyVerifier()
    orchestrator = AuthnOrchestrator(
        resolver=_Resolver(),
        eligibility=_Eligibility(),
        enabled_methods=frozenset({"password"}),
        password_verifier=verifier,
        events=AuthnEventEmitter(sink=sink, route="main") if sink is not None else None,
        lockout=lockout,
    )
    return orchestrator, verifier


def _guard(threshold: int = 3) -> tuple[LoginLockoutGuard, _MemoryCounter]:
    counter = _MemoryCounter()
    guard = LoginLockoutGuard(
        counter=counter,
        config=LockoutConfig(threshold=threshold, window=timedelta(minutes=15)),
    )
    return guard, counter


# ----------------------- #


class TestPasswordEmission:
    async def test_success_emits_login_succeeded_with_principal_and_digest(self) -> None:
        sink = _RecordingSink()
        orchestrator, _verifier = _orchestrator(sink=sink)

        result = await orchestrator.authenticate_with_password(GOOD)

        assert result.identity.principal_id == PRINCIPAL
        (event,) = sink.events
        assert event.kind is AuthnEventKind.LOGIN_SUCCEEDED
        assert event.principal_id == PRINCIPAL
        assert event.login_digest == login_digest("alice")
        assert event.login_digest != "alice"
        assert event.route == "main"

    async def test_failure_emits_login_failed_after_the_uniform_error(self) -> None:
        sink = _RecordingSink()
        orchestrator, verifier = _orchestrator(sink=sink)

        with pytest.raises(CoreException) as ei:
            await orchestrator.authenticate_with_password(BAD)

        assert ei.value.kind is ExceptionKind.AUTHENTICATION
        assert ei.value.code == "invalid_credentials"
        assert verifier.calls == 1  # the verifier ran (no pre-verifier branch)
        (event,) = sink.events
        assert event.kind is AuthnEventKind.LOGIN_FAILED
        assert event.principal_id is None
        assert event.login_digest == login_digest("alice")

    async def test_infrastructure_failures_emit_nothing(self) -> None:
        sink = _RecordingSink()
        orchestrator, _verifier = _orchestrator(
            verifier=_SpyVerifier(fail_with=exc.infrastructure("db down")),
            sink=sink,
        )

        with pytest.raises(CoreException) as ei:
            await orchestrator.authenticate_with_password(GOOD)

        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
        assert sink.events == []

    async def test_no_sink_emits_nothing_and_flow_is_unchanged(self) -> None:
        orchestrator, _verifier = _orchestrator()

        result = await orchestrator.authenticate_with_password(GOOD)

        assert result.identity.principal_id == PRINCIPAL

    async def test_raising_sink_never_fails_the_flow(self) -> None:
        orchestrator, _verifier = _orchestrator(sink=_RaisingSink())

        result = await orchestrator.authenticate_with_password(GOOD)
        assert result.identity.principal_id == PRINCIPAL

        with pytest.raises(CoreException) as ei:
            await orchestrator.authenticate_with_password(BAD)
        assert ei.value.kind is ExceptionKind.AUTHENTICATION

    async def test_method_disabled_emits_nothing(self) -> None:
        sink = _RecordingSink()
        orchestrator = AuthnOrchestrator(
            resolver=_Resolver(),
            eligibility=_Eligibility(),
            enabled_methods=frozenset({"token"}),
            token_verifier=object(),  # type: ignore[arg-type]
            events=AuthnEventEmitter(sink=sink, route="main"),
        )

        with pytest.raises(CoreException, match="not enabled"):
            await orchestrator.authenticate_with_password(GOOD)

        assert sink.events == []


class TestPasswordLockout:
    async def test_threshold_failures_then_locked_before_verification(self) -> None:
        sink = _RecordingSink()
        guard, _counter = _guard(threshold=3)
        orchestrator, verifier = _orchestrator(sink=sink, lockout=guard)

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(3):
                with pytest.raises(CoreException) as ei:
                    await orchestrator.authenticate_with_password(BAD)
                assert ei.value.kind is ExceptionKind.AUTHENTICATION

            assert verifier.calls == 3

            # The next attempt is throttled BEFORE the verifier runs — even with
            # the correct password.
            with pytest.raises(CoreException) as ei:
                await orchestrator.authenticate_with_password(GOOD)

            assert ei.value.kind is ExceptionKind.THROTTLED
            assert ei.value.code == "login_locked"
            assert verifier.calls == 3  # untouched: rejected pre-verification

        kinds = [event.kind for event in sink.events]
        assert kinds == [AuthnEventKind.LOGIN_FAILED] * 3 + [AuthnEventKind.LOGIN_LOCKED]
        assert sink.events[-1].login_digest == login_digest("alice")

    async def test_normal_failures_keep_timing_posture(self) -> None:
        """Below the threshold the verifier always runs — no early exit exists."""

        guard, _counter = _guard(threshold=5)
        orchestrator, verifier = _orchestrator(lockout=guard)

        with bind_time_source(FrozenTimeSource(T0)):
            for attempt in range(1, 5):
                with pytest.raises(CoreException):
                    await orchestrator.authenticate_with_password(BAD)
                assert verifier.calls == attempt

    async def test_success_resets_the_window(self) -> None:
        guard, _counter = _guard(threshold=3)
        orchestrator, _verifier = _orchestrator(lockout=guard)

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(2):
                with pytest.raises(CoreException):
                    await orchestrator.authenticate_with_password(BAD)

            await orchestrator.authenticate_with_password(GOOD)

            # The bucket was reset: three fresh failures are needed again.
            for _ in range(3):
                with pytest.raises(CoreException) as ei:
                    await orchestrator.authenticate_with_password(BAD)
                assert ei.value.kind is ExceptionKind.AUTHENTICATION

            with pytest.raises(CoreException) as ei:
                await orchestrator.authenticate_with_password(GOOD)
            assert ei.value.kind is ExceptionKind.THROTTLED

    async def test_window_rollover_unlocks(self) -> None:
        guard, _counter = _guard(threshold=2)
        orchestrator, _verifier = _orchestrator(lockout=guard)

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(2):
                with pytest.raises(CoreException):
                    await orchestrator.authenticate_with_password(BAD)

            with pytest.raises(CoreException) as ei:
                await orchestrator.authenticate_with_password(GOOD)
            assert ei.value.kind is ExceptionKind.THROTTLED

        with bind_time_source(FrozenTimeSource(T0 + timedelta(minutes=15))):
            result = await orchestrator.authenticate_with_password(GOOD)
            assert result.identity.principal_id == PRINCIPAL

    async def test_nonexistent_login_locks_identically(self) -> None:
        """No-enumeration: lockout keys on the login string, not on account existence."""

        guard, counter = _guard(threshold=2)
        orchestrator, verifier = _orchestrator(lockout=guard)
        ghost = PasswordCredentials(login="nobody", password="x")

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(2):
                with pytest.raises(CoreException) as ei:
                    await orchestrator.authenticate_with_password(ghost)
                assert ei.value.kind is ExceptionKind.AUTHENTICATION

            with pytest.raises(CoreException) as ei:
                await orchestrator.authenticate_with_password(ghost)

            assert ei.value.kind is ExceptionKind.THROTTLED
            assert ei.value.code == "login_locked"

        # Counter keys carry the digest of the nonexistent login, never the login.
        assert all("nobody" not in (suffix or "") for suffix in counter.values)
        assert any(login_digest("nobody") in (suffix or "") for suffix in counter.values)

    async def test_lockout_off_means_no_counter_traffic(self) -> None:
        orchestrator, _verifier = _orchestrator()

        with pytest.raises(CoreException):
            await orchestrator.authenticate_with_password(BAD)

        result = await orchestrator.authenticate_with_password(GOOD)
        assert result.identity.principal_id == PRINCIPAL

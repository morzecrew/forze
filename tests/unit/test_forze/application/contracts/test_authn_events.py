"""Unit tests for the authn event contract (VO, enum, digest, emit_safe, emitter)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.authn import (
    AuthnEvent,
    AuthnEventEmitter,
    AuthnEventKind,
    AuthnEventSink,
    AuthnEventSinkDepKey,
    AuthnSpec,
    emit_safe,
    login_digest,
    resolve_authn_event_emitter,
)
from forze.application.execution import Deps
from forze.base.primitives import FrozenTimeSource, bind_time_source
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

# ----------------------- #

NOW = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)


def _event(**overrides: object) -> AuthnEvent:
    kwargs: dict[str, object] = {"route": "main", "occurred_at": NOW}
    kwargs.update(overrides)
    return AuthnEvent(AuthnEventKind.LOGIN_FAILED, **kwargs)  # type: ignore[arg-type]


class _RecordingSink(AuthnEventSink):
    def __init__(self) -> None:
        self.events: list[AuthnEvent] = []

    async def record(self, event: AuthnEvent) -> None:
        self.events.append(event)


class _RaisingSink(AuthnEventSink):
    async def record(self, event: AuthnEvent) -> None:
        raise RuntimeError("sink down")


# ----------------------- #


class TestAuthnEventKind:
    def test_member_set(self) -> None:
        assert {kind.value for kind in AuthnEventKind} == {
            "login_succeeded",
            "login_failed",
            "login_locked",
            "token_refreshed",
            "refresh_reuse_detected",
            "logout",
            "password_changed",
            "password_reset_requested",
            "password_reset_completed",
            "principal_deactivated",
        }

    def test_str_enum_values(self) -> None:
        assert AuthnEventKind.LOGIN_LOCKED == "login_locked"


class TestAuthnEvent:
    def test_minimal_construction_defaults(self) -> None:
        event = _event()

        assert event.kind is AuthnEventKind.LOGIN_FAILED
        assert event.principal_id is None
        assert event.login_digest is None
        assert event.tenant_id is None
        assert event.route == "main"
        assert event.occurred_at == NOW
        assert dict(event.details) == {}

    def test_frozen(self) -> None:
        event = _event()

        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            event.kind = AuthnEventKind.LOGOUT  # type: ignore[misc]

    def test_kind_positional_rest_keyword(self) -> None:
        pid = uuid4()
        event = AuthnEvent(
            AuthnEventKind.LOGIN_SUCCEEDED,
            principal_id=pid,
            login_digest="d",
            route="r",
            occurred_at=NOW,
            details={"k": "v"},
        )

        assert event.principal_id == pid
        assert event.details["k"] == "v"


class TestLoginDigest:
    def test_matches_documented_construction(self) -> None:
        expected = hashlib.sha256(b"lockout:alice").hexdigest()

        assert login_digest("alice") == expected

    def test_lowercases_login(self) -> None:
        assert login_digest("Alice") == login_digest("alice")
        assert login_digest("ALICE@EXAMPLE.COM") == login_digest("alice@example.com")

    def test_stable_and_never_the_raw_login(self) -> None:
        digest = login_digest("alice")

        assert digest == login_digest("alice")
        assert digest != "alice"
        assert "alice" not in digest
        assert len(digest) == 64
        int(digest, 16)  # hex


class TestEmitSafe:
    async def test_no_sink_is_a_noop(self) -> None:
        await emit_safe(None, _event())

    async def test_records_through_the_sink(self) -> None:
        sink = _RecordingSink()
        event = _event()

        await emit_safe(sink, event)

        assert sink.events == [event]

    async def test_sink_failure_is_swallowed_and_logged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from forze.application.contracts.authn import events as events_module

        warnings: list[tuple[object, ...]] = []
        logger_stub = type(
            "L",
            (),
            {"warning": staticmethod(lambda *a, **kw: warnings.append(a))},
        )()
        monkeypatch.setattr(events_module, "logger", logger_stub)

        await emit_safe(_RaisingSink(), _event())  # must not raise

        assert len(warnings) == 1
        assert "login_failed" in warnings[0]


class TestAuthnEventEmitter:
    async def test_stamps_route_and_frozen_time(self) -> None:
        sink = _RecordingSink()
        emitter = AuthnEventEmitter(sink=sink, route="main")
        pid = uuid4()

        with bind_time_source(FrozenTimeSource(NOW)):
            await emitter.emit(
                AuthnEventKind.LOGIN_SUCCEEDED,
                principal_id=pid,
                login_digest=login_digest("alice"),
            )

        (event,) = sink.events
        assert event.kind is AuthnEventKind.LOGIN_SUCCEEDED
        assert event.route == "main"
        assert event.occurred_at == NOW
        assert event.principal_id == pid
        assert event.login_digest == login_digest("alice")
        assert dict(event.details) == {}

    async def test_never_raises_with_a_failing_sink(self) -> None:
        emitter = AuthnEventEmitter(sink=_RaisingSink(), route="main")

        await emitter.emit(AuthnEventKind.LOGOUT)  # must not raise


class TestResolveAuthnEventEmitter:
    SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"token"}))

    def test_no_registration_returns_none(self) -> None:
        ctx = context_from_deps(Deps())

        assert resolve_authn_event_emitter(ctx, self.SPEC) is None

    def test_routed_registration_resolves(self) -> None:
        sink = _RecordingSink()
        ctx = context_from_deps(
            Deps.routed(
                {AuthnEventSinkDepKey: {"main": lambda ctx, spec: sink}},
            ),
        )

        emitter = resolve_authn_event_emitter(ctx, self.SPEC)

        assert emitter is not None
        assert emitter.sink is sink
        assert emitter.route == "main"

    def test_plain_registration_is_shared_across_routes(self) -> None:
        sink = _RecordingSink()
        ctx = context_from_deps(
            Deps.plain({AuthnEventSinkDepKey: lambda ctx, spec: sink}),
        )

        emitter = resolve_authn_event_emitter(ctx, self.SPEC)

        assert emitter is not None
        assert emitter.sink is sink

    def test_other_route_registration_does_not_match(self) -> None:
        ctx = context_from_deps(
            Deps.routed(
                {AuthnEventSinkDepKey: {"other": lambda ctx, spec: _RecordingSink()}},
            ),
        )

        assert resolve_authn_event_emitter(ctx, self.SPEC) is None

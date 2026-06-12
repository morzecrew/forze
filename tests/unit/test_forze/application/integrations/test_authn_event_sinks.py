"""Unit tests for :class:`LoggingAuthnEventSink` (levels, privacy, never raises)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnEvent, AuthnEventKind, login_digest
from forze.application.integrations.authn import LoggingAuthnEventSink

pytestmark = pytest.mark.unit

# ----------------------- #

NOW = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)


class _LoggerStub:
    def __init__(self) -> None:
        self.infos: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.warnings: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.infos.append((args, kwargs))

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.warnings.append((args, kwargs))


@pytest.fixture
def logger_stub(monkeypatch: pytest.MonkeyPatch) -> _LoggerStub:
    from forze.application.integrations.authn import sinks as sinks_module

    stub = _LoggerStub()
    monkeypatch.setattr(sinks_module, "logger", stub)
    return stub


def _event(kind: AuthnEventKind, **overrides: object) -> AuthnEvent:
    kwargs: dict[str, object] = {"route": "main", "occurred_at": NOW}
    kwargs.update(overrides)
    return AuthnEvent(kind, **kwargs)  # type: ignore[arg-type]


# ----------------------- #


class TestLoggingAuthnEventSink:
    @pytest.mark.parametrize(
        "kind",
        [
            AuthnEventKind.LOGIN_FAILED,
            AuthnEventKind.LOGIN_LOCKED,
            AuthnEventKind.REFRESH_REUSE_DETECTED,
        ],
    )
    async def test_operator_relevant_kinds_log_at_warning(
        self,
        logger_stub: _LoggerStub,
        kind: AuthnEventKind,
    ) -> None:
        await LoggingAuthnEventSink().record(_event(kind))

        assert len(logger_stub.warnings) == 1
        assert not logger_stub.infos

    @pytest.mark.parametrize(
        "kind",
        [
            AuthnEventKind.LOGIN_SUCCEEDED,
            AuthnEventKind.TOKEN_REFRESHED,
            AuthnEventKind.LOGOUT,
            AuthnEventKind.PASSWORD_CHANGED,
            AuthnEventKind.PASSWORD_RESET_REQUESTED,
            AuthnEventKind.PASSWORD_RESET_COMPLETED,
            AuthnEventKind.PRINCIPAL_DEACTIVATED,
        ],
    )
    async def test_success_ish_kinds_log_at_info(
        self,
        logger_stub: _LoggerStub,
        kind: AuthnEventKind,
    ) -> None:
        await LoggingAuthnEventSink().record(_event(kind))

        assert len(logger_stub.infos) == 1
        assert not logger_stub.warnings

    async def test_structured_fields_carry_digest_never_a_login(
        self,
        logger_stub: _LoggerStub,
    ) -> None:
        pid = uuid4()
        digest = login_digest("alice")

        await LoggingAuthnEventSink().record(
            _event(
                AuthnEventKind.LOGIN_SUCCEEDED,
                principal_id=pid,
                login_digest=digest,
                details={"extra": "context"},
            ),
        )

        ((args, kwargs),) = logger_stub.infos
        assert "login_succeeded" in args
        assert "main" in args
        assert kwargs["principal_id"] == str(pid)
        assert kwargs["login_digest"] == digest
        assert kwargs["extra"] == "context"
        assert "alice" not in str(args) + str(kwargs)

    async def test_never_raises_even_when_the_logger_does(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from forze.application.integrations.authn import sinks as sinks_module

        class _Exploding:
            def info(self, *args: Any, **kwargs: Any) -> None:
                raise RuntimeError("backend down")

            def warning(self, *args: Any, **kwargs: Any) -> None:
                raise RuntimeError("backend down")

        monkeypatch.setattr(sinks_module, "logger", _Exploding())

        await LoggingAuthnEventSink().record(_event(AuthnEventKind.LOGIN_FAILED))

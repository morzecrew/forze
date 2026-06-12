"""Event emission from the real identity adapters (token/password/reset/deactivation).

The mock-plane matrix (``tests/unit/test_forze_mock/test_authn_events.py``)
covers the flow semantics; these tests pin the same emissions on the
document-backed adapters themselves, with mocked document ports.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import (
    AuthnEvent,
    AuthnEventEmitter,
    AuthnEventKind,
    AuthnEventSink,
    AuthnIdentity,
    RefreshTokenCredentials,
    login_digest,
)
from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze_identity.authn.adapters.password_lifecycle import PasswordLifecycleAdapter
from forze_identity.authn.adapters.password_reset import PasswordResetAdapter
from forze_identity.authn.adapters.principal_deactivation import (
    PrincipalDeactivationAdapter,
)
from forze_identity.authn.adapters.token_lifecycle import TokenLifecycleAdapter
from forze_identity.authn.domain.models.account import ReadPasswordAccount
from forze_identity.authn.domain.models.reset import ReadPasswordReset
from forze_identity.authn.domain.models.session import ReadSession
from forze_identity.authn.services import (
    RefreshTokenConfig,
    RefreshTokenService,
    ResetTokenConfig,
    ResetTokenService,
)

pytestmark = pytest.mark.unit

# ----------------------- #


class _RecordingSink(AuthnEventSink):
    def __init__(self) -> None:
        self.events: list[AuthnEvent] = []

    async def record(self, event: AuthnEvent) -> None:
        self.events.append(event)


def _emitter() -> tuple[AuthnEventEmitter, _RecordingSink]:
    sink = _RecordingSink()
    return AuthnEventEmitter(sink=sink, route="main"), sink


def _qry(read_model: type) -> MagicMock:
    qry = MagicMock()
    qry.spec = DocumentSpec(name="spec", read=read_model)
    qry.find = AsyncMock(return_value=None)
    qry.find_many = AsyncMock(return_value=CountlessPage(hits=[], page=1, size=0))
    return qry


def _cmd(read_model: type) -> MagicMock:
    cmd = MagicMock()
    cmd.spec = DocumentSpec(name="spec", read=read_model)
    cmd.create = AsyncMock()
    cmd.update = AsyncMock()
    cmd.update_many = AsyncMock()
    return cmd


def _eligibility() -> MagicMock:
    eligibility = MagicMock()
    eligibility.require_authentication_allowed = AsyncMock()
    return eligibility


def _read_session(
    *,
    principal_id: UUID,
    tenant_id: UUID | None = None,
    rotated_at: datetime | None = None,
) -> ReadSession:
    now = datetime.now(tz=UTC)
    return ReadSession(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=principal_id,
        tenant_id=tenant_id,
        refresh_digest="digest",
        expires_at=now + timedelta(days=7),
        rotated_at=rotated_at,
    )


def _read_account(*, principal_id: UUID) -> ReadPasswordAccount:
    now = datetime.now(tz=UTC)
    return ReadPasswordAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=principal_id,
        username="alice",
        password_hash="old-hash",
        is_active=True,
    )


# ----------------------- #


class TestTokenLifecycleEmission:
    REFRESH_SVC = RefreshTokenService(pepper=b"p" * 32, config=RefreshTokenConfig())

    def _adapter(self, **overrides: Any) -> TokenLifecycleAdapter:
        access_svc = MagicMock()
        access_svc.config.expires_in = timedelta(minutes=15)
        access_svc.issue_token = MagicMock(return_value="jwt")

        kwargs: dict[str, Any] = {
            "access_svc": access_svc,
            "refresh_svc": self.REFRESH_SVC,
            "session_qry": _qry(ReadSession),
            "session_cmd": _cmd(ReadSession),
            "eligibility": _eligibility(),
        }
        kwargs.update(overrides)
        return TokenLifecycleAdapter(**kwargs)

    async def test_refresh_emits_token_refreshed(self) -> None:
        emitter, sink = _emitter()
        principal_id = uuid4()
        tenant_id = uuid4()
        old = _read_session(principal_id=principal_id, tenant_id=tenant_id)

        session_qry = _qry(ReadSession)
        session_qry.find = AsyncMock(return_value=old)
        session_cmd = _cmd(ReadSession)
        session_cmd.create = AsyncMock(return_value=MagicMock(id=uuid4()))

        adapter = self._adapter(
            session_qry=session_qry,
            session_cmd=session_cmd,
            events=emitter,
        )

        await adapter.refresh_tokens(
            RefreshTokenCredentials(token=self.REFRESH_SVC.generate_token()),
        )

        (event,) = sink.events
        assert event.kind is AuthnEventKind.TOKEN_REFRESHED
        assert event.principal_id == principal_id
        assert event.tenant_id == tenant_id
        assert event.route == "main"

    async def test_reuse_emits_refresh_reuse_detected_and_uniform_error(self) -> None:
        emitter, sink = _emitter()
        principal_id = uuid4()
        rotated = _read_session(
            principal_id=principal_id,
            rotated_at=datetime.now(tz=UTC),
        )

        session_qry = _qry(ReadSession)
        session_qry.find = AsyncMock(return_value=rotated)

        adapter = self._adapter(session_qry=session_qry, events=emitter)

        with pytest.raises(CoreException, match="Invalid refresh token"):
            await adapter.refresh_tokens(
                RefreshTokenCredentials(token=self.REFRESH_SVC.generate_token()),
            )

        (event,) = sink.events
        assert event.kind is AuthnEventKind.REFRESH_REUSE_DETECTED
        assert event.principal_id == principal_id

    async def test_revoke_emits_logout(self) -> None:
        emitter, sink = _emitter()
        principal_id = uuid4()
        adapter = self._adapter(events=emitter)

        await adapter.revoke_tokens(AuthnIdentity(principal_id=principal_id))

        (event,) = sink.events
        assert event.kind is AuthnEventKind.LOGOUT
        assert event.principal_id == principal_id

    async def test_no_emitter_keeps_flows_silent(self) -> None:
        adapter = self._adapter()

        await adapter.revoke_tokens(AuthnIdentity(principal_id=uuid4()))


class TestPasswordLifecycleEmission:
    async def test_change_password_emits_password_changed(self) -> None:
        emitter, sink = _emitter()
        principal_id = uuid4()
        account = _read_account(principal_id=principal_id)

        password_svc = MagicMock()
        password_svc.verify_password = MagicMock(return_value=True)
        password_svc.hash_password = MagicMock(return_value="new-hash")

        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find = AsyncMock(return_value=account)

        adapter = PasswordLifecycleAdapter(
            password_svc=password_svc,
            pa_qry=pa_qry,
            pa_cmd=_cmd(ReadPasswordAccount),
            eligibility=_eligibility(),
            session_qry=_qry(ReadSession),
            session_cmd=_cmd(ReadSession),
            events=emitter,
        )

        await adapter.change_password(
            AuthnIdentity(principal_id=principal_id),
            "current",
            "next",
        )

        (event,) = sink.events
        assert event.kind is AuthnEventKind.PASSWORD_CHANGED
        assert event.principal_id == principal_id

    async def test_failed_change_emits_nothing(self) -> None:
        emitter, sink = _emitter()
        principal_id = uuid4()
        account = _read_account(principal_id=principal_id)

        password_svc = MagicMock()
        password_svc.verify_password = MagicMock(return_value=False)

        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find = AsyncMock(return_value=account)

        adapter = PasswordLifecycleAdapter(
            password_svc=password_svc,
            pa_qry=pa_qry,
            pa_cmd=_cmd(ReadPasswordAccount),
            eligibility=_eligibility(),
            session_qry=_qry(ReadSession),
            session_cmd=_cmd(ReadSession),
            events=emitter,
        )

        with pytest.raises(CoreException, match="incorrect"):
            await adapter.change_password(
                AuthnIdentity(principal_id=principal_id),
                "wrong",
                "next",
            )

        assert sink.events == []


class TestPasswordResetEmission:
    def _adapter(self, **overrides: Any) -> PasswordResetAdapter:
        password_svc = MagicMock()
        password_svc.hash_password = MagicMock(return_value="argon2-hash")

        kwargs: dict[str, Any] = {
            "password_svc": password_svc,
            "reset_svc": ResetTokenService(pepper=b"p" * 32, config=ResetTokenConfig()),
            "pa_qry": _qry(ReadPasswordAccount),
            "pa_cmd": _cmd(ReadPasswordAccount),
            "reset_qry": _qry(ReadPasswordReset),
            "reset_cmd": _cmd(ReadPasswordReset),
            "eligibility": _eligibility(),
            "session_qry": _qry(ReadSession),
            "session_cmd": _cmd(ReadSession),
        }
        kwargs.update(overrides)
        return PasswordResetAdapter(**kwargs)

    async def test_request_emits_only_on_actual_issuance(self) -> None:
        emitter, sink = _emitter()
        principal_id = uuid4()
        account = _read_account(principal_id=principal_id)

        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find_many = AsyncMock(
            return_value=CountlessPage(hits=[account], page=1, size=1),
        )

        adapter = self._adapter(pa_qry=pa_qry, events=emitter)

        issued = await adapter.request_reset("alice")

        assert issued is not None
        (event,) = sink.events
        assert event.kind is AuthnEventKind.PASSWORD_RESET_REQUESTED
        assert event.principal_id == principal_id
        assert event.login_digest == login_digest("alice")
        assert event.login_digest != "alice"

    async def test_unknown_login_emits_nothing(self) -> None:
        emitter, sink = _emitter()
        adapter = self._adapter(events=emitter)

        assert await adapter.request_reset("nobody") is None
        assert sink.events == []

    async def test_reset_completion_emits_password_reset_completed(self) -> None:
        emitter, sink = _emitter()
        principal_id = uuid4()
        account = _read_account(principal_id=principal_id)
        reset_svc = ResetTokenService(pepper=b"p" * 32, config=ResetTokenConfig())
        token = reset_svc.generate_token()
        digest = reset_svc.calculate_token_digest(token)

        now = datetime.now(tz=UTC)
        reset_row = ReadPasswordReset(
            id=uuid4(),
            rev=1,
            created_at=now,
            last_update_at=now,
            principal_id=principal_id,
            token_digest=digest,
            expires_at=now + timedelta(hours=1),
            used_at=None,
        )

        reset_qry = _qry(ReadPasswordReset)
        reset_qry.find = AsyncMock(return_value=reset_row)
        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find = AsyncMock(return_value=account)

        adapter = self._adapter(
            reset_svc=reset_svc,
            reset_qry=reset_qry,
            pa_qry=pa_qry,
            events=emitter,
        )

        await adapter.reset_password(token, "new-password")

        (event,) = sink.events
        assert event.kind is AuthnEventKind.PASSWORD_RESET_COMPLETED
        assert event.principal_id == principal_id

    async def test_failed_reset_emits_nothing(self) -> None:
        emitter, sink = _emitter()
        adapter = self._adapter(events=emitter)

        with pytest.raises(CoreException, match="Invalid or expired reset token"):
            await adapter.reset_password("garbage", "new-password")

        assert sink.events == []


class TestPrincipalDeactivationEmission:
    async def test_deactivate_emits_principal_deactivated(self) -> None:
        emitter, sink = _emitter()
        principal_id = uuid4()

        registry = MagicMock()
        registry.deactivate_principal = AsyncMock()
        token_lifecycle = MagicMock()
        token_lifecycle.revoke_tokens = AsyncMock()
        credentials = MagicMock()
        credentials.deactivate_all = AsyncMock()

        adapter = PrincipalDeactivationAdapter(
            principal_registry=registry,
            token_lifecycle=token_lifecycle,
            credentials=credentials,
            events=emitter,
        )

        await adapter.deactivate(principal_id)

        (event,) = sink.events
        assert event.kind is AuthnEventKind.PRINCIPAL_DEACTIVATED
        assert event.principal_id == principal_id
        registry.deactivate_principal.assert_awaited_once_with(principal_id)
        token_lifecycle.revoke_tokens.assert_awaited_once()
        credentials.deactivate_all.assert_awaited_once_with(principal_id)

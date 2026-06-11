"""Unit tests for :class:`~forze_identity.authn.adapters.password_reset.PasswordResetAdapter`."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze_identity.authn.adapters.password_reset import (
    INVALID_RESET_TOKEN_MSG,
    PasswordResetAdapter,
)
from forze_identity.authn.domain.models.account import ReadPasswordAccount
from forze_identity.authn.domain.models.reset import ReadPasswordReset
from forze_identity.authn.domain.models.session import ReadSession
from forze_identity.authn.services import ResetTokenConfig, ResetTokenService

# ----------------------- #


def get_test_password() -> str:
    """Return a generated throwaway password for credential fixtures."""

    return secrets.token_urlsafe(16)


def _svc() -> ResetTokenService:
    return ResetTokenService(pepper=b"p" * 32, config=ResetTokenConfig())


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


def _adapter(**overrides: object) -> PasswordResetAdapter:
    eligibility = MagicMock()
    eligibility.require_authentication_allowed = AsyncMock()

    password_svc = MagicMock()
    password_svc.hash_password = MagicMock(return_value="argon2-hash")

    kwargs: dict[str, Any] = {
        "password_svc": password_svc,
        "reset_svc": _svc(),
        "pa_qry": _qry(ReadPasswordAccount),
        "pa_cmd": _cmd(ReadPasswordAccount),
        "reset_qry": _qry(ReadPasswordReset),
        "reset_cmd": _cmd(ReadPasswordReset),
        "eligibility": eligibility,
        "session_qry": _qry(ReadSession),
        "session_cmd": _cmd(ReadSession),
    }
    kwargs.update(overrides)
    return PasswordResetAdapter(**kwargs)  # type: ignore[arg-type]


def _read_account(
    *,
    principal_id: UUID | None = None,
    is_active: bool = True,
) -> ReadPasswordAccount:
    now = datetime.now(tz=UTC)
    return ReadPasswordAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=principal_id or uuid4(),
        username="alice",
        password_hash="old-hash",
        is_active=is_active,
    )


def _read_reset(
    *,
    principal_id: UUID,
    token_digest: str,
    expires_at: datetime | None = None,
    used_at: datetime | None = None,
) -> ReadPasswordReset:
    now = datetime.now(tz=UTC)
    return ReadPasswordReset(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=principal_id,
        token_digest=token_digest,
        expires_at=expires_at or (now + timedelta(hours=1)),
        used_at=used_at,
    )


def _account_page(account: ReadPasswordAccount) -> CountlessPage:
    return CountlessPage(hits=[account], page=1, size=1)


# ----------------------- #


class TestPasswordResetInit:
    def test_rejects_cached_reset_spec(self) -> None:
        qry = MagicMock()
        qry.spec = DocumentSpec(
            name="resets",
            read=ReadPasswordReset,
            cache=MagicMock(),
        )

        with pytest.raises(CoreException, match="Password reset caching"):
            _adapter(reset_qry=qry)

    def test_revoke_sessions_requires_session_ports(self) -> None:
        with pytest.raises(CoreException, match="revoke_sessions_on_reset"):
            _adapter(session_qry=None, session_cmd=None)

    def test_opting_out_of_session_revocation_drops_the_requirement(self) -> None:
        adapter = _adapter(
            session_qry=None,
            session_cmd=None,
            revoke_sessions_on_reset=False,
        )

        assert adapter.revoke_sessions_on_reset is False


# ----------------------- #


class TestRequestReset:
    @pytest.mark.asyncio
    async def test_unknown_login_returns_none_and_writes_nothing(self) -> None:
        adapter = _adapter()

        issued = await adapter.request_reset("nobody")

        assert issued is None
        adapter.reset_cmd.create.assert_not_called()
        adapter.reset_cmd.update_many.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_login_returns_none(self) -> None:
        adapter = _adapter()

        assert await adapter.request_reset("") is None

    @pytest.mark.asyncio
    async def test_inactive_account_returns_none(self) -> None:
        account = _read_account(is_active=False)
        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find_many = AsyncMock(return_value=_account_page(account))

        adapter = _adapter(pa_qry=pa_qry)

        assert await adapter.request_reset("alice") is None
        adapter.reset_cmd.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_ineligible_principal_returns_none(self) -> None:
        account = _read_account()
        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find_many = AsyncMock(return_value=_account_page(account))

        eligibility = MagicMock()
        eligibility.require_authentication_allowed = AsyncMock(
            side_effect=exc.authentication("Principal not found"),
        )

        adapter = _adapter(pa_qry=pa_qry, eligibility=eligibility)

        assert await adapter.request_reset("alice") is None
        adapter.reset_cmd.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_issue_persists_digest_only_never_the_raw_token(self) -> None:
        account = _read_account()
        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find_many = AsyncMock(return_value=_account_page(account))

        adapter = _adapter(pa_qry=pa_qry)

        issued = await adapter.request_reset("alice")

        assert issued is not None
        assert issued.token
        assert issued.principal_id == account.principal_id
        assert issued.login == "alice"

        adapter.reset_cmd.create.assert_awaited_once()
        create_cmd = adapter.reset_cmd.create.await_args.args[0]

        # Only the HMAC digest is persisted; the raw token appears nowhere in
        # the persisted command.
        assert create_cmd.token_digest == adapter.reset_svc.calculate_token_digest(
            issued.token
        )
        assert issued.token not in str(create_cmd.model_dump())
        assert create_cmd.principal_id == account.principal_id
        assert create_cmd.expires_at == issued.expires_at

    @pytest.mark.asyncio
    async def test_token_is_redacted_from_vo_repr(self) -> None:
        account = _read_account()
        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find_many = AsyncMock(return_value=_account_page(account))

        adapter = _adapter(pa_qry=pa_qry)

        issued = await adapter.request_reset("alice")

        assert issued is not None
        assert issued.token not in repr(issued)

    @pytest.mark.asyncio
    async def test_issuing_supersedes_previous_outstanding_resets(self) -> None:
        account = _read_account()
        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find_many = AsyncMock(return_value=_account_page(account))

        previous = _read_reset(
            principal_id=account.principal_id,
            token_digest="old-digest",
        )
        reset_qry = _qry(ReadPasswordReset)
        reset_qry.find_many = AsyncMock(
            return_value=CountlessPage(hits=[previous], page=1, size=1),
        )

        adapter = _adapter(pa_qry=pa_qry, reset_qry=reset_qry)

        issued = await adapter.request_reset("alice")

        assert issued is not None
        adapter.reset_cmd.update_many.assert_awaited_once()
        upds = adapter.reset_cmd.update_many.await_args.args[0]
        assert [(pk, rev) for pk, rev, _cmd in upds] == [(previous.id, previous.rev)]
        assert all(cmd.used_at is not None for _pk, _rev, cmd in upds)

    @pytest.mark.asyncio
    async def test_no_outstanding_resets_skips_supersession_write(self) -> None:
        account = _read_account()
        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find_many = AsyncMock(return_value=_account_page(account))

        adapter = _adapter(pa_qry=pa_qry)

        assert await adapter.request_reset("alice") is not None
        adapter.reset_cmd.update_many.assert_not_called()

    @pytest.mark.asyncio
    async def test_expiry_follows_config_ttl(self) -> None:
        instant = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        account = _read_account()
        pa_qry = _qry(ReadPasswordAccount)
        pa_qry.find_many = AsyncMock(return_value=_account_page(account))

        adapter = _adapter(
            pa_qry=pa_qry,
            reset_svc=ResetTokenService(
                pepper=b"p" * 32,
                config=ResetTokenConfig(expires_in=timedelta(minutes=30)),
            ),
        )

        with bind_time_source(FrozenTimeSource(instant)):
            issued = await adapter.request_reset("alice")

        assert issued is not None
        assert issued.expires_at == instant + timedelta(minutes=30)


# ----------------------- #


def _confirm_env(
    *,
    account: ReadPasswordAccount | None = None,
    reset: ReadPasswordReset | None = None,
    **overrides: object,
) -> PasswordResetAdapter:
    """Adapter whose query ports resolve the given account + reset."""

    pa_qry = _qry(ReadPasswordAccount)
    if account is not None:
        pa_qry.find = AsyncMock(return_value=account)

    reset_qry = _qry(ReadPasswordReset)
    if reset is not None:
        reset_qry.find = AsyncMock(return_value=reset)

    return _adapter(pa_qry=pa_qry, reset_qry=reset_qry, **overrides)


class TestResetPassword:
    @pytest.mark.asyncio
    async def test_success_sets_hash_consumes_reset_and_revokes_sessions(self) -> None:
        svc = _svc()
        token = svc.generate_token()
        account = _read_account()
        reset = _read_reset(
            principal_id=account.principal_id,
            token_digest=svc.calculate_token_digest(token),
        )

        session = MagicMock()
        session.id = uuid4()
        session.rev = 1
        session_qry = _qry(ReadSession)
        session_qry.find_many = AsyncMock(
            return_value=CountlessPage(hits=[session], page=1, size=1),
        )

        adapter = _confirm_env(
            account=account,
            reset=reset,
            reset_svc=svc,
            session_qry=session_qry,
        )

        await adapter.reset_password(token, get_test_password())

        # New hash persisted on the account row.
        adapter.pa_cmd.update.assert_awaited_once()
        pa_args = adapter.pa_cmd.update.await_args.args
        assert pa_args[0] == account.id
        assert pa_args[2].password_hash == "argon2-hash"

        # Reset consumed (single-use).
        adapter.reset_cmd.update.assert_awaited_once()
        reset_args = adapter.reset_cmd.update.await_args.args
        assert reset_args[0] == reset.id
        assert reset_args[2].used_at is not None

        # ALL sessions of the principal revoked ("log out everywhere").
        session_qry.find_many.assert_awaited_once()
        filters = session_qry.find_many.await_args.kwargs["filters"]
        assert filters == {"$values": {"principal_id": account.principal_id}}
        adapter.session_cmd.update_many.assert_awaited_once()
        upds = adapter.session_cmd.update_many.await_args.args[0]
        assert [(pk, rev) for pk, rev, _cmd in upds] == [(session.id, session.rev)]

    @pytest.mark.asyncio
    async def test_opted_out_session_revocation_skips_session_writes(self) -> None:
        svc = _svc()
        token = svc.generate_token()
        account = _read_account()
        reset = _read_reset(
            principal_id=account.principal_id,
            token_digest=svc.calculate_token_digest(token),
        )

        adapter = _confirm_env(
            account=account,
            reset=reset,
            reset_svc=svc,
            session_qry=None,
            session_cmd=None,
            revoke_sessions_on_reset=False,
        )

        await adapter.reset_password(token, get_test_password())

        adapter.pa_cmd.update.assert_awaited_once()

    # ....................... #
    # Uniform failure modes

    async def _assert_uniform_failure(
        self,
        adapter: PasswordResetAdapter,
        token: str,
    ) -> None:
        with pytest.raises(CoreException, match=INVALID_RESET_TOKEN_MSG) as ei:
            await adapter.reset_password(token, get_test_password())

        assert ei.value.kind is ExceptionKind.AUTHENTICATION
        adapter.pa_cmd.update.assert_not_called()
        adapter.reset_cmd.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_token_uniform_error(self) -> None:
        await self._assert_uniform_failure(_adapter(), "")

    @pytest.mark.asyncio
    async def test_garbage_token_uniform_error(self) -> None:
        # Non-base64 material fails digest computation — same uniform error.
        await self._assert_uniform_failure(_adapter(), "%%% not base64 %%%")

    @pytest.mark.asyncio
    async def test_unknown_token_uniform_error(self) -> None:
        await self._assert_uniform_failure(_adapter(), _svc().generate_token())

    @pytest.mark.asyncio
    async def test_used_token_uniform_error(self) -> None:
        svc = _svc()
        token = svc.generate_token()
        reset = _read_reset(
            principal_id=uuid4(),
            token_digest=svc.calculate_token_digest(token),
            used_at=datetime.now(tz=UTC),
        )

        await self._assert_uniform_failure(
            _confirm_env(reset=reset, reset_svc=svc),
            token,
        )

    @pytest.mark.asyncio
    async def test_expired_token_uniform_error_via_frozen_time(self) -> None:
        svc = _svc()
        token = svc.generate_token()
        issued_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        reset = _read_reset(
            principal_id=uuid4(),
            token_digest=svc.calculate_token_digest(token),
            expires_at=issued_at + timedelta(hours=1),
        )
        adapter = _confirm_env(reset=reset, reset_svc=svc)

        # One second past expiry: uniformly rejected.
        with bind_time_source(FrozenTimeSource(issued_at + timedelta(hours=1, seconds=1))):
            await self._assert_uniform_failure(adapter, token)

    @pytest.mark.asyncio
    async def test_not_yet_expired_token_passes_the_ttl_gate(self) -> None:
        svc = _svc()
        token = svc.generate_token()
        issued_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
        account = _read_account()
        reset = _read_reset(
            principal_id=account.principal_id,
            token_digest=svc.calculate_token_digest(token),
            expires_at=issued_at + timedelta(hours=1),
        )
        adapter = _confirm_env(account=account, reset=reset, reset_svc=svc)

        with bind_time_source(FrozenTimeSource(issued_at + timedelta(minutes=59))):
            await adapter.reset_password(token, get_test_password())

        adapter.pa_cmd.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_wrong_pepper_token_uniform_error(self) -> None:
        # A token minted under a different pepper maps to a different digest —
        # indistinguishable from an unknown token.
        other_svc = ResetTokenService(pepper=b"q" * 32, config=ResetTokenConfig())

        await self._assert_uniform_failure(_adapter(), other_svc.generate_token())

    @pytest.mark.asyncio
    async def test_ineligible_principal_uniform_error(self) -> None:
        svc = _svc()
        token = svc.generate_token()
        account = _read_account()
        reset = _read_reset(
            principal_id=account.principal_id,
            token_digest=svc.calculate_token_digest(token),
        )

        eligibility = MagicMock()
        eligibility.require_authentication_allowed = AsyncMock(
            side_effect=exc.authentication("Principal not found"),
        )

        await self._assert_uniform_failure(
            _confirm_env(
                account=account,
                reset=reset,
                reset_svc=svc,
                eligibility=eligibility,
            ),
            token,
        )

    @pytest.mark.asyncio
    async def test_missing_account_uniform_error(self) -> None:
        svc = _svc()
        token = svc.generate_token()
        reset = _read_reset(
            principal_id=uuid4(),
            token_digest=svc.calculate_token_digest(token),
        )

        # No account resolves for the principal — same uniform error.
        await self._assert_uniform_failure(
            _confirm_env(reset=reset, reset_svc=svc),
            token,
        )

    @pytest.mark.asyncio
    async def test_inactive_account_uniform_error(self) -> None:
        svc = _svc()
        token = svc.generate_token()
        account = _read_account(is_active=False)
        reset = _read_reset(
            principal_id=account.principal_id,
            token_digest=svc.calculate_token_digest(token),
        )

        await self._assert_uniform_failure(
            _confirm_env(account=account, reset=reset, reset_svc=svc),
            token,
        )

"""Unit tests for :class:`Argon2PasswordVerifier`."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from forze.application.contracts.authn import PasswordCredentials
from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn.domain.constants import ISSUER_FORZE_PASSWORD
from forze_identity.authn.domain.models.account import ReadPasswordAccount
from forze_identity.authn.services import PasswordConfig, PasswordService
from forze_identity.authn.verifiers import Argon2PasswordVerifier

pytestmark = pytest.mark.unit

_INVALID_LOGIN_MSG = "Invalid login or password"
_INVALID_LOGIN_CODE = "invalid_credentials"


def _slow_password_config() -> PasswordConfig:
    return PasswordConfig(time_cost=1, memory_cost=8192, parallelism=1)


def _password_spec() -> DocumentSpec:
    return DocumentSpec(name="pwd", read=ReadPasswordAccount)


def _account(
    *,
    username: str = "alice",
    password_hash: str,
    is_active: bool = True,
) -> ReadPasswordAccount:
    now = datetime.now(UTC)
    pid = uuid4()
    return ReadPasswordAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=pid,
        username=username,
        password_hash=password_hash,
        is_active=is_active,
    )


def _verifier(
    *,
    account: ReadPasswordAccount | None,
    password_svc: PasswordService | None = None,
    pa_cmd: MagicMock | None = None,
) -> Argon2PasswordVerifier:
    pa_qry = MagicMock()
    pa_qry.spec = _password_spec()
    hits = [] if account is None else [account]
    pa_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=hits, page=1, size=len(hits)),
    )
    svc = password_svc or PasswordService(config=_slow_password_config())
    return Argon2PasswordVerifier(password_svc=svc, pa_qry=pa_qry, pa_cmd=pa_cmd)


def _pa_cmd() -> MagicMock:
    pa_cmd = MagicMock()
    pa_cmd.spec = _password_spec()
    pa_cmd.update = AsyncMock(return_value=None)
    return pa_cmd


def _old_params_hash(password: str) -> str:
    """Hash produced under parameters that differ from ``_slow_password_config``."""

    old_svc = PasswordService(
        config=PasswordConfig(time_cost=2, memory_cost=16384, parallelism=1),
    )
    return old_svc.hash_password(password)


def _assert_invalid_login(exc: BaseException) -> None:
    assert isinstance(exc, CoreException)
    assert exc.kind == ExceptionKind.AUTHENTICATION
    assert exc.summary == _INVALID_LOGIN_MSG
    assert exc.code == _INVALID_LOGIN_CODE


@pytest.mark.asyncio
async def test_unknown_login_raises_generic_invalid_credentials() -> None:
    verifier = _verifier(account=None)

    with pytest.raises(CoreException) as raised:
        await verifier.verify_password(
            PasswordCredentials(login="nobody", password="any"),
        )

    _assert_invalid_login(raised.value)


@pytest.mark.asyncio
async def test_inactive_account_raises_generic_invalid_credentials() -> None:
    pwd = PasswordService(config=_slow_password_config())
    account = _account(
        password_hash=pwd.hash_password("secret"),
        is_active=False,
    )
    verifier = _verifier(account=account, password_svc=pwd)

    with pytest.raises(CoreException) as raised:
        await verifier.verify_password(
            PasswordCredentials(login="alice", password="secret"),
        )

    _assert_invalid_login(raised.value)


@pytest.mark.asyncio
async def test_wrong_password_raises_generic_invalid_credentials() -> None:
    pwd = PasswordService(config=_slow_password_config())
    account = _account(password_hash=pwd.hash_password("correct"))
    verifier = _verifier(account=account, password_svc=pwd)

    with pytest.raises(CoreException) as raised:
        await verifier.verify_password(
            PasswordCredentials(login="alice", password="wrong"),
        )

    _assert_invalid_login(raised.value)


@pytest.mark.asyncio
async def test_valid_password_returns_assertion() -> None:
    pwd = PasswordService(config=_slow_password_config())
    account = _account(password_hash=pwd.hash_password("correct"))
    verifier = _verifier(account=account, password_svc=pwd)

    assertion = await verifier.verify_password(
        PasswordCredentials(login="alice", password="correct"),
    )

    assert assertion.issuer == ISSUER_FORZE_PASSWORD
    assert assertion.subject == str(account.principal_id)


# ....................... #
# Rehash-on-login (Argon2 parameter upgrades)


@pytest.mark.asyncio
async def test_outdated_hash_is_rehashed_when_command_port_wired() -> None:
    pwd = PasswordService(config=_slow_password_config())
    old_hash = _old_params_hash("correct")
    account = _account(password_hash=old_hash)
    pa_cmd = _pa_cmd()
    verifier = _verifier(account=account, password_svc=pwd, pa_cmd=pa_cmd)

    assertion = await verifier.verify_password(
        PasswordCredentials(login="alice", password="correct"),
    )

    assert assertion.subject == str(account.principal_id)

    pa_cmd.update.assert_awaited_once()
    doc_id, rev, upd_cmd = pa_cmd.update.await_args.args
    assert doc_id == account.id
    assert rev == account.rev

    new_hash = upd_cmd.password_hash
    assert new_hash is not None
    assert new_hash != old_hash
    assert pwd.verify_password(password_hash=new_hash, password="correct")
    assert not pwd.password_needs_rehash(new_hash)


@pytest.mark.asyncio
async def test_rehash_persistence_failure_never_fails_login() -> None:
    pwd = PasswordService(config=_slow_password_config())
    account = _account(password_hash=_old_params_hash("correct"))
    pa_cmd = _pa_cmd()
    pa_cmd.update = AsyncMock(side_effect=RuntimeError("rev conflict"))
    verifier = _verifier(account=account, password_svc=pwd, pa_cmd=pa_cmd)

    with patch(
        "forze_identity.authn.verifiers.argon2_password.logger"
    ) as mock_logger:
        assertion = await verifier.verify_password(
            PasswordCredentials(login="alice", password="correct"),
        )

    assert assertion.subject == str(account.principal_id)
    pa_cmd.update.assert_awaited_once()
    mock_logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_no_command_port_skips_rehash_write() -> None:
    pwd = PasswordService(config=_slow_password_config())
    account = _account(password_hash=_old_params_hash("correct"))
    verifier = _verifier(account=account, password_svc=pwd, pa_cmd=None)

    with patch.object(
        PasswordService,
        "password_needs_rehash",
        autospec=True,
    ) as needs_rehash:
        assertion = await verifier.verify_password(
            PasswordCredentials(login="alice", password="correct"),
        )

    assert assertion.subject == str(account.principal_id)
    needs_rehash.assert_not_called()


@pytest.mark.asyncio
async def test_up_to_date_hash_is_not_rewritten() -> None:
    pwd = PasswordService(config=_slow_password_config())
    account = _account(password_hash=pwd.hash_password("correct"))
    pa_cmd = _pa_cmd()
    verifier = _verifier(account=account, password_svc=pwd, pa_cmd=pa_cmd)

    assertion = await verifier.verify_password(
        PasswordCredentials(login="alice", password="correct"),
    )

    assert assertion.subject == str(account.principal_id)
    pa_cmd.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_wrong_password_never_triggers_rehash() -> None:
    pwd = PasswordService(config=_slow_password_config())
    account = _account(password_hash=_old_params_hash("correct"))
    pa_cmd = _pa_cmd()
    verifier = _verifier(account=account, password_svc=pwd, pa_cmd=pa_cmd)

    with pytest.raises(CoreException):
        await verifier.verify_password(
            PasswordCredentials(login="alice", password="wrong"),
        )

    pa_cmd.update.assert_not_awaited()


# ....................... #


@pytest.mark.asyncio
async def test_missing_account_invokes_verify_once() -> None:
    pwd = PasswordService(config=_slow_password_config())
    verifier = _verifier(account=None, password_svc=pwd)
    calls = 0
    original = PasswordService.verify_password

    def counting_verify(
        self: PasswordService,
        password_hash: str,
        password: str,
    ) -> bool:
        nonlocal calls
        calls += 1
        return original(self, password_hash, password)

    with (
        patch.object(PasswordService, "verify_password", counting_verify),
        pytest.raises(CoreException),
    ):
        await verifier.verify_password(
            PasswordCredentials(login="nobody", password="x"),
        )

    assert calls == 1

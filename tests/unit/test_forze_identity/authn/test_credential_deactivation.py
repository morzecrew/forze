"""Tests for :mod:`forze_identity.authn.adapters.credential_deactivation`."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import KeyedUpdate
from forze.base.exceptions import CoreException, exc
from forze_identity.authn.adapters._utils import STALE_RETRIES
from forze_identity.authn.adapters.credential_deactivation import (
    AuthnCredentialDeactivationHelper,
)
from forze_identity.authn.domain.models.account import (
    ReadApiKeyAccount,
    ReadPasswordAccount,
    UpdateApiKeyAccountCmd,
    UpdatePasswordAccountCmd,
)

pytestmark = pytest.mark.unit


def _password_row(*, principal_id, active: bool = True) -> ReadPasswordAccount:
    now = datetime.now(tz=UTC)
    return ReadPasswordAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=principal_id,
        username="alice",
        password_hash="hash",
        is_active=active,
    )


def _api_key_row(*, principal_id, active: bool = True) -> ReadApiKeyAccount:
    now = datetime.now(tz=UTC)
    return ReadApiKeyAccount(
        id=uuid4(),
        rev=2,
        created_at=now,
        last_update_at=now,
        principal_id=principal_id,
        key_hash="digest",
        is_active=active,
    )


@pytest.mark.asyncio
async def test_deactivate_all_updates_password_and_active_api_keys() -> None:
    pid = uuid4()
    password = _password_row(principal_id=pid)
    active_key = _api_key_row(principal_id=pid, active=True)
    inactive_key = _api_key_row(principal_id=pid, active=False)

    pa_qry = MagicMock()
    pa_qry.find = AsyncMock(return_value=password)
    pa_cmd = MagicMock()
    pa_cmd.update = AsyncMock()

    ak_qry = MagicMock()
    ak_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[active_key, inactive_key], page=1, size=10),
    )
    ak_cmd = MagicMock()
    ak_cmd.update_many = AsyncMock()

    helper = AuthnCredentialDeactivationHelper(
        pa_qry=pa_qry,
        pa_cmd=pa_cmd,
        ak_qry=ak_qry,
        ak_cmd=ak_cmd,
    )

    await helper.deactivate_all(pid)

    pa_cmd.update.assert_awaited_once_with(
        password.id,
        password.rev,
        UpdatePasswordAccountCmd(is_active=False),
        return_new=False,
    )
    ak_cmd.update_many.assert_awaited_once_with(
        [
            KeyedUpdate(
                id=active_key.id,
                rev=active_key.rev,
                dto=UpdateApiKeyAccountCmd(is_active=False),
            )
        ],
        return_new=False,
    )


@pytest.mark.asyncio
async def test_deactivate_all_skips_missing_password() -> None:
    pid = uuid4()
    pa_qry = MagicMock()
    pa_qry.find = AsyncMock(return_value=None)
    pa_cmd = MagicMock()
    pa_cmd.update = AsyncMock()

    ak_qry = MagicMock()
    ak_qry.find_many = AsyncMock(return_value=CountlessPage(hits=[], page=1, size=10))
    ak_cmd = MagicMock()
    ak_cmd.update_many = AsyncMock()

    helper = AuthnCredentialDeactivationHelper(
        pa_qry=pa_qry,
        pa_cmd=pa_cmd,
        ak_qry=ak_qry,
        ak_cmd=ak_cmd,
    )

    await helper.deactivate_all(pid)

    pa_cmd.update.assert_not_called()
    ak_cmd.update_many.assert_not_called()


def _stale() -> CoreException:
    return exc.precondition("Revision mismatch", code="revision_mismatch")


@pytest.mark.asyncio
async def test_a_password_account_written_under_the_deactivation_is_read_again_and_closed() -> None:
    # A password change wrote the account between the read and the update: the update is
    # stale, so the account is read again and closed at the revision the change left.
    pid = uuid4()
    first = _password_row(principal_id=pid)
    changed = first.model_copy(update={"rev": 2, "password_hash": "new"})
    pa_qry = MagicMock()
    pa_qry.find = AsyncMock(side_effect=[first, changed])
    pa_cmd = MagicMock()
    pa_cmd.update = AsyncMock(side_effect=[_stale(), None])

    await AuthnCredentialDeactivationHelper(pa_qry=pa_qry, pa_cmd=pa_cmd).deactivate_all(pid)

    assert pa_cmd.update.await_count == 2
    assert pa_cmd.update.await_args.args[:2] == (changed.id, 2)


@pytest.mark.asyncio
async def test_a_password_account_stale_on_every_attempt_fails_the_deactivation() -> None:
    pid = uuid4()
    pa_qry = MagicMock()
    pa_qry.find = AsyncMock(return_value=_password_row(principal_id=pid))
    pa_cmd = MagicMock()
    pa_cmd.update = AsyncMock(side_effect=_stale())
    helper = AuthnCredentialDeactivationHelper(pa_qry=pa_qry, pa_cmd=pa_cmd)

    with pytest.raises(CoreException) as caught:
        await helper.deactivate_all(pid)

    assert caught.value.code == "revision_mismatch"
    assert pa_cmd.update.await_count == STALE_RETRIES + 1


@pytest.mark.asyncio
async def test_a_conflict_the_backend_reports_is_not_retried() -> None:
    # The backend aborted the transaction with it, so a retry could only fail again.
    pid = uuid4()
    conflict = exc.concurrency("Deadlock detected")
    conflict.__cause__ = RuntimeError("deadlock detected")
    pa_qry = MagicMock()
    pa_qry.find = AsyncMock(return_value=_password_row(principal_id=pid))
    pa_cmd = MagicMock()
    pa_cmd.update = AsyncMock(side_effect=conflict)
    helper = AuthnCredentialDeactivationHelper(pa_qry=pa_qry, pa_cmd=pa_cmd)

    with pytest.raises(CoreException):
        await helper.deactivate_all(pid)

    assert pa_cmd.update.await_count == 1


@pytest.mark.asyncio
async def test_a_key_page_written_under_the_deactivation_is_read_again() -> None:
    # A rotation retired the key and minted another between the read and the update: the
    # page is read again, and the minted key is closed too.
    pid = uuid4()
    rotated = _api_key_row(principal_id=pid)
    retired = rotated.model_copy(update={"rev": 3, "is_active": False})
    minted = _api_key_row(principal_id=pid)
    ak_qry = MagicMock()
    ak_qry.find_many = AsyncMock(
        side_effect=[
            CountlessPage(hits=[rotated], page=1, size=10),
            CountlessPage(hits=[retired, minted], page=1, size=10),
        ]
    )
    ak_cmd = MagicMock()
    ak_cmd.update_many = AsyncMock(side_effect=[_stale(), None])

    await AuthnCredentialDeactivationHelper(ak_qry=ak_qry, ak_cmd=ak_cmd).deactivate_all(pid)

    assert ak_cmd.update_many.await_count == 2
    ak_cmd.update_many.assert_awaited_with(
        [KeyedUpdate(id=minted.id, rev=minted.rev, dto=UpdateApiKeyAccountCmd(is_active=False))],
        return_new=False,
    )

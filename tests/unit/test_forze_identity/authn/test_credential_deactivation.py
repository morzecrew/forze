"""Tests for :mod:`forze_identity.authn.adapters.credential_deactivation`."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import KeyedUpdate
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

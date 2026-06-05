"""Tests for current-password re-auth in :class:`PasswordLifecycleAdapter`."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze_identity.authn.adapters.password_lifecycle import PasswordLifecycleAdapter
from forze_identity.authn.domain.models.account import ReadPasswordAccount
from forze_identity.authn.services import PasswordService

# ----------------------- #


def _account(password_hash: str) -> ReadPasswordAccount:
    now = datetime.now(tz=timezone.utc)
    return ReadPasswordAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=uuid4(),
        username="bob",
        password_hash=password_hash,
        is_active=True,
    )


def _adapter(
    password_svc: PasswordService,
    account: ReadPasswordAccount,
) -> tuple[PasswordLifecycleAdapter, MagicMock]:
    pa_qry = MagicMock()
    pa_qry.spec = DocumentSpec(name="pwd", read=ReadPasswordAccount)
    pa_qry.find = AsyncMock(return_value=account)

    pa_cmd = MagicMock()
    pa_cmd.spec = DocumentSpec(name="pwd_cmd", read=ReadPasswordAccount)
    pa_cmd.update = AsyncMock(return_value=None)

    eligibility = MagicMock()
    eligibility.require_authentication_allowed = AsyncMock(return_value=None)

    adapter = PasswordLifecycleAdapter(
        password_svc=password_svc,
        pa_qry=pa_qry,
        pa_cmd=pa_cmd,
        eligibility=eligibility,
    )

    return adapter, pa_cmd


# ....................... #


@pytest.mark.asyncio
async def test_change_password_succeeds_with_correct_current_password() -> None:
    svc = PasswordService()
    account = _account(svc.hash_password("old-secret"))
    adapter, pa_cmd = _adapter(svc, account)

    await adapter.change_password(
        AuthnIdentity(principal_id=account.principal_id),
        "old-secret",
        "new-secret",
    )

    pa_cmd.update.assert_awaited_once()


@pytest.mark.asyncio
async def test_change_password_rejects_wrong_current_password() -> None:
    svc = PasswordService()
    account = _account(svc.hash_password("old-secret"))
    adapter, pa_cmd = _adapter(svc, account)

    with pytest.raises(CoreException, match="Current password is incorrect"):
        await adapter.change_password(
            AuthnIdentity(principal_id=account.principal_id),
            "wrong-secret",
            "new-secret",
        )

    pa_cmd.update.assert_not_awaited()

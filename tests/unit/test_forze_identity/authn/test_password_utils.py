"""Unit tests for password account lookup helpers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_identity.authn.adapters._utils import find_password_account_by_login
from forze_identity.authn.domain.models.account import ReadPasswordAccount


def _account(*, username: str) -> ReadPasswordAccount:
    now = utcnow()
    return ReadPasswordAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=uuid4(),
        username=username,
        password_hash="hash",
        is_active=True,
    )


@pytest.mark.asyncio
async def test_find_password_account_by_login_returns_none_when_missing() -> None:
    qry = MagicMock()
    qry.find_many = AsyncMock(return_value=CountlessPage(hits=[], page=1, size=0))

    got = await find_password_account_by_login(qry, "alice")

    assert got is None


@pytest.mark.asyncio
async def test_find_password_account_by_login_returns_single_hit() -> None:
    row = _account(username="alice")
    qry = MagicMock()
    qry.find_many = AsyncMock(return_value=CountlessPage(hits=[row], page=1, size=1))

    got = await find_password_account_by_login(qry, "alice")

    assert got is row


@pytest.mark.asyncio
async def test_find_password_account_by_login_raises_when_ambiguous() -> None:
    qry = MagicMock()
    qry.find_many = AsyncMock(
        return_value=CountlessPage(
            hits=[_account(username="alice"), _account(username="alicia")],
            page=1,
            size=2,
        ),
    )

    with pytest.raises(CoreException, match="Multiple password accounts") as ei:
        await find_password_account_by_login(qry, "alice")

    assert ei.value.kind is ExceptionKind.INTERNAL
    assert ei.value.code == "password_account_ambiguous"

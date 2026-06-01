"""Unit tests for :class:`~forze_identity.authn.adapters.password_provisioning.PasswordAccountProvisioningAdapter`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import PasswordCredentials
from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn.adapters.password_provisioning import PasswordAccountProvisioningAdapter
from forze_identity.authn.domain.models.account import (
    PasswordAccount,
    ReadPasswordAccount,
)

# ----------------------- #


def _spec_with_cache() -> DocumentSpec:
    return DocumentSpec(name="pwd", read=ReadPasswordAccount, cache=MagicMock())


def _adapter(**overrides: object) -> PasswordAccountProvisioningAdapter:
    password_account_qry = MagicMock()
    password_account_qry.spec = DocumentSpec(name="pwd", read=ReadPasswordAccount)
    password_account_cmd = MagicMock()
    password_account_cmd.spec = DocumentSpec(
        name="pwd",
        read=ReadPasswordAccount,
        write={
            "domain": PasswordAccount,
            "create_cmd": MagicMock(),
            "update_cmd": MagicMock(),
        },
    )
    eligibility = MagicMock()
    eligibility.require_authentication_allowed = AsyncMock()

    kwargs = {
        "password_svc": MagicMock(),
        "password_account_qry": password_account_qry,
        "password_account_cmd": password_account_cmd,
        "eligibility": eligibility,
    }
    kwargs.update(overrides)
    return PasswordAccountProvisioningAdapter(**kwargs)  # type: ignore[arg-type]


class TestPasswordAccountProvisioningInit:
    def test_rejects_password_account_query_cache(self) -> None:
        qry = MagicMock()
        qry.spec = _spec_with_cache()

        with pytest.raises(CoreException, match="Password account caching"):
            _adapter(password_account_qry=qry)

    @pytest.mark.asyncio
    async def test_accept_invite_not_implemented(self) -> None:
        adapter = _adapter()
        with pytest.raises(NotImplementedError, match="Invite token"):
            await adapter.accept_invite_with_password(
                "token",
                uuid4(),
                MagicMock(),
            )


class TestPasswordAccountProvisioningRegister:
    @pytest.mark.asyncio
    async def test_register_rejects_duplicate_login(self) -> None:
        existing = MagicMock()
        qry = MagicMock()
        qry.spec = DocumentSpec(name="pwd", read=ReadPasswordAccount)
        qry.find_many = AsyncMock(
            return_value=CountlessPage(hits=[existing], page=1, size=1),
        )

        adapter = _adapter(password_account_qry=qry)

        with pytest.raises(CoreException, match="already exists") as ei:
            await adapter.register_with_password(
                uuid4(),
                PasswordCredentials(login="alice", password="secret"),
            )

        assert ei.value.kind is ExceptionKind.CONFLICT
        assert ei.value.code == "password_account_exists"
        adapter.password_account_cmd.create.assert_not_called()

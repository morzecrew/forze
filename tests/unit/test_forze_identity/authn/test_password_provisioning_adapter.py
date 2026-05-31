"""Unit tests for :class:`~forze_identity.authn.adapters.password_provisioning.PasswordAccountProvisioningAdapter`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
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
                __import__("uuid").uuid4(),
                MagicMock(),
            )

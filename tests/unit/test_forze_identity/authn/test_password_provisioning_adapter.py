"""Unit tests for :class:`~forze_identity.authn.adapters.password_provisioning.PasswordAccountProvisioningAdapter`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity, PasswordCredentials
from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn.adapters.password_provisioning import PasswordAccountProvisioningAdapter
from forze_identity.authn.domain.models.account import (
    PasswordAccount,
    ReadPasswordAccount,
)
from forze_identity.authn.domain.models.invite import ReadPasswordInvite
from forze_identity.authn.services import InviteTokenConfig, InviteTokenService

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
    async def test_invites_require_configuration(self) -> None:
        adapter = _adapter()

        with pytest.raises(CoreException, match="kernel.invite_token_pepper"):
            await adapter.issue_password_invite(
                AuthnIdentity(principal_id=uuid4()),
                uuid4(),
            )

        with pytest.raises(CoreException, match="kernel.invite_token_pepper"):
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


# ----------------------- #


def _invite_qry() -> MagicMock:
    qry = MagicMock()
    qry.spec = DocumentSpec(name="invite", read=ReadPasswordInvite)
    return qry


def _invite_cmd() -> MagicMock:
    cmd = MagicMock()
    cmd.spec = DocumentSpec(name="invite", read=ReadPasswordInvite)
    cmd.create = AsyncMock()
    cmd.update = AsyncMock()
    return cmd


def _invite_adapter(
    invite_qry: MagicMock,
    invite_cmd: MagicMock,
    **overrides: object,
) -> PasswordAccountProvisioningAdapter:
    return _adapter(
        invite_svc=InviteTokenService(pepper=b"p" * 32, config=InviteTokenConfig()),
        invite_qry=invite_qry,
        invite_cmd=invite_cmd,
        **overrides,
    )


def _read_invite(
    *,
    principal_id,
    token_digest: str,
    expires_at: datetime,
    consumed_at: datetime | None = None,
) -> ReadPasswordInvite:
    now = datetime.now(tz=UTC)
    return ReadPasswordInvite(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=principal_id,
        token_digest=token_digest,
        expires_at=expires_at,
        consumed_at=consumed_at,
    )


class TestPasswordInviteIssue:
    @pytest.mark.asyncio
    async def test_issue_persists_digest_and_returns_raw_token(self) -> None:
        cmd = _invite_cmd()
        adapter = _invite_adapter(_invite_qry(), cmd)
        operator = AuthnIdentity(principal_id=uuid4())
        principal_id = uuid4()

        issued = await adapter.issue_password_invite(operator, principal_id)

        assert issued.token
        assert issued.principal_id == principal_id
        cmd.create.assert_awaited_once()
        create_cmd = cmd.create.await_args.args[0]
        # The raw token is never persisted; only its digest.
        assert create_cmd.token_digest == adapter.invite_svc.calculate_token_digest(
            issued.token
        )
        assert create_cmd.principal_id == principal_id


class TestPasswordInviteAccept:
    @pytest.mark.asyncio
    async def test_accept_provisions_and_marks_consumed(self) -> None:
        principal_id = uuid4()
        svc = InviteTokenService(pepper=b"p" * 32, config=InviteTokenConfig())
        token = svc.generate_token()
        invite = _read_invite(
            principal_id=principal_id,
            token_digest=svc.calculate_token_digest(token),
            expires_at=datetime.now(tz=UTC) + timedelta(days=1),
        )

        qry = _invite_qry()
        qry.find = AsyncMock(return_value=invite)
        cmd = _invite_cmd()

        pwd_qry = MagicMock()
        pwd_qry.spec = DocumentSpec(name="pwd", read=ReadPasswordAccount)
        pwd_qry.find_many = AsyncMock(
            return_value=CountlessPage(hits=[], page=1, size=0),
        )

        pwd_cmd = MagicMock()
        pwd_cmd.spec = DocumentSpec(
            name="pwd",
            read=ReadPasswordAccount,
            write={
                "domain": PasswordAccount,
                "create_cmd": MagicMock(),
                "update_cmd": MagicMock(),
            },
        )
        pwd_cmd.create = AsyncMock()

        password_svc = MagicMock()
        password_svc.hash_password = MagicMock(return_value="hashed")

        adapter = _adapter(
            password_svc=password_svc,
            password_account_qry=pwd_qry,
            password_account_cmd=pwd_cmd,
            invite_svc=svc,
            invite_qry=qry,
            invite_cmd=cmd,
        )

        await adapter.accept_invite_with_password(
            token,
            principal_id,
            PasswordCredentials(login="alice", password="secret"),
        )

        pwd_cmd.create.assert_awaited_once()
        cmd.update.assert_awaited_once()
        update_cmd = cmd.update.await_args.args[2]
        assert update_cmd.consumed_at is not None

    @pytest.mark.asyncio
    async def test_accept_rejects_empty_token(self) -> None:
        adapter = _invite_adapter(_invite_qry(), _invite_cmd())

        with pytest.raises(CoreException, match="Invite token is required"):
            await adapter.accept_invite_with_password(
                "",
                uuid4(),
                PasswordCredentials(login="alice", password="secret"),
            )

    @pytest.mark.asyncio
    async def test_accept_rejects_unknown_token(self) -> None:
        svc = InviteTokenService(pepper=b"p" * 32, config=InviteTokenConfig())
        qry = _invite_qry()
        qry.find = AsyncMock(return_value=None)

        adapter = _adapter(invite_svc=svc, invite_qry=qry, invite_cmd=_invite_cmd())

        with pytest.raises(CoreException, match="Invalid invite token"):
            await adapter.accept_invite_with_password(
                svc.generate_token(),
                uuid4(),
                PasswordCredentials(login="alice", password="secret"),
            )

    @pytest.mark.asyncio
    async def test_accept_rejects_consumed_invite(self) -> None:
        principal_id = uuid4()
        svc = InviteTokenService(pepper=b"p" * 32, config=InviteTokenConfig())
        token = svc.generate_token()
        invite = _read_invite(
            principal_id=principal_id,
            token_digest=svc.calculate_token_digest(token),
            expires_at=datetime.now(tz=UTC) + timedelta(days=1),
            consumed_at=datetime.now(tz=UTC),
        )
        qry = _invite_qry()
        qry.find = AsyncMock(return_value=invite)

        adapter = _adapter(invite_svc=svc, invite_qry=qry, invite_cmd=_invite_cmd())

        with pytest.raises(CoreException, match="Invalid invite token"):
            await adapter.accept_invite_with_password(
                token,
                principal_id,
                PasswordCredentials(login="alice", password="secret"),
            )

    @pytest.mark.asyncio
    async def test_accept_rejects_principal_mismatch(self) -> None:
        svc = InviteTokenService(pepper=b"p" * 32, config=InviteTokenConfig())
        token = svc.generate_token()
        invite = _read_invite(
            principal_id=uuid4(),
            token_digest=svc.calculate_token_digest(token),
            expires_at=datetime.now(tz=UTC) + timedelta(days=1),
        )
        qry = _invite_qry()
        qry.find = AsyncMock(return_value=invite)

        adapter = _adapter(invite_svc=svc, invite_qry=qry, invite_cmd=_invite_cmd())

        with pytest.raises(CoreException, match="Invalid invite token"):
            await adapter.accept_invite_with_password(
                token,
                uuid4(),
                PasswordCredentials(login="alice", password="secret"),
            )

    @pytest.mark.asyncio
    async def test_accept_rejects_expired_invite(self) -> None:
        principal_id = uuid4()
        svc = InviteTokenService(pepper=b"p" * 32, config=InviteTokenConfig())
        token = svc.generate_token()
        invite = _read_invite(
            principal_id=principal_id,
            token_digest=svc.calculate_token_digest(token),
            expires_at=datetime.now(tz=UTC) - timedelta(seconds=1),
        )
        qry = _invite_qry()
        qry.find = AsyncMock(return_value=invite)

        adapter = _adapter(invite_svc=svc, invite_qry=qry, invite_cmd=_invite_cmd())

        with pytest.raises(CoreException, match="expired"):
            await adapter.accept_invite_with_password(
                token,
                principal_id,
                PasswordCredentials(login="alice", password="secret"),
            )
        adapter.password_account_cmd.create.assert_not_called()

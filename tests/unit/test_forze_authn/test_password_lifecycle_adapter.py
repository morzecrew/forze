"""Tests for current-password re-auth and session revocation in :class:`PasswordLifecycleAdapter`."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnIdentity,
    RefreshTokenCredentials,
)
from forze.application.contracts.base import CountlessPage
from forze.application.contracts.document import DocumentSpec, KeyedUpdate
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_identity.authn.adapters.password_lifecycle import PasswordLifecycleAdapter
from forze_identity.authn.adapters.token_lifecycle import TokenLifecycleAdapter
from forze_identity.authn.domain.models.account import ReadPasswordAccount
from forze_identity.authn.domain.models.session import ReadSession
from forze_identity.authn.execution import (
    AuthnKernelConfig,
    build_authn_shared_services,
)
from forze_identity.authn.services import PasswordConfig, PasswordService
from forze_identity.authn.verifiers import ForzeJwtTokenVerifier

# ----------------------- #


def _slow_password_config() -> PasswordConfig:
    return PasswordConfig(time_cost=1, memory_cost=8192, parallelism=1)


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


class _SessionStore:
    """Minimal in-memory session document store mimicking the document ports."""

    def __init__(self) -> None:
        self.sessions: dict[UUID, ReadSession] = {}

    # ....................... #

    def _matches(self, session: ReadSession, values: dict[str, Any]) -> bool:
        return all(getattr(session, k) == v for k, v in values.items())

    # ....................... #

    def qry(self) -> MagicMock:
        port = MagicMock()
        port.spec = DocumentSpec(name="sess", read=ReadSession)

        async def find(filters: dict[str, Any]) -> ReadSession | None:
            values = filters["$values"]
            for session in self.sessions.values():
                if self._matches(session, values):
                    return session
            return None

        async def find_many(filters: dict[str, Any]) -> CountlessPage[ReadSession]:
            values = filters["$values"]
            hits = [s for s in self.sessions.values() if self._matches(s, values)]
            return CountlessPage(hits=hits, page=1, size=len(hits))

        port.find = AsyncMock(side_effect=find)
        port.find_many = AsyncMock(side_effect=find_many)

        return port

    # ....................... #

    def cmd(self) -> MagicMock:
        port = MagicMock()
        port.spec = DocumentSpec(name="sess", read=ReadSession)

        async def create(cmd: Any, return_new: bool = True) -> ReadSession:
            now = utcnow()
            session = ReadSession(
                id=uuid4(),
                rev=1,
                created_at=now,
                last_update_at=now,
                **cmd.model_dump(),
            )
            self.sessions[session.id] = session
            return session

        async def update(
            doc_id: UUID,
            rev: int,
            cmd: Any,
            return_new: bool = True,
        ) -> ReadSession:
            current = self.sessions[doc_id]
            patch = cmd.model_dump(exclude_none=True)
            updated = current.model_copy(update={**patch, "rev": rev + 1})
            self.sessions[doc_id] = updated
            return updated

        async def update_many(
            upds: list[KeyedUpdate[Any]],
            return_new: bool = True,
        ) -> list[ReadSession]:
            return [await update(u.id, u.rev, u.dto) for u in upds]

        port.create = AsyncMock(side_effect=create)
        port.update = AsyncMock(side_effect=update)
        port.update_many = AsyncMock(side_effect=update_many)

        return port


def _eligibility() -> MagicMock:
    eligibility = MagicMock()
    eligibility.require_authentication_allowed = AsyncMock(return_value=None)
    return eligibility


def _adapter(
    password_svc: PasswordService,
    account: ReadPasswordAccount,
    *,
    store: _SessionStore | None = None,
    revoke_sessions_on_password_change: bool = True,
) -> tuple[PasswordLifecycleAdapter, MagicMock]:
    pa_qry = MagicMock()
    pa_qry.spec = DocumentSpec(name="pwd", read=ReadPasswordAccount)
    pa_qry.find = AsyncMock(return_value=account)

    pa_cmd = MagicMock()
    pa_cmd.spec = DocumentSpec(name="pwd_cmd", read=ReadPasswordAccount)
    pa_cmd.update = AsyncMock(return_value=None)

    adapter = PasswordLifecycleAdapter(
        password_svc=password_svc,
        pa_qry=pa_qry,
        pa_cmd=pa_cmd,
        eligibility=_eligibility(),
        session_qry=store.qry() if store is not None else None,
        session_cmd=store.cmd() if store is not None else None,
        revoke_sessions_on_password_change=revoke_sessions_on_password_change,
    )

    return adapter, pa_cmd


def _token_lifecycle(store: _SessionStore) -> tuple[TokenLifecycleAdapter, Any]:
    shared = build_authn_shared_services(
        AuthnKernelConfig(
            access_token_secret=b"k" * 32,
            refresh_token_pepper=b"p" * 32,
        ),
    )

    adapter = TokenLifecycleAdapter(
        access_svc=shared.access_svc,
        refresh_svc=shared.refresh_svc,
        session_qry=store.qry(),
        session_cmd=store.cmd(),
        eligibility=_eligibility(),
    )

    return adapter, shared.access_svc


# ....................... #


@pytest.mark.asyncio
async def test_change_password_succeeds_with_correct_current_password() -> None:
    svc = PasswordService(config=_slow_password_config())
    account = _account(svc.hash_password_sync("old-secret"))
    adapter, pa_cmd = _adapter(svc, account, store=_SessionStore())

    await adapter.change_password(
        AuthnIdentity(principal_id=account.principal_id),
        "old-secret",
        "new-secret",
    )

    pa_cmd.update.assert_awaited_once()


@pytest.mark.asyncio
async def test_change_password_rejects_wrong_current_password() -> None:
    svc = PasswordService(config=_slow_password_config())
    account = _account(svc.hash_password_sync("old-secret"))
    adapter, pa_cmd = _adapter(svc, account, store=_SessionStore())

    with pytest.raises(CoreException, match="Current password is incorrect"):
        await adapter.change_password(
            AuthnIdentity(principal_id=account.principal_id),
            "wrong-secret",
            "new-secret",
        )

    pa_cmd.update.assert_not_awaited()


# ....................... #
# Session revocation cascade ("log out everywhere")


@pytest.mark.asyncio
async def test_change_password_revokes_all_sessions() -> None:
    svc = PasswordService(config=_slow_password_config())
    account = _account(svc.hash_password_sync("old-secret"))
    identity = AuthnIdentity(principal_id=account.principal_id)

    store = _SessionStore()
    token_lifecycle, access_svc = _token_lifecycle(store)

    first = await token_lifecycle.issue_tokens(identity)
    second = await token_lifecycle.issue_tokens(identity)

    adapter, _ = _adapter(svc, account, store=store)
    await adapter.change_password(identity, "old-secret", "new-secret")

    assert len(store.sessions) == 2
    assert all(s.revoked_at is not None for s in store.sessions.values())

    # Refresh-token families are dead: rotation of a revoked session is rejected.
    for issued in (first, second):
        with pytest.raises(CoreException, match="Invalid refresh token"):
            await token_lifecycle.refresh_tokens(
                RefreshTokenCredentials(token=issued.refresh.token.token),
            )

    # sid-bound access JWTs fail verification before their exp.
    verifier = ForzeJwtTokenVerifier(access_svc=access_svc, session_qry=store.qry())

    for issued in (first, second):
        with pytest.raises(CoreException, match="Session revoked"):
            await verifier.verify_token(
                AccessTokenCredentials(token=issued.access.token.token),
            )


@pytest.mark.asyncio
async def test_change_password_opt_out_keeps_sessions_alive() -> None:
    svc = PasswordService(config=_slow_password_config())
    account = _account(svc.hash_password_sync("old-secret"))
    identity = AuthnIdentity(principal_id=account.principal_id)

    store = _SessionStore()
    token_lifecycle, access_svc = _token_lifecycle(store)
    issued = await token_lifecycle.issue_tokens(identity)

    adapter, pa_cmd = _adapter(
        svc,
        account,
        store=None,
        revoke_sessions_on_password_change=False,
    )

    await adapter.change_password(identity, "old-secret", "new-secret")

    pa_cmd.update.assert_awaited_once()
    assert all(s.revoked_at is None for s in store.sessions.values())

    verifier = ForzeJwtTokenVerifier(access_svc=access_svc, session_qry=store.qry())
    assertion = await verifier.verify_token(
        AccessTokenCredentials(token=issued.access.token.token),
    )

    assert assertion.subject == str(account.principal_id)


@pytest.mark.asyncio
async def test_change_password_does_not_revoke_when_current_password_wrong() -> None:
    svc = PasswordService(config=_slow_password_config())
    account = _account(svc.hash_password_sync("old-secret"))
    identity = AuthnIdentity(principal_id=account.principal_id)

    store = _SessionStore()
    token_lifecycle, _ = _token_lifecycle(store)
    await token_lifecycle.issue_tokens(identity)

    adapter, _ = _adapter(svc, account, store=store)

    with pytest.raises(CoreException, match="Current password is incorrect"):
        await adapter.change_password(identity, "wrong-secret", "new-secret")

    assert all(s.revoked_at is None for s in store.sessions.values())


def test_revocation_default_requires_session_ports() -> None:
    svc = PasswordService(config=_slow_password_config())
    account = _account(svc.hash_password_sync("old-secret"))

    with pytest.raises(CoreException, match="revoke_sessions_on_password_change"):
        _adapter(svc, account, store=None)

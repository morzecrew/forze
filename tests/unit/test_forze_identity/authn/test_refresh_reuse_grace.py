"""Overlapping refreshes within the reuse grace are superseded, not reuse.

Tabs sharing one refresh cookie refresh at overlapping moments with the same token. Within
``RefreshTokenConfig.reuse_grace`` of its rotation, and while its successor is unused, the
rotated token is refused as ``refresh_superseded`` and nothing is revoked; outside it, or once
the successor has rotated, it is reuse and the family is revoked, as with no grace at all.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.authn import (
    AuthnEvent,
    AuthnEventEmitter,
    AuthnEventKind,
    AuthnIdentity,
    IssuedTokens,
    RefreshTokenCredentials,
    RefreshTokenReuseError,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_identity.authn.adapters.token_lifecycle import TokenLifecycleAdapter
from forze_identity.authn.application.specs import session_spec
from forze_identity.authn.domain.models.session import ReadSession, Session
from forze_identity.authn.services import (
    AccessTokenService,
    Hs256Signer,
    RefreshTokenConfig,
    RefreshTokenService,
)
from forze_mock import MockState
from forze_mock.adapters.document import MockDocumentAdapter
from forze_mock.adapters.identity.authn import MockTokenLifecyclePort

pytestmark = pytest.mark.unit

GRACE = timedelta(seconds=10)
REAL = "forze_identity.authn.adapters.token_lifecycle.utcnow"
MOCK = "forze_mock.adapters.identity.authn.utcnow"


class _Sink:
    def __init__(self) -> None:
        self.kinds: list[AuthnEventKind] = []

    async def record(self, event: AuthnEvent) -> None:
        self.kinds.append(event.kind)


class _Gate:
    """Eligibility that, once armed, holds the next call until released, as a slow request."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.armed = False

    async def __call__(self, *_: Any, **__: Any) -> None:
        if self.armed:
            self.armed = False
            self.entered.set()
            await self.release.wait()


def _real(
    grace: timedelta, *, gate: _Gate | None = None
) -> tuple[TokenLifecycleAdapter, _Sink, MockDocumentAdapter[Any, Any, Any, Any]]:
    sessions = MockDocumentAdapter(
        spec=session_spec,
        state=MockState(),
        namespace="sessions",
        read_model=ReadSession,
        domain_model=Session,
    )
    eligibility = MagicMock()
    eligibility.require_authentication_allowed = gate or AsyncMock()
    sink = _Sink()
    adapter = TokenLifecycleAdapter(
        access_svc=AccessTokenService(signer=Hs256Signer(secret=b"k" * 32)),
        refresh_svc=RefreshTokenService(
            pepper=b"p" * 32,
            config=RefreshTokenConfig(reuse_grace=grace),
        ),
        session_qry=sessions,
        session_cmd=sessions,
        eligibility=eligibility,
        events=AuthnEventEmitter(sink=sink, route="main"),
    )

    return adapter, sink, sessions


def _refresh(issued: IssuedTokens) -> RefreshTokenCredentials:
    assert issued.refresh is not None

    return issued.refresh.token


async def _live(sessions: MockDocumentAdapter[Any, Any, Any, Any]) -> int:
    page = await sessions.find_many()

    return sum(1 for s in page.hits if s.revoked_at is None and s.rotated_at is None)


# ----------------------- #


async def test_without_a_grace_a_just_rotated_token_revokes_the_family() -> None:
    adapter, sink, sessions = _real(timedelta(0))
    old = _refresh(await adapter.issue_tokens(AuthnIdentity(principal_id=uuid4())))
    await adapter.refresh_tokens(old)

    with pytest.raises(RefreshTokenReuseError):
        await adapter.refresh_tokens(old)

    assert await _live(sessions) == 0
    assert AuthnEventKind.REFRESH_REUSE_DETECTED in sink.kinds


async def test_within_the_grace_a_rotated_token_is_superseded_and_nothing_is_revoked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, sink, sessions = _real(GRACE)
    old = _refresh(await adapter.issue_tokens(AuthnIdentity(principal_id=uuid4())))
    successor = _refresh(await adapter.refresh_tokens(old))
    later = utcnow() + timedelta(seconds=2)
    monkeypatch.setattr(REAL, lambda: later)

    with pytest.raises(CoreException) as caught:
        await adapter.refresh_tokens(old)

    assert caught.value.code == "refresh_superseded"
    assert not isinstance(caught.value, RefreshTokenReuseError)
    assert sink.kinds[-1] is AuthnEventKind.REFRESH_SUPERSEDED
    assert await _live(sessions) == 1

    # The successor the winning refresh issued still refreshes.
    assert (await adapter.refresh_tokens(successor)).refresh is not None


async def test_past_the_grace_a_rotated_token_is_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, _, sessions = _real(GRACE)
    old = _refresh(await adapter.issue_tokens(AuthnIdentity(principal_id=uuid4())))
    await adapter.refresh_tokens(old)
    later = utcnow() + timedelta(seconds=11)
    monkeypatch.setattr(REAL, lambda: later)

    with pytest.raises(RefreshTokenReuseError):
        await adapter.refresh_tokens(old)

    assert await _live(sessions) == 0


async def test_within_the_grace_a_token_whose_successor_rotated_is_reuse() -> None:
    adapter, _, sessions = _real(GRACE)
    old = _refresh(await adapter.issue_tokens(AuthnIdentity(principal_id=uuid4())))
    successor = _refresh(await adapter.refresh_tokens(old))
    await adapter.refresh_tokens(successor)

    with pytest.raises(RefreshTokenReuseError):
        await adapter.refresh_tokens(old)

    assert await _live(sessions) == 0


@pytest.mark.parametrize(
    ("grace", "code"),
    [(GRACE, "refresh_superseded"), (timedelta(0), "revision_mismatch")],
    ids=["grace", "no-grace"],
)
async def test_a_refresh_losing_a_concurrent_rotation(grace: timedelta, code: str) -> None:
    # Both read the session before either rotates it: the loser's revision is stale.
    gate = _Gate()
    adapter, _, _ = _real(grace, gate=gate)
    old = _refresh(await adapter.issue_tokens(AuthnIdentity(principal_id=uuid4())))
    gate.armed = True

    async with asyncio.timeout(5):
        loser = asyncio.create_task(adapter.refresh_tokens(old))
        await gate.entered.wait()
        successor = _refresh(await adapter.refresh_tokens(old))
        gate.release.set()

        with pytest.raises(CoreException) as caught:
            await loser

    assert caught.value.code == code
    assert (await adapter.refresh_tokens(successor)).refresh is not None


async def test_a_refresh_losing_to_a_concurrent_logout_keeps_its_conflict() -> None:
    # The write that won revoked the session rather than rotating it: nothing to supersede.
    gate = _Gate()
    adapter, _, _ = _real(GRACE, gate=gate)
    identity = AuthnIdentity(principal_id=uuid4())
    old = _refresh(await adapter.issue_tokens(identity))
    gate.armed = True

    async with asyncio.timeout(5):
        loser = asyncio.create_task(adapter.refresh_tokens(old))
        await gate.entered.wait()
        await adapter.revoke_tokens(identity)
        gate.release.set()

        with pytest.raises(CoreException) as caught:
            await loser

    assert caught.value.code == "revision_mismatch"


async def test_within_the_grace_a_token_whose_successor_was_revoked_is_reuse() -> None:
    adapter, sink, _ = _real(GRACE)
    identity = AuthnIdentity(principal_id=uuid4())
    old = _refresh(await adapter.issue_tokens(identity))
    await adapter.refresh_tokens(old)
    await adapter.revoke_tokens(identity)

    with pytest.raises(RefreshTokenReuseError):
        await adapter.refresh_tokens(old)

    assert sink.kinds[-1] is AuthnEventKind.REFRESH_REUSE_DETECTED


async def test_within_the_grace_a_rotation_failing_otherwise_is_not_superseded() -> None:
    adapter, _, sessions = _real(GRACE)
    old = _refresh(await adapter.issue_tokens(AuthnIdentity(principal_id=uuid4())))
    broken = CoreException(kind=ExceptionKind.INFRASTRUCTURE, summary="down", code="down")

    class _FailingUpdate:
        def __getattr__(self, name: str) -> Any:
            return getattr(sessions, name)

        async def update(self, *_: Any, **__: Any) -> None:
            raise broken

    with pytest.raises(CoreException) as caught:
        await attrs.evolve(adapter, session_cmd=_FailingUpdate()).refresh_tokens(old)

    assert caught.value is broken


@pytest.mark.parametrize("grace", [timedelta(seconds=-1), timedelta(days=7)])
def test_a_grace_outside_the_refresh_lifetime_is_refused(grace: timedelta) -> None:
    with pytest.raises(CoreException) as caught:
        RefreshTokenConfig(expires_in=timedelta(days=7), reuse_grace=grace)

    assert caught.value.code == "core.configuration"

    with pytest.raises(CoreException) as caught:
        MockTokenLifecyclePort(
            state=MockState(),
            refresh_expires_in=timedelta(days=7),
            refresh_reuse_grace=grace,
        )

    assert caught.value.code == "core.configuration"


# ....................... #


async def test_the_mock_supersedes_within_the_grace_and_detects_reuse_past_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = _Sink()
    mock = MockTokenLifecyclePort(
        state=MockState(),
        refresh_reuse_grace=GRACE,
        events=AuthnEventEmitter(sink=sink, route="main"),
    )
    old = _refresh(await mock.issue_tokens(AuthnIdentity(principal_id=uuid4())))
    successor = _refresh(await mock.refresh_tokens(old))

    with pytest.raises(CoreException) as caught:
        await mock.refresh_tokens(old)

    assert caught.value.code == "refresh_superseded"
    assert sink.kinds[-1] is AuthnEventKind.REFRESH_SUPERSEDED

    later = utcnow() + timedelta(seconds=11)
    monkeypatch.setattr(MOCK, lambda: later)

    with pytest.raises(RefreshTokenReuseError):
        await mock.refresh_tokens(old)

    # Reuse revoked the family, the successor with it.
    with pytest.raises(CoreException):
        await mock.refresh_tokens(successor)

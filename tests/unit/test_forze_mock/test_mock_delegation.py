"""Tests for the in-memory mock delegation (``may_act``) ports."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze_mock.adapters.identity import MockDelegationGrantPort, MockDelegationPort
from forze_mock.state import MockState

pytestmark = pytest.mark.unit


def _ports() -> tuple[MockDelegationGrantPort, MockDelegationPort]:
    state = MockState()
    return (
        MockDelegationGrantPort(state=state, route="main"),
        MockDelegationPort(state=state, route="main"),
    )


@pytest.mark.asyncio
async def test_deny_unless_granted_by_default() -> None:
    _, query = _ports()
    assert await query.may_act(uuid4(), uuid4()) is False


@pytest.mark.asyncio
async def test_grant_then_may_act() -> None:
    grants, query = _ports()
    actor, subject = uuid4(), uuid4()

    await grants.grant_delegation(AuthnIdentity(principal_id=actor), subject)

    assert await query.may_act(actor, subject) is True
    # Asymmetric: the reverse pairing is not granted.
    assert await query.may_act(subject, actor) is False


@pytest.mark.asyncio
async def test_revoke_removes_grant() -> None:
    grants, query = _ports()
    actor, subject = uuid4(), uuid4()

    await grants.grant_delegation(actor, subject)
    await grants.revoke_delegation(actor, subject)

    assert await query.may_act(actor, subject) is False


@pytest.mark.asyncio
async def test_list_delegators() -> None:
    grants, _ = _ports()
    actor = uuid4()
    s1, s2 = uuid4(), uuid4()

    await grants.grant_delegation(actor, s1)
    await grants.grant_delegation(actor, s2)
    await grants.grant_delegation(uuid4(), uuid4())  # unrelated

    assert await grants.list_delegators(actor) == frozenset({s1, s2})


@pytest.mark.asyncio
async def test_allow_by_default_override() -> None:
    state = MockState()
    query = MockDelegationPort(state=state, route="main", allow_by_default=True)
    assert await query.may_act(uuid4(), uuid4()) is True

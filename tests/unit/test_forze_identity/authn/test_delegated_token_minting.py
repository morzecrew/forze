"""A session is never minted for a delegated identity.

A token carries the principal alone. Minting one for an agent acting for a user would hand the
agent a credential that authenticates as the user with no actor, outside the intersection of the
two. The real lifecycle and the mock refuse it before anything is written.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn.adapters.token_lifecycle import TokenLifecycleAdapter
from forze_identity.authn.domain.models.session import ReadSession
from forze_mock import MockState
from forze_mock.adapters.identity.authn import MockTokenLifecyclePort

pytestmark = pytest.mark.unit

AS_AGENT_FOR_USER = AuthnIdentity(
    principal_id=uuid4(), actor=AuthnIdentity(principal_id=uuid4())
)


def _real() -> tuple[TokenLifecycleAdapter, MagicMock]:
    session_qry = MagicMock()
    session_qry.spec = DocumentSpec(name="sessions", read=ReadSession)
    session_cmd = MagicMock()
    session_cmd.spec = DocumentSpec(name="sessions", read=ReadSession)
    session_cmd.create = AsyncMock()
    eligibility = MagicMock()
    eligibility.require_authentication_allowed = AsyncMock()
    adapter = TokenLifecycleAdapter(
        access_svc=MagicMock(),
        refresh_svc=MagicMock(),
        session_qry=session_qry,
        session_cmd=session_cmd,
        eligibility=eligibility,
    )
    return adapter, session_cmd


async def test_the_lifecycle_refuses_a_delegated_identity() -> None:
    adapter, session_cmd = _real()

    with pytest.raises(CoreException) as caught:
        await adapter.issue_tokens(AS_AGENT_FOR_USER, tenant_id=uuid4())

    assert caught.value.kind is ExceptionKind.AUTHORIZATION
    assert caught.value.code == "delegate_denied"
    session_cmd.create.assert_not_awaited()


async def test_the_mock_refuses_a_delegated_identity() -> None:
    state = MockState()

    with pytest.raises(CoreException) as caught:
        await MockTokenLifecyclePort(state=state).issue_tokens(AS_AGENT_FOR_USER)

    assert caught.value.code == "delegate_denied"


async def test_the_mock_still_mints_for_the_principal_itself() -> None:
    issued = await MockTokenLifecyclePort(state=MockState()).issue_tokens(
        AuthnIdentity(principal_id=uuid4())
    )

    assert issued.access is not None

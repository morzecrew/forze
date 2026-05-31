"""Unit tests for :class:`PolicyPrincipalEligibilityAdapter`."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn.adapters.principal_eligibility import PolicyPrincipalEligibilityAdapter
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal

pytestmark = pytest.mark.unit


def _adapter(*, principal: ReadPolicyPrincipal | None) -> PolicyPrincipalEligibilityAdapter:
    qry = MagicMock()
    qry.spec = DocumentSpec(name="policy_principals", read=ReadPolicyPrincipal)
    qry.find = AsyncMock(return_value=principal)
    return PolicyPrincipalEligibilityAdapter(principal_qry=qry)


@pytest.mark.asyncio
async def test_require_authentication_allowed_active_principal() -> None:
    pid = uuid4()
    now = datetime.now(tz=UTC)
    principal = ReadPolicyPrincipal(
        id=pid,
        rev=1,
        created_at=now,
        last_update_at=now,
        kind="user",
        is_active=True,
    )
    adapter = _adapter(principal=principal)

    await adapter.require_authentication_allowed(pid)


@pytest.mark.asyncio
async def test_require_authentication_allowed_inactive_principal() -> None:
    pid = uuid4()
    now = datetime.now(tz=UTC)
    principal = ReadPolicyPrincipal(
        id=pid,
        rev=1,
        created_at=now,
        last_update_at=now,
        kind="user",
        is_active=False,
    )
    adapter = _adapter(principal=principal)

    with pytest.raises(CoreException) as exc_info:
        await adapter.require_authentication_allowed(pid)

    assert exc_info.value.kind is ExceptionKind.AUTHENTICATION


@pytest.mark.asyncio
async def test_require_authentication_allowed_missing_principal() -> None:
    pid = uuid4()
    adapter = _adapter(principal=None)

    with pytest.raises(CoreException) as exc_info:
        await adapter.require_authentication_allowed(pid)

    assert exc_info.value.kind is ExceptionKind.AUTHENTICATION

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


@pytest.mark.asyncio
async def test_allow_all_admits_any_principal() -> None:
    # The declared opt-out for token-only deployments without the authz plane:
    # no policy_principal document, no upsert bookkeeping — every principal is
    # eligible, and revocation lives entirely on credential lifecycle.
    from forze_identity.authn.adapters.principal_eligibility import (
        AllowAllPrincipalEligibilityAdapter,
    )

    adapter = AllowAllPrincipalEligibilityAdapter()

    await adapter.require_authentication_allowed(uuid4())  # never raises


@pytest.mark.asyncio
async def test_module_eligibility_knob_selects_the_gate() -> None:
    from forze_identity.authn.execution.deps.deps import (
        ConfigurableAllowAllEligibility,
        ConfigurablePolicyPrincipalEligibility,
    )
    from forze_identity.authn.execution.deps.module import AuthnDepsModule

    assert AuthnDepsModule().eligibility == "policy_principal"
    assert AuthnDepsModule(eligibility="allow_all").eligibility == "allow_all"

    # the factories build the matching adapters
    from forze_identity.authn.adapters.principal_eligibility import (
        AllowAllPrincipalEligibilityAdapter,
    )

    built = ConfigurableAllowAllEligibility()(MagicMock(), MagicMock())
    assert isinstance(built, AllowAllPrincipalEligibilityAdapter)
    assert isinstance(
        ConfigurablePolicyPrincipalEligibility, type
    )  # the default stays constructible

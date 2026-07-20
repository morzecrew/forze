"""Unit tests for tenancy operation-plan hooks."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, InvocationMetadata
from forze.application.hooks.tenancy import TenantRequired
from forze.base.exceptions import CoreException
from tests.support.execution_context import (
    context_from_deps,
)

pytestmark = pytest.mark.unit

@pytest.mark.asyncio
async def test_tenancy_before_required_allows_when_bound() -> None:
    ctx = context_from_deps(Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    tenant = TenantIdentity(tenant_id=uuid4())

    with ctx.inv_ctx.bind(metadata=metadata, tenant=tenant):
        hook = TenantRequired()(ctx)
        await hook(None)

@pytest.mark.asyncio
async def test_tenancy_before_required_denies_when_missing() -> None:
    ctx = context_from_deps(Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv_ctx.bind(metadata=metadata):
        hook = TenantRequired()(ctx)

        with pytest.raises(CoreException) as exc_info:
            await hook(None)

    assert exc_info.value.code == "tenant_required"

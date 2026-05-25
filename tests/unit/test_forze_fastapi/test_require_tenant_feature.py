"""Tests for :class:`RequireTenantFeature`."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_fastapi.endpoints.http.features.security import RequireTenantFeature

pytestmark = pytest.mark.unit


async def _handler(ctx) -> str:  # type: ignore[no-untyped-def]
    return "ok"


def _http_ctx(exec_ctx: ExecutionContext) -> object:
    return SimpleNamespace(exec_ctx=exec_ctx)


@pytest.mark.asyncio
async def test_require_tenant_feature_raises_when_tenant_missing() -> None:
    exec_ctx = ExecutionContext(deps=Deps())
    wrapped = RequireTenantFeature().wrap(_handler)

    with pytest.raises(HTTPException, match="Tenant context required"):
        await wrapped(_http_ctx(exec_ctx))


@pytest.mark.asyncio
async def test_require_tenant_feature_allows_bound_tenant() -> None:
    exec_ctx = ExecutionContext(deps=Deps())
    wrapped = RequireTenantFeature().wrap(_handler)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with exec_ctx.inv.bind(metadata=metadata, tenant=TenantIdentity(tenant_id=uuid4())):
        assert await wrapped(_http_ctx(exec_ctx)) == "ok"

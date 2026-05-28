"""Unit tests for authz operation-plan helpers."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzSpec, AuthzDecision
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze.application.hooks.authz import AuthzBeforeAuthorize, merge_query_filters
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit

class _AllowDecision:
    async def authorize(self, request):  # noqa: ANN001
        _ = request
        return AuthzDecision(allowed=True, matched_permission_key="x.read")

class _DenyDecision:
    async def authorize(self, request):  # noqa: ANN001
        _ = request
        return AuthzDecision(allowed=False, reason="denied")

def test_merge_query_filters_and() -> None:
    merged = merge_query_filters({"$values": {"a": 1}}, {"$values": {"b": 2}})
    assert merged == {"$and": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]}

@pytest.mark.asyncio
async def test_before_authorize_allows() -> None:
    ctx = ExecutionContext(deps=Deps())
    ident = AuthnIdentity(principal_id=uuid4())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with patch.object(ctx.authz, "decision", return_value=_AllowDecision()):
        with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)
            await hook(None)

@pytest.mark.asyncio
async def test_before_authorize_denies() -> None:
    ctx = ExecutionContext(deps=Deps())
    ident = AuthnIdentity(principal_id=uuid4())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with patch.object(ctx.authz, "decision", return_value=_DenyDecision()):
        with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)

            with pytest.raises(CoreException) as exc_info:
                await hook(None)
            assert exc_info.value.kind == ExceptionKind.AUTHORIZATION

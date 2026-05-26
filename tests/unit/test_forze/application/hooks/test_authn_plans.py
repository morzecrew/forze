"""Unit tests for authn operation-plan hooks."""

from __future__ import annotations

from forze.base.exceptions import CoreException
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze.application.hooks.authn import AuthnRequired

pytestmark = pytest.mark.unit

@pytest.mark.asyncio
async def test_authn_before_required_allows_when_bound() -> None:
    ctx = ExecutionContext(deps=Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=uuid4())

    with ctx.inv.bind(metadata=metadata, authn=ident):
        hook = AuthnRequired()(ctx)
        await hook(None)

@pytest.mark.asyncio
async def test_authn_before_required_denies_when_missing() -> None:
    ctx = ExecutionContext(deps=Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv.bind(metadata=metadata):
        hook = AuthnRequired()(ctx)

        with pytest.raises(CoreException) as exc_info:
            await hook(None)

    assert exc_info.value.code == "auth_required"

"""Indirect access: nested operations inherit selector-based authz patches."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzSpec
from forze.application.hooks.authz import authorize_before_step
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze.application.execution.registry import OperationRegistry
from forze.base.errors import AuthorizationError
from forze.base.primitives import str_key_selector

pytestmark = pytest.mark.unit


class _AllowRuntime:
    async def authorize(self, request):  # noqa: ANN001
        from forze.application.contracts.authz import AuthzDecision

        _ = request
        return AuthzDecision(allowed=True, matched_permission_key="child.read")


class _DenyRuntime:
    async def authorize(self, request):  # noqa: ANN001
        from forze.application.contracts.authz import AuthzDecision

        _ = request
        return AuthzDecision(allowed=False, reason="denied")


@pytest.mark.asyncio
async def test_patch_inherits_authz_before_on_child_operation() -> None:
    """Child operations under a namespace inherit the same BeforeStep from a patch."""

    async def _child_handler(_args):
        return "ok"

    reg = (
        OperationRegistry(
            handlers={
                "parent.dispatch": lambda _ctx: None,
                "child.read": lambda _ctx: _child_handler,
            },
        )
        .patch(str_key_selector.prefix("child."))
        .bind_outer()
        .before(
            authorize_before_step(
                step_id="authz_child",
                spec=AuthzSpec(name="main"),
                action="child.read",
                requires=(),
            ),
        )
        .finish(deep=True)
        .freeze()
    )

    ctx = ExecutionContext(deps=Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=uuid4())

    with patch.object(ctx.authz, "decision", return_value=_AllowRuntime()):
        with ctx.inv.bind(metadata=metadata, authn=ident):
            run = reg.resolve("child.read", ctx)
            assert await run(None) == "ok"


@pytest.mark.asyncio
async def test_patch_authz_denies_child_without_grant() -> None:
    async def _child_handler(_args):
        return "ok"

    reg = (
        OperationRegistry(handlers={"child.read": lambda _ctx: _child_handler})
        .patch(str_key_selector.exact("child.read"))
        .bind_outer()
        .before(
            authorize_before_step(
                step_id="authz_child",
                spec=AuthzSpec(name="main"),
                action="child.read",
                requires=(),
            ),
        )
        .finish(deep=True)
        .freeze()
    )

    ctx = ExecutionContext(deps=Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=uuid4())

    with patch.object(ctx.authz, "decision", return_value=_DenyRuntime()):
        with ctx.inv.bind(metadata=metadata, authn=ident):
            run = reg.resolve("child.read", ctx)

            with pytest.raises(AuthorizationError):
                await run(None)

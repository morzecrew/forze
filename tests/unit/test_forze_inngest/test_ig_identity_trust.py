"""The event's ``_forze`` envelope is untrusted, so a registered Inngest function does NOT bind
the claimed ``principal_id`` / ``tenant_id`` by default — only when ``bind_identity_from_event``
is opted in (trusted producers). Otherwise any event could impersonate any principal/tenant."""

from typing import Any
from uuid import uuid4

import inngest
import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_inngest import InngestClient, InngestFunctionBinding, register_functions
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit


class _In(BaseModel):
    value: str


class _Out(BaseModel):
    ok: bool = True


class _FakeEvent:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class _FakeContext:
    def __init__(self, data: dict[str, Any]) -> None:
        self.event = _FakeEvent(data)
        self.step = object()


_PRINCIPAL = uuid4()
_TENANT = uuid4()


def _event_with_identity() -> dict[str, Any]:
    return {
        "_forze": {"principal_id": str(_PRINCIPAL), "tenant_id": str(_TENANT)},
        "value": "ok",
    }


def _register(captured: dict[str, Any], *, bind_identity_from_event: bool) -> inngest.Function:
    def _factory(exec_ctx: ExecutionContext) -> Any:
        async def _h(_args: _In) -> _Out:
            # Read the identity bound for this invocation (inside _bind_invocation).
            captured["authn"] = exec_ctx.inv_ctx.get_authn()
            captured["tenant"] = exec_ctx.inv_ctx.get_tenant()
            return _Out()

        return _h

    client = InngestClient(app_id="forze-identity-test")
    spec = DurableFunctionSpec(
        name="on-x",
        run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
        triggers=(DurableFunctionEventTrigger(event="app/x"),),
    )
    fns = register_functions(
        client,
        [InngestFunctionBinding(spec=spec, handler_factory=_factory)],
        ctx_factory=lambda: context_from_deps(Deps.plain({})),
        bind_identity_from_event=bind_identity_from_event,
    )
    return fns[0]


async def test_event_identity_not_bound_by_default() -> None:
    captured: dict[str, Any] = {}
    fn = _register(captured, bind_identity_from_event=False)

    await fn._handler(_FakeContext(_event_with_identity()))  # pyright: ignore[reportPrivateUsage]

    # The event-supplied principal/tenant are untrusted → not bound.
    assert captured["authn"] is None
    assert captured["tenant"] is None


async def test_event_identity_bound_when_opted_in() -> None:
    captured: dict[str, Any] = {}
    fn = _register(captured, bind_identity_from_event=True)

    await fn._handler(_FakeContext(_event_with_identity()))  # pyright: ignore[reportPrivateUsage]

    # Opted in (trusted producers): the claimed identity is bound.
    assert captured["authn"] is not None and captured["authn"].principal_id == _PRINCIPAL
    assert captured["tenant"] is not None and captured["tenant"].tenant_id == _TENANT

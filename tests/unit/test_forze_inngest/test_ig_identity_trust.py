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


# ....................... #
# A malformed envelope. It is producer-controlled text: what the function does not bind must
# not stop it, and what it would bind must stop it for good rather than be retried forever.

_MALFORMED_IDENTITY = [
    {"principal_id": "not-a-uuid"},
    {"principal_id": str(_PRINCIPAL), "actor_ids": "not-a-list"},
    {"principal_id": str(_PRINCIPAL), "actor_ids": 5},
    {"principal_id": 5},
    {"principal_id": str(_PRINCIPAL), "actor_ids": ["not-a-uuid"]},
    {"principal_id": str(_PRINCIPAL), "tenant_id": "not-a-uuid"},
    {"principal_id": ""},
    {"principal_id": 0},
    {"tenant_id": ""},
    {"actor_ids": [str(uuid4())]},
]


@pytest.mark.parametrize("envelope", _MALFORMED_IDENTITY)
async def test_a_malformed_identity_is_ignored_when_not_bound(envelope: dict[str, Any]) -> None:
    captured: dict[str, Any] = {}
    fn = _register(captured, bind_identity_from_event=False)

    await fn._handler(_FakeContext({"_forze": envelope, "value": "ok"}))  # pyright: ignore[reportPrivateUsage]

    assert captured["authn"] is None
    assert captured["tenant"] is None


@pytest.mark.parametrize("envelope", _MALFORMED_IDENTITY)
async def test_a_malformed_identity_stops_a_binding_function_for_good(
    envelope: dict[str, Any],
) -> None:
    captured: dict[str, Any] = {}
    fn = _register(captured, bind_identity_from_event=True)

    with pytest.raises(inngest.NonRetriableError):
        await fn._handler(_FakeContext({"_forze": envelope, "value": "ok"}))  # pyright: ignore[reportPrivateUsage]

    assert captured == {}


async def test_malformed_tracing_ids_are_dropped() -> None:
    captured: dict[str, Any] = {}
    fn = _register(captured, bind_identity_from_event=True)
    envelope = {"execution_id": "nope", "principal_id": str(_PRINCIPAL)}

    await fn._handler(_FakeContext({"_forze": envelope, "value": "ok"}))  # pyright: ignore[reportPrivateUsage]

    assert captured["authn"].principal_id == _PRINCIPAL


def test_function_args_parse_past_a_malformed_envelope() -> None:
    from forze_inngest.adapters.context import parse_function_args

    parsed = parse_function_args(
        {"_forze": {"principal_id": "not-a-uuid"}, "value": "ok"}, args_type=_In
    )

    assert parsed.value == "ok"


def test_a_valid_tenant_survives_a_malformed_principal() -> None:
    # The tenant is decoded on its own: a sealed payload's AAD needs it even when the
    # function does not bind the envelope's identity.
    from forze_inngest.adapters.context import split_envelope

    decoded, _ = split_envelope(
        {"_forze": {"principal_id": "not-a-uuid", "tenant_id": str(_TENANT)}, "value": "ok"}
    )

    assert decoded.identity_malformed
    assert decoded.authn is None
    assert decoded.tenant is not None and decoded.tenant.tenant_id == _TENANT

"""A registered Inngest function maps a deterministic failure (a malformed event or a
non-retryable ``CoreException``) to ``inngest.NonRetriableError`` so Inngest stops retrying;
retryable kinds (infrastructure/throttled/concurrency) propagate for Inngest's own retry."""

from typing import Any

import inngest
import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, exc
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
    """Minimal stand-in for ``inngest.Context`` — only ``.event.data`` / ``.step`` are read."""

    def __init__(self, data: dict[str, Any]) -> None:
        self.event = _FakeEvent(data)
        self.step = object()


def _register(handler: Any) -> inngest.Function:
    client = InngestClient(app_id="forze-err-test")
    spec = DurableFunctionSpec(
        name="on-x",
        run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
        triggers=(DurableFunctionEventTrigger(event="app/x"),),
    )
    binding = InngestFunctionBinding(spec=spec, handler_factory=lambda _ctx: handler)
    fns = register_functions(
        client, [binding], ctx_factory=lambda: context_from_deps(Deps.plain({}))
    )
    return fns[0]


async def test_non_retryable_core_exception_becomes_non_retriable() -> None:
    async def _h(_args: _In) -> _Out:
        raise exc.validation("bad charge", code="charge.invalid")

    fn = _register(_h)
    with pytest.raises(inngest.NonRetriableError):
        await fn._handler(_FakeContext({"value": "ok"}))  # pyright: ignore[reportPrivateUsage]


async def test_retryable_core_exception_propagates_for_inngest_retry() -> None:
    async def _h(_args: _In) -> _Out:
        raise exc.infrastructure("downstream down", code="x.down")

    fn = _register(_h)
    with pytest.raises(CoreException) as ei:
        await fn._handler(_FakeContext({"value": "ok"}))  # pyright: ignore[reportPrivateUsage]
    assert not isinstance(ei.value, inngest.NonRetriableError)  # left retryable


async def test_invalid_event_payload_is_non_retriable() -> None:
    async def _h(_args: _In) -> _Out:
        return _Out()

    fn = _register(_h)
    with pytest.raises(inngest.NonRetriableError):
        # Missing required ``value`` -> pydantic ValidationError -> NonRetriableError.
        await fn._handler(_FakeContext({}))  # pyright: ignore[reportPrivateUsage]

"""Statement text on a captured trace: absent by default, present only by declaration.

Value capture is DST-only, but a DST bundle is still an artifact that gets stored and shared —
and a runtime-authored statement embeds the literals it was compiled with, which on a BI plane
means filter values and sometimes user input. So the statement follows the inference
``capture_inputs`` rule: masked unless the spec's author opts in.

Masked rather than dropped, deliberately: ``"<redacted>"`` lets a trace consumer tell "a
statement ran and its text was withheld" from "no statement was recorded", which are different
facts about a run.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import pytest

from forze.application.contracts.dynamic_read import DynamicReadDepKey, DynamicReadSpec
from forze.application.execution import Deps, DepsRegistry, ExecutionContext
from forze.application.execution.tracing.port_proxy import REDACTED
from forze.application.integrations.dynamic_read import DynamicReadRequest
from forze.base.primitives import JsonDict
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockDynamicReadAdapter, MockDynamicReadRegistry

pytestmark = pytest.mark.asyncio

ROUTE = "widgets"
STATEMENT = "SELECT revenue FROM gold WHERE customer_email = 'nadia@example.com'"


def _handler(request: DynamicReadRequest, state: MockState) -> Sequence[JsonDict]:
    _ = request, state
    return [{"revenue": 1}]


def _ctx(spec: DynamicReadSpec, mock_state: MockState) -> ExecutionContext:
    registry = MockDynamicReadRegistry().on(ROUTE, _handler)

    def factory(ctx: ExecutionContext, resolved: DynamicReadSpec) -> MockDynamicReadAdapter:
        _ = ctx
        return MockDynamicReadAdapter(
            state=mock_state,
            spec=resolved,
            registry=registry,
            statement_timeout=timedelta(seconds=5),
        )

    deps = (
        DepsRegistry.from_modules(lambda: MockDepsModule(state=mock_state)())
        .with_deps(Deps.routed({DynamicReadDepKey: {spec.name: factory}}))
        .with_tracing(runtime=True, capture_values=True)
        .freeze()
        .resolve()
    )
    return ExecutionContext(deps=deps)


def _captured_payloads(ctx: ExecutionContext) -> list[Any]:
    trace = ctx.deps.runtime_trace()

    assert trace is not None

    return [
        event.payload
        for event in trace.events
        if event.surface == DynamicReadDepKey.name and event.payload is not None
    ]


# ....................... #


async def test_statement_text_is_redacted_on_the_trace_by_default() -> None:
    """The text is recorded as a mask, so the run is visible and the literals are not."""

    state = MockState()
    ctx = _ctx(DynamicReadSpec(name=ROUTE), state)

    await ctx.dynamic_read.query(DynamicReadSpec(name=ROUTE)).run(STATEMENT)

    payloads = _captured_payloads(ctx)

    assert payloads, "the dynamic-read call should appear on the trace"
    assert payloads[0] == {"statement": REDACTED}
    assert "nadia@example.com" not in repr(payloads)


async def test_opting_in_captures_the_statement_verbatim() -> None:
    """``capture_statements=True`` is what a value-level invariant over statements needs."""

    state = MockState()
    spec = DynamicReadSpec(name=ROUTE, capture_statements=True)
    ctx = _ctx(spec, state)

    await ctx.dynamic_read.query(spec).run(STATEMENT)

    payloads = _captured_payloads(ctx)

    assert payloads
    assert payloads[0] == {"statement": STATEMENT}

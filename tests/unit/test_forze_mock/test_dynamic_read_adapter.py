"""Mock dynamic-read specifics: unprogrammed routes and handlers that lie about the engine.

The shared behaviour is covered by ``test_mock_dynamic_read_conformance``. What is left is the
mock's own contract with whoever programs it — because a handler that returns something no real
adapter could return turns the differential into two suites that agree about nothing.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import pytest

from forze.application.contracts.dynamic_read import DynamicReadSpec
from forze.application.integrations.dynamic_read import DynamicReadRequest
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import JsonDict
from forze_mock.adapters import (
    MockDynamicReadAdapter,
    MockDynamicReadRegistry,
    MockState,
)

pytestmark = pytest.mark.asyncio

ROUTE = "widgets"


def _adapter(registry: MockDynamicReadRegistry, **spec_kwargs: Any) -> MockDynamicReadAdapter:
    return MockDynamicReadAdapter(
        state=MockState(),
        spec=DynamicReadSpec(name=ROUTE, **spec_kwargs),
        registry=registry,
        statement_timeout=timedelta(seconds=5),
    )


# ....................... #


async def test_an_unprogrammed_route_says_so() -> None:
    """Silence would look like an empty result set, which is a real and different answer."""

    with pytest.raises(CoreException) as ei:
        await _adapter(MockDynamicReadRegistry()).run("SELECT 1")

    assert ei.value.code == "mock.dynamic_read.unprogrammed"
    assert ei.value.kind == ExceptionKind.CONFIGURATION


async def test_an_async_handler_is_awaited() -> None:
    """Handlers may be async, so a scripted delay or fault can be modelled."""

    async def handler(request: DynamicReadRequest, state: MockState) -> Sequence[JsonDict]:
        _ = request, state
        return [{"n": 1}]

    rows = await _adapter(MockDynamicReadRegistry().on(ROUTE, handler)).run("SELECT 1")

    assert rows == [{"n": 1}]


async def test_a_handler_may_script_a_refusal_the_mock_cannot_produce() -> None:
    """The engine-enforced taxonomy is reachable in simulation, by declaration.

    The mock cannot detect a write in a string — but a DST scenario still needs to exercise
    what a handler does when the write refusal arrives, so raising the taxonomy is supported and
    passes through untouched.
    """

    from forze.application.integrations.dynamic_read import WRITE_REFUSED_CODE, write_refused

    def handler(request: DynamicReadRequest, state: MockState) -> Sequence[JsonDict]:
        _ = request, state
        raise write_refused(ROUTE)

    with pytest.raises(CoreException) as ei:
        await _adapter(MockDynamicReadRegistry().on(ROUTE, handler)).run("INSERT INTO t ...")

    assert ei.value.code == WRITE_REFUSED_CODE


async def test_a_handler_returning_more_than_the_probe_is_a_handler_bug() -> None:
    """A real adapter stops at ``row_probe``; a handler that does not is not modelling one.

    Left unchecked it would make the mock's row-cap refusal fire on a row count no engine could
    have produced — the differential would still be green, and it would be comparing two
    different things.
    """

    def handler(request: DynamicReadRequest, state: MockState) -> Sequence[JsonDict]:
        _ = state
        return [{"n": index} for index in range(request.row_probe + 5)]

    with pytest.raises(CoreException) as ei:
        await _adapter(MockDynamicReadRegistry().on(ROUTE, handler), row_cap=3).run("SELECT 1")

    assert ei.value.kind == ExceptionKind.INTERNAL
    assert "row_probe" in ei.value.summary


@pytest.mark.parametrize(
    "returned",
    ["not-a-sequence", [("n", 1)], [None]],
    ids=["string", "tuples", "none-row"],
)
async def test_a_handler_returning_non_rows_is_refused(returned: Any) -> None:
    """Row mappings or nothing: the port's contract is ``Sequence[JsonDict]``."""

    def handler(request: DynamicReadRequest, state: MockState) -> Any:
        _ = request, state
        return returned

    with pytest.raises(CoreException) as ei:
        await _adapter(MockDynamicReadRegistry().on(ROUTE, handler)).run("SELECT 1")

    assert ei.value.kind == ExceptionKind.INTERNAL


async def test_the_handler_sees_the_governed_request() -> None:
    """Everything the shell resolved is visible, so a scenario can assert on it."""

    seen: list[DynamicReadRequest] = []

    def handler(request: DynamicReadRequest, state: MockState) -> Sequence[JsonDict]:
        _ = state
        seen.append(request)
        return []

    adapter = _adapter(MockDynamicReadRegistry().on(ROUTE, handler), row_cap=9)
    await adapter.run("SELECT %(a)s", {"a": 2}, options={"timeout": timedelta(seconds=1)})

    assert seen[-1].statement == "SELECT %(a)s"
    assert seen[-1].params == {"a": 2}
    assert seen[-1].row_cap == 9
    assert seen[-1].row_probe == 10
    assert seen[-1].timeout == timedelta(seconds=1)

"""Mock counter — the shared conformance battery.

The mock runs the same battery the real backends do, which is the whole point: it used to
count with unbounded Python integers and accept allocations no store can hold, so anything
proven against it about a counter's domain was proven against a fiction.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze_mock.adapters.counter import MockCounterAdapter
from forze_mock.state import MockState
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest.fixture
def harness() -> CounterHarness:
    run = uuid4().hex[:8]

    return CounterHarness(
        counter=MockCounterAdapter(state=MockState(), namespace="conformance"),
        suffix=lambda name: f"{name}-{run}",
    )


@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)

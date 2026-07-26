"""Firestore counter against the emulator — the shared conformance battery.

# covers: CounterPort.incr
# covers: CounterPort.incr_batch
# covers: CounterPort.decr
# covers: CounterPort.reset
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

from forze_firestore.adapters import FirestoreCounterAdapter
from forze_firestore.execution.deps.configs import FirestoreCounterConfig
from forze_firestore.kernel.client import FirestoreClient
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest_asyncio.fixture(scope="function")
async def harness(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> CounterHarness:
    run = uuid4().hex[:8]

    return CounterHarness(
        counter=FirestoreCounterAdapter(
            client=firestore_client,
            config=FirestoreCounterConfig(collection=("(default)", unique_collection)),
            route="conformance",
        ),
        suffix=lambda name: f"{name}-{run}",
    )


@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)

"""The in-memory high-water-mark store against the shared battery — the oracle's leg.

The mock is what DST and every unit test resolve, so its answers are the ones a simulation
believes. This leg is what makes "passed against the mock" mean "matches the stores a
deployment actually runs".
"""

from __future__ import annotations

import pytest

from forze.application.contracts.hlc import HlcCheckpointPort
from forze_mock import MockDepsModule
from forze_mock.adapters.hlc_checkpoint import MockHlcCheckpointAdapter
from forze_mock.state import MockState
from tests.support.execution_context import context_from_modules
from tests.support.hlc_checkpoint_conformance import (
    HLC_CHECKPOINT_BATTERY,
    Check,
    HlcCheckpointHarness,
)

# ----------------------- #

pytestmark = pytest.mark.asyncio


@pytest.fixture
def harness() -> HlcCheckpointHarness:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state, hlc_checkpoint=True))

    def store_for(node_key: str) -> HlcCheckpointPort:
        return MockHlcCheckpointAdapter(state=state, node_key=node_key)

    return HlcCheckpointHarness(
        store_for=store_for,
        # The journal transaction manager: an advance inside it is reverted by the undo
        # journal on rollback, which is how the mock models what a real one gets from the
        # database.
        transaction=lambda: ctx.tx_ctx.scope("default"),
        backend="mock",
    )


@pytest.mark.conformance(plane="hlc_checkpoint", engine="mock")
@pytest.mark.parametrize("check", HLC_CHECKPOINT_BATTERY, ids=lambda check: check.__name__)
async def test_hlc_checkpoint_battery(check: Check, harness: HlcCheckpointHarness) -> None:
    await check(harness)

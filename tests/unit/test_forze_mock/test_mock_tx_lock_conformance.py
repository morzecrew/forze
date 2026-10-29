"""Mock transaction-scoped lock — the shared conformance battery.

Journal transactions only: strict roots never run together, so no second transaction can even
open while the first holds its key, and the battery's contention would be a hang.
"""

from __future__ import annotations

import pytest

from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules
from tests.support.tx_lock_conformance import TX_LOCK_BATTERY, Check, TxLockHarness

# ----------------------- #


@pytest.fixture
def harness() -> TxLockHarness:
    # Two contexts over one state, as two sessions share one database.
    state = MockState()

    return TxLockHarness(
        first=context_from_modules(MockDepsModule(state=state)),
        second=context_from_modules(MockDepsModule(state=state)),
        route="tx",
        backend="mock",
    )


@pytest.mark.conformance(plane="tx_lock", engine="mock")
@pytest.mark.parametrize("check", TX_LOCK_BATTERY, ids=lambda check: check.__name__)
async def test_tx_lock_battery(check: Check, harness: TxLockHarness) -> None:
    await check(harness)

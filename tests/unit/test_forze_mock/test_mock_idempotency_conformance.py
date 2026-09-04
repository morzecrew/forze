"""The in-memory idempotency store against the shared battery."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

from forze_mock.adapters import MockIdempotencyAdapter, MockState
from tests.support.idempotency_conformance import (
    IDEMPOTENCY_BATTERY,
    Check,
    IdempotencyHarness,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
def harness() -> IdempotencyHarness:
    # One state for both stores: the TTL checks mint a short-window store that has to see
    # the same claims as the long-window one.
    state = MockState()

    def _store(ttl: timedelta) -> MockIdempotencyAdapter:
        return MockIdempotencyAdapter(state=state, namespace="idem", ttl=ttl)

    return IdempotencyHarness(
        # Far longer than any non-TTL check needs: a short window here would make the
        # battery race the clock instead of asserting the promises.
        store=_store(timedelta(hours=1)),
        backend="mock",
        key=lambda: f"battery-{uuid4().hex[:12]}",
        store_with_ttl=_store,
    )


@pytest.mark.conformance(plane="idempotency", engine="mock")
@pytest.mark.parametrize("check", IDEMPOTENCY_BATTERY, ids=lambda check: check.__name__)
async def test_idempotency_battery(check: Check, harness: IdempotencyHarness) -> None:
    await check(harness)

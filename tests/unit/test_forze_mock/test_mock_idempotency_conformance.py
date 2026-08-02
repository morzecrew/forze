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
    return IdempotencyHarness(
        store=MockIdempotencyAdapter(
            state=MockState(),
            namespace="idem",
            # Far longer than any check needs: expiry has its own dedicated suite, and a
            # short TTL here would make the battery race the clock instead of asserting
            # the promises.
            ttl=timedelta(hours=1),
        ),
        backend="mock",
        key=lambda: f"battery-{uuid4().hex[:12]}",
    )


@pytest.mark.conformance(plane="idempotency", engine="mock")
@pytest.mark.parametrize("check", IDEMPOTENCY_BATTERY, ids=lambda check: check.__name__)
async def test_idempotency_battery(check: Check, harness: IdempotencyHarness) -> None:
    await check(harness)

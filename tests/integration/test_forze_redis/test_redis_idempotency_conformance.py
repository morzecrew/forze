"""The Redis idempotency store against the shared battery.

Redis had no ``fail`` coverage of any kind, so neither half of the release promise — that it
frees the caller's own claim, and that it leaves everyone else's alone — was verified on the
store most likely to be wired for it.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze_redis.adapters import RedisIdempotencyAdapter
from tests.support.idempotency_conformance import (
    IDEMPOTENCY_BATTERY,
    Check,
    IdempotencyHarness,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
def harness(redis_idempotency: RedisIdempotencyAdapter) -> IdempotencyHarness:
    return IdempotencyHarness(
        store=redis_idempotency,
        backend="redis",
        key=lambda: f"battery-{uuid4().hex[:12]}",
    )


@pytest.mark.parametrize("check", IDEMPOTENCY_BATTERY, ids=lambda check: check.__name__)
async def test_idempotency_battery(check: Check, harness: IdempotencyHarness) -> None:
    await check(harness)

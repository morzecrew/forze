"""The Redis idempotency store against the shared battery.

Redis had no ``fail`` coverage of any kind, so neither half of the release promise — that it
frees the caller's own claim, and that it leaves everyone else's alone — was verified on the
store most likely to be wired for it.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import attrs
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
        # Same client and namespace, so the short-window store sees the same keys.
        store_with_ttl=lambda ttl: attrs.evolve(redis_idempotency, ttl=ttl),
        # Redis keeps the claim under a native key TTL and refuses anything below a
        # second, so this leg waits where the others take milliseconds.
        min_ttl=timedelta(seconds=1),
    )


@pytest.mark.conformance(plane="idempotency", engine="redis")
@pytest.mark.parametrize("check", IDEMPOTENCY_BATTERY, ids=lambda check: check.__name__)
async def test_idempotency_battery(check: Check, harness: IdempotencyHarness) -> None:
    await check(harness)

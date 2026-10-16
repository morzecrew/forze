"""The Redis revocation states against the shared battery, on a real server's clock."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest

from forze_redis.adapters.revocation_states import RedisRevocationStatesAdapter
from forze_redis.kernel.client import RedisClient
from tests.support.revocation_states_conformance import (
    STATES_BATTERY,
    StatesCheck,
    StatesHarness,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
def harness(redis_client: RedisClient) -> StatesHarness:
    async def wait(delta: timedelta) -> None:
        await asyncio.sleep(delta.total_seconds())

    return StatesHarness(
        backend="redis",
        states=RedisRevocationStatesAdapter(
            client=redis_client,
            namespace=f"it:revocation:{uuid4().hex[:12]}",
        ),
        wait=wait,
    )


@pytest.mark.conformance(plane="revocation_states", engine="redis")
@pytest.mark.parametrize("check", STATES_BATTERY, ids=lambda check: check.__name__)
async def test_revocation_states_battery(check: StatesCheck, harness: StatesHarness) -> None:
    await check(harness)

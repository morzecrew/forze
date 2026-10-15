"""The Redis fleet against the shared battery, on a real server's clock."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest

from forze.application.contracts.fleet import FleetPort, FleetSpec
from forze_redis.adapters import RedisFleetAdapter
from forze_redis.kernel.client import RedisClient
from tests.support.fleet_conformance import FLEET_BATTERY, Check, FleetHarness

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
def harness(redis_client: RedisClient) -> FleetHarness:
    namespace = f"it:fleet:{uuid4().hex[:12]}"

    def fleet(spec: FleetSpec) -> FleetPort:
        return RedisFleetAdapter(client=redis_client, namespace=namespace, spec=spec)

    async def wait(delta: timedelta) -> None:
        await asyncio.sleep(delta.total_seconds())

    return FleetHarness(backend="redis", fleet=fleet, wait=wait)


@pytest.mark.conformance(plane="fleet", engine="redis")
@pytest.mark.parametrize("check", FLEET_BATTERY, ids=lambda check: check.__name__)
async def test_fleet_battery(check: Check, harness: FleetHarness) -> None:
    await check(harness)

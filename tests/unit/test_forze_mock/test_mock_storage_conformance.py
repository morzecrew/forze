"""Mock storage — the shared conformance battery.

The mock is a parallel reimplementation of the storage adapter rather than a thin wrapper, so
it is the plane most able to drift. It runs the same battery the real object stores do.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze_mock.adapters.storage import MockStorageAdapter
from forze_mock.state import MockState
from tests.support.storage_conformance import STORAGE_BATTERY, Check, StorageHarness

# ----------------------- #


@pytest.fixture
def harness() -> StorageHarness:
    # One shared state across every bucket, so an absent container is genuinely absent
    # rather than merely unreachable from a separate store.
    state = MockState()
    adapter = MockStorageAdapter(state=state, bucket="files")
    run = uuid4().hex[:8]

    def _for_bucket(name: str) -> tuple[MockStorageAdapter, MockStorageAdapter]:
        port = MockStorageAdapter(state=state, bucket=name)

        return port, port

    return StorageHarness(
        cmd=adapter,
        query=adapter,
        key=lambda name: f"{name}-{run}",
        for_bucket=_for_bucket,
    )


@pytest.mark.conformance(plane="storage", engine="mock")
@pytest.mark.parametrize("check", STORAGE_BATTERY, ids=lambda check: check.__name__)
async def test_storage_battery(check: Check, harness: StorageHarness) -> None:
    await check(harness)

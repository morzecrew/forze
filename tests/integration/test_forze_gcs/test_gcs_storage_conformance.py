"""GCS storage against the emulator — the shared conformance battery.

GCS is the leg that surfaced the delete divergence: its API answers a missing key with 404
where S3 answers 204, so an idempotent cleanup path had to be made the adapter's job.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.storage import StorageSpec
from forze.testing import context_from_deps
from forze_gcs.execution.deps import GCSDepsModule, GCSStorageConfig
from tests.support.storage_conformance import STORAGE_BATTERY, Check, StorageHarness

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def harness(gcs_client, gcs_bucket) -> StorageHarness:
    ctx = context_from_deps(
        GCSDepsModule(
            client=gcs_client,
            storages={gcs_bucket: GCSStorageConfig(bucket=gcs_bucket)},
        )()
    )
    spec = StorageSpec(name=gcs_bucket)
    run = uuid4().hex[:8]

    return StorageHarness(
        cmd=ctx.storage.command(spec),
        query=ctx.storage.query(spec),
        key=lambda name: f"{name}-{run}",
    )


@pytest.mark.conformance(plane="storage", engine="gcs")
@pytest.mark.parametrize("check", STORAGE_BATTERY, ids=lambda check: check.__name__)
async def test_storage_battery(check: Check, harness: StorageHarness) -> None:
    await check(harness)

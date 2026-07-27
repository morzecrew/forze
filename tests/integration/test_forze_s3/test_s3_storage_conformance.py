"""S3 storage against live servers — the shared conformance battery.

Runs against both S3 implementations the suite wires (MinIO and floci), which is where three
of this battery's checks came from: the two servers disagreed with each other about a second
``abort_upload`` and about a same-key copy, and a single-server suite would have called the
plane consistent.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.storage import StorageSpec
from forze.testing import context_from_deps
from forze_s3.execution.deps import S3DepsModule, S3StorageConfig
from tests.support.storage_conformance import STORAGE_BATTERY, Check, StorageHarness

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def harness(s3_client, s3_bucket) -> StorageHarness:
    ctx = context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket)},
        )()
    )
    spec = StorageSpec(name=s3_bucket)
    run = uuid4().hex[:8]

    return StorageHarness(
        cmd=ctx.storage.command(spec),
        query=ctx.storage.query(spec),
        key=lambda name: f"{name}-{run}",
    )


@pytest.mark.parametrize("check", STORAGE_BATTERY, ids=lambda check: check.__name__)
async def test_storage_battery(check: Check, harness: StorageHarness) -> None:
    await check(harness)

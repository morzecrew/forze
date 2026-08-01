"""S3 storage against live servers — the shared conformance battery.

Runs against both S3 implementations the suite wires (MinIO and floci), which is where three
of this battery's checks came from: the two servers disagreed with each other about a second
``abort_upload`` and about a same-key copy, and a single-server suite would have called the
plane consistent.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.storage import StorageSpec
from forze.testing import context_from_deps
from forze_s3.execution.deps import S3DepsModule, S3StorageConfig
from tests.support.storage_conformance import STORAGE_BATTERY, Check, StorageHarness

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _ports(s3_client, bucket: str) -> tuple[Any, Any]:
    ctx = context_from_deps(
        S3DepsModule(client=s3_client, storages={bucket: S3StorageConfig(bucket=bucket)})()
    )
    spec = StorageSpec(name=bucket)

    return ctx.storage.query(spec), ctx.storage.command(spec)


@pytest_asyncio.fixture
async def harness(s3_client, s3_bucket) -> StorageHarness:
    query, command = _ports(s3_client, s3_bucket)
    run = uuid4().hex[:8]

    return StorageHarness(
        cmd=command,
        query=query,
        key=lambda name: f"{name}-{run}",
        for_bucket=lambda bucket: _ports(s3_client, bucket),
    )


@pytest.mark.conformance(plane="storage", engine="s3")
@pytest.mark.parametrize("check", STORAGE_BATTERY, ids=lambda check: check.__name__)
async def test_storage_battery(check: Check, harness: StorageHarness) -> None:
    await check(harness)

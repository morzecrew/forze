"""BigQuery platform client lifecycle and health."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze_bigquery.kernel.platform.client import BigQueryClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bigquery_health_without_initialize() -> None:
    client = BigQueryClient()
    msg, ok = await client.health()
    assert ok is False
    assert msg


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bigquery_close_without_initialize_is_noop() -> None:
    client = BigQueryClient()
    await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bigquery_initialize_is_idempotent(bigquery_emulator_host: str) -> None:
    _ = bigquery_emulator_host
    client = BigQueryClient()
    await client.initialize("test")
    await client.initialize("test")
    msg, ok = await client.health()
    assert ok is True
    assert msg == "ok"
    await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bigquery_query_before_initialize_raises() -> None:
    client = BigQueryClient()
    with pytest.raises(CoreException, match="not initialized"):
        await client.run_query("SELECT 1")

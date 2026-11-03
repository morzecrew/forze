"""Unit tests for :class:`~forze_bigquery.kernel.client.BigQueryClient`."""

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives.owned_temp_path import OwnedTempPath
from forze_bigquery.kernel.client.client import BigQueryClient, BigQueryConfig

# ----------------------- #

_SA_JSON = '{"type":"service_account","project_id":"p"}'


@pytest.mark.asyncio
async def test_close_unlinks_owned_service_file() -> None:
    credential_path = OwnedTempPath.materialize_text(_SA_JSON, prefix="forze-bq-test-")
    client = BigQueryClient()

    mock_session = MagicMock()
    mock_session.close = AsyncMock()

    with patch(
        "forze_bigquery.kernel.client.client.ClientSession",
        return_value=mock_session,
    ):
        await client.initialize(
            "test-project",
            service_file=credential_path.path,
            service_file_owned=credential_path.owned,
        )
        await client.close()

    assert credential_path.path is not None
    assert not Path(credential_path.path).exists()


@pytest.mark.asyncio
async def test_poll_job_done_returns_on_done_status() -> None:
    # Exercises the poll-attempt clamp and the happy-path completion.
    client = BigQueryClient()
    fake_job = MagicMock()
    fake_job.get_job = AsyncMock(return_value={"status": {"state": "DONE"}})

    with patch.object(BigQueryClient, "job", return_value=fake_job):
        poll = client._BigQueryClient__poll_job_done
        await poll("job-1", deadline=asyncio.get_running_loop().time() + 60)

    fake_job.get_job.assert_awaited_once()


@pytest.mark.asyncio
async def test_polling_stops_at_the_deadline_however_slow_each_poll_is() -> None:
    # Each poll is an RPC that takes 0.4 s. Counting polls (1 s / 50 ms = 20) would wait
    # about 9 s; the wall-time deadline stops it at 1 s.
    client = BigQueryClient()
    fake_job = MagicMock()

    async def _slow_rpc(**_kwargs: object) -> dict[str, object]:
        await asyncio.sleep(0.4)
        return {"status": {"state": "RUNNING"}}

    fake_job.get_job = _slow_rpc
    config = BigQueryConfig(poll_interval=timedelta(milliseconds=50))
    loop = asyncio.get_running_loop()
    started = loop.time()

    with (
        patch.object(BigQueryClient, "job", return_value=fake_job),
        patch.object(BigQueryClient, "_BigQueryClient__require_config", return_value=config),
        pytest.raises(CoreException, match="polling timed out"),
    ):
        await client._BigQueryClient__poll_job_done("job-1", deadline=started + 1.0)  # type: ignore[attr-defined]

    assert 0.9 < loop.time() - started < 1.3


@pytest.mark.asyncio
async def test_the_results_fetch_counts_against_the_query_timeout() -> None:
    # The query and the polling finish at once; the results call then hangs. The whole wait is
    # one clock, so the 1 s route ends at 1 s, not 1 s after the results call starts.
    client = BigQueryClient()
    fake_job = MagicMock()
    fake_job.query = AsyncMock(return_value={"jobComplete": False, "jobReference": {"jobId": "j"}})
    fake_job.get_job = AsyncMock(return_value={"status": {"state": "DONE"}})

    async def _hanging_results(**_kwargs: object) -> dict[str, object]:
        await asyncio.sleep(5)
        return {}

    fake_job.get_query_results = _hanging_results
    loop = asyncio.get_running_loop()
    started = loop.time()

    with (
        patch.object(BigQueryClient, "job", return_value=fake_job),
        patch.object(
            BigQueryClient, "_BigQueryClient__require_config", return_value=BigQueryConfig()
        ),
        pytest.raises(CoreException, match="timed out"),
    ):
        await client.run_query("SELECT 1", timeout=timedelta(seconds=1))

    assert 0.9 < loop.time() - started < 1.5


def _job_client(fake_job: MagicMock) -> tuple[BigQueryClient, Any]:
    client = BigQueryClient()
    patches = (
        patch.object(BigQueryClient, "job", return_value=fake_job),
        patch.object(
            BigQueryClient, "_BigQueryClient__require_config", return_value=BigQueryConfig()
        ),
    )
    return client, patches


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "message"),
    [
        ({"status": {"state": "DONE", "errors": [{"reason": "x"}]}}, "job failed"),
        ({"status": {"state": "FAILED"}}, "ended with state"),
    ],
)
async def test_a_job_that_ends_badly_is_refused(status: dict[str, Any], message: str) -> None:
    fake_job = MagicMock()
    fake_job.query = AsyncMock(return_value={"jobComplete": False, "jobReference": {"jobId": "j"}})
    fake_job.get_job = AsyncMock(return_value=status)
    client, (job_patch, config_patch) = _job_client(fake_job)

    with job_patch, config_patch, pytest.raises(CoreException, match=message):
        await client.run_query("SELECT 1", timeout=timedelta(seconds=5))


@pytest.mark.asyncio
async def test_an_incomplete_query_without_a_job_id_is_refused() -> None:
    fake_job = MagicMock()
    fake_job.query = AsyncMock(return_value={"jobComplete": False, "jobReference": {}})
    client, (job_patch, config_patch) = _job_client(fake_job)

    with job_patch, config_patch, pytest.raises(CoreException, match="did not return a job id"):
        await client.run_query("SELECT 1", timeout=timedelta(seconds=5))


@pytest.mark.asyncio
async def test_a_polled_job_returns_its_results() -> None:
    fake_job = MagicMock()
    fake_job.query = AsyncMock(return_value={"jobComplete": False, "jobReference": {"jobId": "j"}})
    fake_job.get_job = AsyncMock(return_value={"status": {"state": "DONE"}})
    fake_job.get_query_results = AsyncMock(
        return_value={
            "schema": {"fields": [{"name": "n", "type": "INTEGER", "mode": "NULLABLE"}]},
            "rows": [{"f": [{"v": "7"}]}],
            "totalRows": "1",
        }
    )
    client, (job_patch, config_patch) = _job_client(fake_job)

    with job_patch, config_patch:
        result = await client.run_query("SELECT 7 AS n", timeout=timedelta(seconds=5))

    assert result.rows == [{"n": 7}]

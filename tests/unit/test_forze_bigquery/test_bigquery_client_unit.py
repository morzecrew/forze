"""Unit tests for :class:`~forze_bigquery.kernel.client.BigQueryClient`."""

from pathlib import Path
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


@pytest.mark.parametrize("bad", [0, -1])
def test_config_rejects_non_positive_max_poll_attempts(bad: int) -> None:
    with pytest.raises(CoreException, match="max_poll_attempts must be >= 1"):
        BigQueryConfig(max_poll_attempts=bad)


@pytest.mark.asyncio
async def test_poll_job_done_returns_on_done_status() -> None:
    # Exercises the poll-attempt clamp and the happy-path completion.
    client = BigQueryClient()
    fake_job = MagicMock()
    fake_job.get_job = AsyncMock(return_value={"status": {"state": "DONE"}})

    with patch.object(BigQueryClient, "job", return_value=fake_job):
        poll = client._BigQueryClient__poll_job_done
        await poll("job-1", timeout=60)

    fake_job.get_job.assert_awaited_once()

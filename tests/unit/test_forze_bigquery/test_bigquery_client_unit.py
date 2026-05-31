"""Unit tests for :class:`~forze_bigquery.kernel.client.BigQueryClient`."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.base.primitives.gcp_service_file import materialize_service_account_json
from forze_bigquery.kernel.client.client import BigQueryClient

# ----------------------- #

_SA_JSON = '{"type":"service_account","project_id":"p"}'


@pytest.mark.asyncio
async def test_close_unlinks_owned_service_file() -> None:
    path, owned = materialize_service_account_json(_SA_JSON, prefix="forze-bq-test-")
    client = BigQueryClient()

    mock_session = MagicMock()
    mock_session.close = AsyncMock()

    with patch(
        "forze_bigquery.kernel.client.client.ClientSession",
        return_value=mock_session,
    ):
        await client.initialize(
            "test-project",
            service_file=path,
            service_file_owned=owned,
        )
        await client.close()

    assert not Path(path).exists()

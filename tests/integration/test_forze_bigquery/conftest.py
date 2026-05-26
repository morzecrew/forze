"""Pytest configuration for forze_bigquery integration tests."""

from __future__ import annotations

import os
import shutil
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("gcloud.aio.bigquery")
pytest.importorskip("testcontainers")

from gcloud.aio.bigquery import Dataset, Table
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from forze_bigquery.kernel.platform.client import BigQueryClient

BQ_EMULATOR_IMAGE = "ghcr.io/goccy/bigquery-emulator:latest"
BQ_EMULATOR_PORT = 9050
TEST_PROJECT_ID = "test"


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for BigQuery integration tests")


@pytest.fixture(scope="session")
def bigquery_emulator_host():
    """Start goccy/bigquery-emulator for integration tests."""

    _ensure_docker()

    container = (
        DockerContainer(image=BQ_EMULATOR_IMAGE)
        .with_command(["--project", TEST_PROJECT_ID, "--port", str(BQ_EMULATOR_PORT)])
        .with_exposed_ports(BQ_EMULATOR_PORT)
    )
    container.start()

    wait_for_logs(container, "REST server listening", timeout=90)

    host = container.get_container_host_ip()
    port = container.get_exposed_port(BQ_EMULATOR_PORT)
    emulator_url = f"http://{host}:{port}"

    os.environ["BIGQUERY_EMULATOR_HOST"] = emulator_url

    yield emulator_url

    container.stop()


@pytest_asyncio.fixture(scope="function")
async def bigquery_client(bigquery_emulator_host: str) -> BigQueryClient:
    """Initialized BigQuery client against the emulator."""

    _ = bigquery_emulator_host
    client = BigQueryClient()
    await client.initialize(TEST_PROJECT_ID)

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def analytics_dataset(bigquery_client: BigQueryClient) -> str:
    """Create dataset and table for analytics smoke tests."""

    dataset_id = "forze_analytics"
    table_id = f"events_{uuid4().hex[:12]}"
    project = TEST_PROJECT_ID

    bq_dataset = Dataset(
        dataset_name=dataset_id,
        project=project,
        session=bigquery_client.session,
        api_root=bigquery_client.api_root,
    )
    try:
        await bq_dataset.insert(
            {
                "datasetReference": {
                    "projectId": project,
                    "datasetId": dataset_id,
                }
            },
            timeout=30,
        )
    except Exception:
        pass

    bq_table = Table(
        dataset_name=dataset_id,
        table_name=table_id,
        project=project,
        session=bigquery_client.session,
        api_root=bigquery_client.api_root,
    )
    try:
        await bq_table.create(
            {
                "tableReference": {
                    "projectId": project,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "schema": {
                    "fields": [
                        {"name": "event", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "value", "type": "INTEGER", "mode": "NULLABLE"},
                    ]
                },
            },
            timeout=30,
        )
    except Exception:
        pass

    return dataset_id, table_id

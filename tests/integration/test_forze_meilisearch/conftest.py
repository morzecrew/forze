"""Pytest fixtures for Meilisearch integration tests."""

from __future__ import annotations

import shutil

import pytest
import pytest_asyncio

pytest.importorskip("meilisearch_python_sdk")

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from forze_meilisearch.kernel.platform import MeilisearchClient

_MEILI_IMAGE = "getmeili/meilisearch:v1.45.0"
_MEILI_MASTER_KEY = "masterKey"


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Meilisearch integration tests")


@pytest.fixture(scope="session")
def meilisearch_container() -> DockerContainer:
    _ensure_docker()

    container = (
        DockerContainer(_MEILI_IMAGE)
        .with_env("MEILI_MASTER_KEY", _MEILI_MASTER_KEY)
        .with_env("MEILI_ENV", "development")
        .with_bind_ports(7700, 7700)
    )
    container.start()

    try:
        wait_for_logs(container, "listening on", timeout=120)
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def meilisearch_url(meilisearch_container: DockerContainer) -> str:
    host = meilisearch_container.get_container_host_ip()
    port = meilisearch_container.get_exposed_port(7700)
    return f"http://{host}:{port}"


@pytest_asyncio.fixture
async def meilisearch_client(meilisearch_url: str) -> MeilisearchClient:
    client = MeilisearchClient()
    await client.initialize(meilisearch_url, _MEILI_MASTER_KEY)

    try:
        yield client
    finally:
        await client.aclose()

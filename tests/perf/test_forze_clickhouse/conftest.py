"""Perf test fixtures for ClickHouse (requires Docker)."""

from __future__ import annotations

import shutil
import time
import urllib.error
import urllib.request

import pytest
import pytest_asyncio

pytest.importorskip("clickhouse_connect")
pytest.importorskip("testcontainers")

from testcontainers.core.container import DockerContainer

from forze_clickhouse.kernel.client import ClickHouseClient, ClickHouseConfig

CH_IMAGE = "clickhouse/clickhouse-server:24.8"
CH_HTTP_PORT = 8123


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for ClickHouse perf tests")


def _wait_http_ready(host: str, port: int, *, timeout_sec: float = 120.0) -> None:
    url = f"http://{host}:{port}/ping"
    deadline = time.monotonic() + timeout_sec

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.read().strip() == b"Ok.":
                    return

        except (urllib.error.URLError, TimeoutError, ConnectionResetError, OSError):
            pass

        time.sleep(1)

    pytest.fail(f"ClickHouse did not become ready at {url}")


@pytest.fixture(scope="session")
def clickhouse_connection() -> ClickHouseConfig:
    _ensure_docker()

    container = (
        DockerContainer(image=CH_IMAGE)
        .with_exposed_ports(CH_HTTP_PORT)
        .with_env("CLICKHOUSE_USER", "default")
        .with_env("CLICKHOUSE_PASSWORD", "forze-test")
        .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
    )
    container.start()
    time.sleep(2)

    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(CH_HTTP_PORT))
    _wait_http_ready(host, port)

    connection = ClickHouseConfig(
        host=host,
        port=port,
        username="default",
        password="forze-test",
        database="default",
    )

    yield connection

    container.stop()


@pytest_asyncio.fixture(scope="function")
async def clickhouse_client(clickhouse_connection: ClickHouseConfig) -> ClickHouseClient:
    client = ClickHouseClient()
    await client.initialize(clickhouse_connection)

    yield client

    await client.close()

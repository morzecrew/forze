"""Helpers for Temporal ``server start-dev`` testcontainers."""

import time
import urllib.error
import urllib.request
from typing import TYPE_CHECKING

import attrs
import pytest
from docker import from_env
from docker.errors import DockerException

if TYPE_CHECKING:
    from testcontainers.core.container import DockerContainer

# Pinned for reproducibility; uses embedded SQLite (no Postgres sidecar).
TEMPORAL_DEV_IMAGE = "temporalio/temporal:latest"
TEMPORAL_GRPC_PORT = 7233
TEMPORAL_HTTP_PORT = 8233
TEMPORAL_READY_TIMEOUT_SEC = 120.0


@attrs.define(frozen=True, slots=True)
class TemporalDevTarget:
    """Connection target for a running Temporal dev server container."""

    host: str
    grpc_port: int
    http_port: int

    @property
    def grpc_address(self) -> str:
        return f"{self.host}:{self.grpc_port}"


def ensure_docker_available() -> None:
    client = None
    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required for Temporal dev server tests: {exc}")
    finally:
        if client is not None:
            client.close()


def wait_temporal_dev_ready(target: TemporalDevTarget, *, timeout_sec: float) -> None:
    """Poll the dev server HTTP API until the default namespace is available."""

    url = f"http://{target.host}:{target.http_port}/api/v1/namespaces"
    deadline = time.monotonic() + timeout_sec

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, ConnectionResetError, OSError):
            pass

        time.sleep(1)

    pytest.fail(f"Temporal dev server did not become ready at {url}")


def start_temporal_dev_container() -> tuple[DockerContainer, TemporalDevTarget]:
    """Start ``temporalio/temporal`` with ``server start-dev`` and return container + target."""

    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(TEMPORAL_DEV_IMAGE)
        .with_command(["server", "start-dev", "--ip", "0.0.0.0"])
        .with_exposed_ports(TEMPORAL_GRPC_PORT, TEMPORAL_HTTP_PORT)
    )
    container.start()

    target = TemporalDevTarget(
        host=container.get_container_host_ip(),
        grpc_port=int(container.get_exposed_port(TEMPORAL_GRPC_PORT)),
        http_port=int(container.get_exposed_port(TEMPORAL_HTTP_PORT)),
    )
    wait_temporal_dev_ready(target, timeout_sec=TEMPORAL_READY_TIMEOUT_SEC)
    return container, target

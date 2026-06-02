"""Helpers for the Inngest Dev Server testcontainer."""

from __future__ import annotations

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

# Pinned for reproducibility.
INNGEST_DEV_IMAGE = "inngest/inngest:latest"
INNGEST_DEV_PORT = 8288
INNGEST_READY_TIMEOUT_SEC = 120.0


@attrs.define(frozen=True, slots=True)
class InngestDevTarget:
    """Connection target for a running Inngest Dev Server container."""

    host: str
    port: int

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


def ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()

    except DockerException as exc:
        pytest.skip(f"Docker is required for Inngest integration tests: {exc}")

    finally:
        if client is not None:
            client.close()


def wait_inngest_dev_ready(target: InngestDevTarget, *, timeout_sec: float) -> None:
    """Poll the dev server HTTP UI until it responds."""

    url = f"{target.base_url}/"
    deadline = time.monotonic() + timeout_sec

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status < 500:
                    return

        except (urllib.error.URLError, TimeoutError, ConnectionResetError, OSError):
            pass

        time.sleep(1)

    pytest.fail(f"Inngest dev server did not become ready at {url}")


def start_inngest_dev_container() -> tuple[DockerContainer, InngestDevTarget]:
    """Start ``inngest/inngest`` in dev mode (manual app sync via PUT)."""

    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(INNGEST_DEV_IMAGE)
        .with_command(["inngest", "dev", "--no-discovery"])
        .with_exposed_ports(INNGEST_DEV_PORT)
        .with_kwargs(extra_hosts={"host.docker.internal": "host-gateway"})
    )
    container.start()

    # Use localhost for host-side HTTP; dev server reaches apps via host.docker.internal.
    target = InngestDevTarget(
        host="127.0.0.1",
        port=int(container.get_exposed_port(INNGEST_DEV_PORT)),
    )
    wait_inngest_dev_ready(target, timeout_sec=INNGEST_READY_TIMEOUT_SEC)
    return container, target

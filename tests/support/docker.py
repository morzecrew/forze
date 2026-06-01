"""Docker availability checks for integration tests."""

from __future__ import annotations

import pytest
from docker import from_env
from docker.errors import DockerException


def ensure_docker_available() -> None:
    """Skip the current test when Docker is not reachable."""

    client = None
    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required: {exc}")
    finally:
        if client is not None:
            client.close()

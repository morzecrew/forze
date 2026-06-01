"""Root pytest configuration for integration tests under ``tests/integration/``."""

from __future__ import annotations

import pytest

from tests.support.docker import ensure_docker_available

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(scope="session")
def docker_available() -> None:
    """Session-scoped check that Docker is reachable (for container fixtures)."""

    ensure_docker_available()

"""Pytest configuration for ``forze_inngest`` integration tests."""

from __future__ import annotations

import pytest

pytest.importorskip("inngest")
pytest.importorskip("fastapi")
pytest.importorskip("testcontainers")

from .inngest_dev_server import (
    InngestDevTarget,
    ensure_docker_available,
    start_inngest_dev_container,
)


@pytest.fixture(scope="session")
def inngest_dev_target() -> InngestDevTarget:
    """Session-scoped Inngest Dev Server (Docker)."""

    ensure_docker_available()
    container, target = start_inngest_dev_container()

    try:
        yield target

    finally:
        container.stop()


@pytest.fixture
def inngest_dev_env(
    inngest_dev_target: InngestDevTarget,
    monkeypatch: pytest.MonkeyPatch,
) -> InngestDevTarget:
    """Per-test env vars so the Inngest SDK talks to the dev server."""

    monkeypatch.setenv("INNGEST_DEV", "1")
    monkeypatch.setenv("INNGEST_BASE_URL", inngest_dev_target.base_url)
    monkeypatch.delenv("INNGEST_EVENT_KEY", raising=False)
    monkeypatch.delenv("INNGEST_SIGNING_KEY", raising=False)

    # ``INNGEST_SERVE_ORIGIN`` is set per app in :func:`start_forze_inngest_app`.
    monkeypatch.delenv("INNGEST_SERVE_ORIGIN", raising=False)

    return inngest_dev_target

"""Health contract tests for Meilisearch clients: ``(message, ok)``, never raise."""

import pytest

pytest.importorskip("meilisearch")

from typing import Any

from forze_meilisearch.kernel.client import MeilisearchClient

# ----------------------- #


class _Health:
    def __init__(self, status: str) -> None:
        self.status = status


class _Transport:
    """Stub for the inner ``meilisearch_python_sdk.AsyncClient``."""

    def __init__(
        self,
        *,
        status: str | None = None,
        error: Exception | None = None,
    ) -> None:
        self._status = status
        self._error = error

    async def health(self) -> Any:
        if self._error is not None:
            raise self._error

        return _Health(self._status or "")


def _client_with(transport: _Transport) -> MeilisearchClient:
    client = MeilisearchClient()
    # Inject the mocked transport (attrs slots, name-mangled private field).
    setattr(client, "_MeilisearchClient__client", transport)
    return client


# ....................... #


@pytest.mark.asyncio
async def test_health_available_returns_ok_tuple() -> None:
    client = _client_with(_Transport(status="Available"))

    msg, ok = await client.health()

    assert ok is True
    assert msg == "available"


@pytest.mark.asyncio
async def test_health_degraded_status_returns_false_with_status() -> None:
    client = _client_with(_Transport(status="unavailable"))

    msg, ok = await client.health()

    assert ok is False
    assert msg == "unavailable"


@pytest.mark.asyncio
async def test_health_connection_failure_returns_message_and_false() -> None:
    client = _client_with(_Transport(error=ConnectionError("connection refused")))

    msg, ok = await client.health()

    assert ok is False
    assert "connection refused" in msg


@pytest.mark.asyncio
async def test_health_not_initialized_returns_message_and_false() -> None:
    client = MeilisearchClient()

    msg, ok = await client.health()

    assert ok is False
    assert "not initialized" in msg

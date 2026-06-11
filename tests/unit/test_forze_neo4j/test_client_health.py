"""Health and not-initialized contract tests for :class:`Neo4jClient`."""

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_neo4j.kernel.client import Neo4jClient

# ----------------------- #


class _BoomDriver:
    """Stub driver whose connectivity check fails."""

    async def verify_connectivity(self) -> None:
        raise RuntimeError("boom-connect")


# ....................... #


@pytest.mark.asyncio
async def test_health_failure_carries_error_message() -> None:
    client = Neo4jClient()
    client._driver = _BoomDriver()  # type: ignore[assignment]

    msg, ok = await client.health()

    assert ok is False
    assert "boom-connect" in msg


@pytest.mark.asyncio
async def test_health_not_initialized_returns_message_not_raises() -> None:
    client = Neo4jClient()

    msg, ok = await client.health()

    assert ok is False
    assert "not initialized" in msg


@pytest.mark.asyncio
async def test_uninitialized_run_raises_internal_core_exception() -> None:
    client = Neo4jClient()

    with pytest.raises(CoreException, match="not initialized") as ei:
        await client.run("RETURN 1")

    assert ei.value.kind is ExceptionKind.INTERNAL

"""Unit tests for :class:`~forze_redis.adapters.dlock.RedisDistributedLockAdapter`."""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("redis")

from forze.application.contracts.dlock import AcquiredLock, DistributedLockSpec
from forze_redis.adapters import RedisDistributedLockAdapter


def _adapter(client: object) -> RedisDistributedLockAdapter:
    return RedisDistributedLockAdapter(
        client=client,  # type: ignore[arg-type]
        namespace="ns",
        spec=DistributedLockSpec(name="locks", ttl=timedelta(seconds=10)),
    )


@pytest.mark.asyncio
async def test_acquire_runs_script_and_returns_fencing_token() -> None:
    client = AsyncMock()
    client.run_script = AsyncMock(return_value="7")
    adapter = _adapter(client)

    acquired = await adapter.acquire("my-key", "owner-a")

    assert acquired == AcquiredLock(key="my-key", owner="owner-a", token=7)

    client.run_script.assert_awaited_once()
    script, keys, args = client.run_script.call_args[0]
    assert "SET" in script and "NX" in script and "INCR" in script
    assert keys[0] == "dlock:ns:my-key"
    assert keys[1] == "dlock:ns:my-key:fence"
    assert list(args) == ["owner-a", 10_000]


@pytest.mark.asyncio
async def test_acquire_contention_returns_none() -> None:
    client = AsyncMock()
    client.run_script = AsyncMock(return_value="0")
    adapter = _adapter(client)

    assert await adapter.acquire("my-key", "owner-b") is None


@pytest.mark.asyncio
async def test_release_invokes_script_and_interprets_truthy() -> None:
    client = AsyncMock()
    client.run_script = AsyncMock(return_value="1")
    adapter = _adapter(client)

    assert await adapter.release("k", "owner-a") is True

    client.run_script.assert_awaited_once()
    script, keys, args = client.run_script.call_args[0]
    assert "GET" in script and "DEL" in script
    assert keys[0] == "dlock:ns:k"
    assert list(args) == ["owner-a"]


@pytest.mark.asyncio
async def test_is_locked_delegates_to_exists() -> None:
    client = AsyncMock()
    client.exists = AsyncMock(return_value=True)
    adapter = _adapter(client)

    assert await adapter.is_locked("x") is True
    client.exists.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_owner_decodes_bytes() -> None:
    client = AsyncMock()
    client.get = AsyncMock(return_value=b"some-owner")
    adapter = _adapter(client)

    assert await adapter.get_owner("z") == "some-owner"


@pytest.mark.asyncio
async def test_reset_invokes_script() -> None:
    client = AsyncMock()
    client.run_script = AsyncMock(return_value="1")
    adapter = _adapter(client)

    assert await adapter.reset("k", "owner-a") is True

    _script, _keys, argv = client.run_script.call_args[0]
    assert argv[0] == "owner-a"
    assert argv[1] == 10_000

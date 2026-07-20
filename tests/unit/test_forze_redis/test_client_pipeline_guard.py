"""Pipeline-scope guard: value-returning methods raise, fire-and-forget methods queue.

Pipeline commands only produce results at ``execute()``. Before the guard,
value-returning methods invoked inside a bound pipeline scope coerced the
pipeline object itself into a return value (silent corruption). They must now
raise a ``precondition`` error with code ``redis_read_in_pipeline``, while
write-batching (fire-and-forget) methods keep queuing onto the pipeline.
"""

from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_redis.kernel.client import RedisClient
from forze_redis.kernel.scripts import MSET_BULK_SET

# ----------------------- #


@pytest.fixture
def redis_client() -> RedisClient:
    client = RedisClient()
    client._RedisClient__client = MagicMock()  # type: ignore[attr-defined]
    return client


@pytest.fixture
def recording_pipe(redis_client: RedisClient) -> AsyncMock:
    """Bind a fake pipeline into the client's context vars and return it."""

    pipe = AsyncMock()
    redis_client._RedisClient__ctx_pipe.set(pipe)  # type: ignore[attr-defined]
    redis_client._RedisClient__ctx_depth.set(1)  # type: ignore[attr-defined]
    return pipe


# ....................... #
# Value-returning methods raise inside a pipeline scope


_VALUE_RETURNING: dict[str, Callable[[RedisClient], Awaitable[Any]]] = {
    "get": lambda c: c.get("k"),
    "mget": lambda c: c.mget(["a", "b"]),
    "exists": lambda c: c.exists("k"),
    "pttl": lambda c: c.pttl("k"),
    "pttl_raw_ms": lambda c: c.pttl_raw_ms("k"),
    "run_script": lambda c: c.run_script("return 1", [], []),
    "incr": lambda c: c.incr("k"),
    "decr": lambda c: c.decr("k"),
    "reset": lambda c: c.reset("k", 0),
    "xadd": lambda c: c.xadd("s", {"f": "v"}),
    "xread": lambda c: c.xread({"s": "0"}),
    "xgroup_create": lambda c: c.xgroup_create("s", "g"),
    "xgroup_read": lambda c: c.xgroup_read("g", "consumer", {"s": ">"}),
    "xack": lambda c: c.xack("s", "g", ["1-1"]),
    "xautoclaim": lambda c: c.xautoclaim("s", "g", "consumer", min_idle_ms=1000),
    "xpending": lambda c: c.xpending("s", "g", count=10),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", sorted(_VALUE_RETURNING))
async def test_value_returning_method_raises_inside_pipeline(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
    method: str,
) -> None:
    with pytest.raises(CoreException) as exc_info:
        await _VALUE_RETURNING[method](redis_client)

    assert exc_info.value.kind is ExceptionKind.PRECONDITION
    assert exc_info.value.code == "redis_read_in_pipeline"
    assert "not available inside a pipeline scope" in exc_info.value.summary

    # Nothing was queued onto the pipeline.
    assert recording_pipe.method_calls == []


@pytest.mark.asyncio
async def test_mset_with_options_does_not_raise_inside_pipeline(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    """mset with EX routes around run_script: it queues the script via pipe.eval."""

    res = await redis_client.mset({"a": "1", "b": "2"}, ex=60)

    assert res is True
    recording_pipe.eval.assert_awaited_once()
    args = recording_pipe.eval.call_args[0]
    assert args[0] == MSET_BULK_SET
    assert args[1] == 2  # numkeys
    assert args[2:4] == ("a", "b")
    assert args[4] == "60"  # ex
    recording_pipe.mset.assert_not_called()


# ....................... #
# Fire-and-forget methods keep queuing inside a pipeline scope


@pytest.mark.asyncio
async def test_set_queues_and_reports_queued(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.set("k", "v", ex=10, nx=True)

    assert res is True
    recording_pipe.set.assert_awaited_once_with(
        "k", "v", ex=10, px=None, nx=True, xx=False
    )


@pytest.mark.asyncio
async def test_mset_plain_queues_native_mset(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.mset({"a": "1"})

    assert res is True
    recording_pipe.mset.assert_awaited_once_with({"a": "1"})


@pytest.mark.asyncio
async def test_delete_queues_and_returns_zero(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.delete("a", "b")

    assert res == 0
    recording_pipe.delete.assert_awaited_once_with("a", "b")


@pytest.mark.asyncio
async def test_unlink_queues_and_returns_zero(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.unlink("a")

    assert res == 0
    recording_pipe.unlink.assert_awaited_once_with("a")


@pytest.mark.asyncio
async def test_expire_queues_and_reports_queued(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.expire("k", 60)

    assert res is True
    recording_pipe.expire.assert_awaited_once_with("k", 60, gt=False)


@pytest.mark.asyncio
async def test_publish_queues_and_returns_zero(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.publish("chan", "msg")

    assert res == 0
    recording_pipe.publish.assert_awaited_once_with("chan", "msg")


@pytest.mark.asyncio
async def test_xdel_queues_and_returns_zero(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.xdel("s", ["1-1", "1-2"])

    assert res == 0
    recording_pipe.xdel.assert_awaited_once_with("s", "1-1", "1-2")


@pytest.mark.asyncio
async def test_xtrim_maxlen_queues_and_returns_zero(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.xtrim_maxlen("s", 100)

    assert res == 0
    recording_pipe.xtrim.assert_awaited_once_with(
        "s", maxlen=100, approximate=True, limit=None
    )


@pytest.mark.asyncio
async def test_xtrim_minid_queues_and_returns_zero(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    res = await redis_client.xtrim_minid("s", "1-0")

    assert res == 0
    recording_pipe.xtrim.assert_awaited_once_with(
        "s", minid="1-0", approximate=True, limit=None
    )


# ....................... #
# No-op short-circuits stay no-ops inside a pipeline scope


@pytest.mark.asyncio
async def test_empty_inputs_short_circuit_inside_pipeline(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    assert await redis_client.delete() == 0
    assert await redis_client.unlink() == 0
    assert await redis_client.xdel("s", []) == 0
    assert await redis_client.xack("s", "g", []) == 0
    assert await redis_client.mget([]) == []
    assert await redis_client.mset({}) is True

    assert recording_pipe.method_calls == []


# ....................... #
# Health path is unaffected by a bound pipeline scope


@pytest.mark.asyncio
async def test_health_works_inside_pipeline(
    redis_client: RedisClient,
    recording_pipe: AsyncMock,
) -> None:
    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.ping = AsyncMock(return_value=True)

    status, ok = await redis_client.health()

    assert (status, ok) == ("ok", True)
    inner.ping.assert_awaited_once()
    assert recording_pipe.method_calls == []


# ....................... #
# Outside a pipeline scope everything behaves normally


@pytest.mark.asyncio
async def test_value_returning_methods_work_outside_pipeline(
    redis_client: RedisClient,
) -> None:
    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.get = AsyncMock(return_value=b"v")
    inner.exists = AsyncMock(return_value=1)
    inner.incrby = AsyncMock(return_value=7)
    inner.pttl = AsyncMock(return_value=1234)

    assert await redis_client.get("k") == b"v"
    assert await redis_client.exists("k") is True
    assert await redis_client.incr("k") == 7
    assert await redis_client.pttl("k") == 1234


@pytest.mark.asyncio
async def test_fire_and_forget_methods_return_real_results_outside_pipeline(
    redis_client: RedisClient,
) -> None:
    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.set = AsyncMock(return_value=None)  # e.g. NX miss
    inner.delete = AsyncMock(return_value=2)
    inner.expire = AsyncMock(return_value=0)
    inner.publish = AsyncMock(return_value=3)

    assert await redis_client.set("k", "v", nx=True) is False
    assert await redis_client.delete("a", "b") == 2
    assert await redis_client.expire("k", 60) is False
    assert await redis_client.publish("chan", "msg") == 3


@pytest.mark.asyncio
async def test_guard_lifts_after_pipeline_scope_resets(
    redis_client: RedisClient,
) -> None:
    """Once the context vars are reset, value-returning methods work again."""

    pipe = AsyncMock()
    token_pipe = redis_client._RedisClient__ctx_pipe.set(pipe)  # type: ignore[attr-defined]
    token_depth = redis_client._RedisClient__ctx_depth.set(1)  # type: ignore[attr-defined]

    with pytest.raises(CoreException):
        await redis_client.get("k")

    redis_client._RedisClient__ctx_pipe.reset(token_pipe)  # type: ignore[attr-defined]
    redis_client._RedisClient__ctx_depth.reset(token_depth)  # type: ignore[attr-defined]

    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.get = AsyncMock(return_value=b"v")

    assert await redis_client.get("k") == b"v"

from uuid import uuid4

import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException
from forze_redis.kernel.client import RedisClient, RedisConfig


@pytest.mark.asyncio
async def test_basic_kv_methods(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:{uuid4()}"
    key_1 = f"{prefix}:k1"
    key_2 = f"{prefix}:k2"
    key_3 = f"{prefix}:k3"

    assert await redis_client.set(key_1, "v1")
    assert await redis_client.get(key_1) == b"v1"

    assert await redis_client.mset({key_2: "v2", key_3: "v3"})
    values = await redis_client.mget([key_1, key_2, key_3, f"{prefix}:missing"])
    assert values == [b"v1", b"v2", b"v3", None]

    assert await redis_client.delete(key_1, key_2) == 2
    assert await redis_client.unlink(key_3) == 1

    assert await redis_client.get(key_1) is None
    assert await redis_client.get(key_2) is None
    assert await redis_client.get(key_3) is None


@pytest.mark.asyncio
async def test_counter_methods(redis_client: RedisClient) -> None:
    key = f"it:redis-client:counter:{uuid4()}"

    assert await redis_client.incr(key) == 1
    assert await redis_client.incr(key, by=4) == 5
    assert await redis_client.decr(key, by=2) == 3
    assert await redis_client.reset(key, value=10) == 3
    assert await redis_client.get(key) == b"10"


@pytest.mark.asyncio
async def test_nested_pipeline_reuses_parent(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:pipeline:{uuid4()}"
    key_1 = f"{prefix}:k1"
    key_2 = f"{prefix}:k2"

    async with redis_client.pipeline(transaction=True):
        await redis_client.set(key_1, "v1")

        async with redis_client.pipeline(transaction=True):
            await redis_client.set(key_2, "v2")

    assert await redis_client.get(key_1) == b"v1"
    assert await redis_client.get(key_2) == b"v2"


@pytest.mark.asyncio
async def test_health_reports_ok(redis_client: RedisClient) -> None:
    """health returns success when the server responds to ping."""
    status, ok = await redis_client.health()
    assert status == "ok"
    assert ok is True


@pytest.mark.asyncio
async def test_set_with_expiry(redis_client: RedisClient) -> None:
    """set with ex stores a key that expires."""
    prefix = f"it:redis-client:ttl:{uuid4()}"
    key = f"{prefix}:k"

    assert await redis_client.set(key, "v", ex=1)
    assert await redis_client.get(key) == b"v"


@pytest.mark.asyncio
async def test_mset_with_ex_sets_all_keys(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:mset-ex:{uuid4()}"
    a, b = f"{prefix}:a", f"{prefix}:b"

    assert await redis_client.mset({a: "1", b: "2"}, ex=3600) is True
    assert await redis_client.get(a) == b"1"
    assert await redis_client.get(b) == b"2"
    assert await redis_client.pttl(a) is not None


@pytest.mark.asyncio
async def test_mset_nx_all_or_nothing(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:mset-nx:{uuid4()}"
    a, b, c = f"{prefix}:a", f"{prefix}:b", f"{prefix}:c"

    await redis_client.set(c, "exists", ex=3600)

    ok = await redis_client.mset({a: "na", b: "nb", c: "nc"}, ex=60, nx=True)
    assert ok is False
    assert await redis_client.get(a) is None
    assert await redis_client.get(b) is None
    assert await redis_client.get(c) == b"exists"

    assert await redis_client.mset({a: "x", b: "y"}, ex=60, nx=True) is True
    assert await redis_client.get(a) == b"x"
    assert await redis_client.get(b) == b"y"


@pytest.mark.asyncio
async def test_mset_nx_and_xx_together_raises(redis_client: RedisClient) -> None:
    """mset rejects nx and xx at the same time."""
    from forze.base.exceptions import CoreException

    prefix = f"it:redis-client:mset-nxxx:{uuid4()}"
    with pytest.raises(CoreException):
        await redis_client.mset({f"{prefix}:a": "1"}, ex=60, nx=True, xx=True)


@pytest.mark.asyncio
async def test_mset_empty_mapping_is_noop(redis_client: RedisClient) -> None:
    assert await redis_client.mset({}) is True


@pytest.mark.asyncio
async def test_empty_arg_methods_short_circuit(redis_client: RedisClient) -> None:
    """delete/unlink with no keys, mget with no keys, xdel/xack with no ids."""
    assert await redis_client.delete() == 0
    assert await redis_client.unlink() == 0
    assert await redis_client.mget([]) == []
    assert await redis_client.xdel(f"it:redis-client:nostream:{uuid4()}", []) == 0
    assert await redis_client.xack(f"it:redis-client:nostream:{uuid4()}", "g", []) == 0


@pytest.mark.asyncio
async def test_mget_chunks_large_key_sets(redis_client: RedisClient) -> None:
    """mget splits requests larger than the chunk size (2000) into batches."""
    prefix = f"it:redis-client:mget-chunk:{uuid4()}"
    total = 2500
    mapping = {f"{prefix}:{i}": str(i) for i in range(total)}

    assert await redis_client.mset(mapping) is True

    keys = list(mapping.keys()) + [f"{prefix}:missing"]
    values = await redis_client.mget(keys)

    assert len(values) == total + 1
    assert values[0] == b"0"
    assert values[total - 1] == str(total - 1).encode()
    assert values[-1] is None


@pytest.mark.asyncio
async def test_read_methods_inside_pipeline_raise(redis_client: RedisClient) -> None:
    """Reads inside a pipeline scope fail loud: results only materialize at execute()."""
    prefix = f"it:redis-client:pipe-read:{uuid4()}"
    key = f"{prefix}:k"

    await redis_client.set(key, "v")

    with pytest.raises(CoreException) as exc_info:
        async with redis_client.pipeline(transaction=True):
            await redis_client.get(key)

    assert exc_info.value.code == "redis_read_in_pipeline"

    with pytest.raises(CoreException) as exc_info:
        async with redis_client.pipeline(transaction=True):
            await redis_client.exists(key)

    assert exc_info.value.code == "redis_read_in_pipeline"

    # The client remains fully usable outside the scope afterwards.
    assert await redis_client.get(key) == b"v"


@pytest.mark.asyncio
async def test_read_inside_pipeline_aborts_queued_writes(
    redis_client: RedisClient,
) -> None:
    """A read raising inside the scope prevents execute(): queued writes are discarded."""
    prefix = f"it:redis-client:pipe-abort:{uuid4()}"
    key = f"{prefix}:k"

    with pytest.raises(CoreException):
        async with redis_client.pipeline(transaction=True):
            await redis_client.set(key, "v")
            await redis_client.get(key)

    assert await redis_client.get(key) is None


@pytest.mark.asyncio
async def test_pipeline_batches_writes_end_to_end(redis_client: RedisClient) -> None:
    """Fire-and-forget writes queue inside the scope and apply at execute()."""
    prefix = f"it:redis-client:pipe-write:{uuid4()}"
    key_set = f"{prefix}:set"
    key_mset_a = f"{prefix}:mset-a"
    key_mset_b = f"{prefix}:mset-b"
    key_expire = f"{prefix}:expire"
    key_delete = f"{prefix}:delete"

    await redis_client.set(key_expire, "v")
    await redis_client.set(key_delete, "v")

    async with redis_client.pipeline(transaction=True):
        # Placeholder return values while queued.
        assert await redis_client.set(key_set, "v1") is True
        assert await redis_client.mset({key_mset_a: "a", key_mset_b: "b"}, ex=3600)
        assert await redis_client.expire(key_expire, 3600) is True
        assert await redis_client.delete(key_delete) == 0

        # Nothing is applied until the scope exits.
        # (Reads must run outside the scope, so verify after exit.)

    assert await redis_client.get(key_set) == b"v1"
    assert await redis_client.get(key_mset_a) == b"a"
    assert await redis_client.get(key_mset_b) == b"b"
    assert await redis_client.pttl(key_mset_a) is not None
    assert await redis_client.pttl(key_expire) is not None
    assert await redis_client.get(key_delete) is None


@pytest.mark.asyncio
async def test_pttl_variants(redis_client: RedisClient) -> None:
    """pttl returns None for persistent/missing keys and a positive value with TTL."""
    prefix = f"it:redis-client:pttl:{uuid4()}"
    persistent = f"{prefix}:persistent"
    with_ttl = f"{prefix}:ttl"
    missing = f"{prefix}:missing"

    await redis_client.set(persistent, "v")
    await redis_client.set(with_ttl, "v", ex=3600)

    assert await redis_client.pttl(persistent) is None
    assert await redis_client.pttl(missing) is None
    raw = await redis_client.pttl(with_ttl)
    assert raw is not None and raw > 0

    assert await redis_client.pttl_raw_ms(persistent) == -1
    assert await redis_client.pttl_raw_ms(missing) == -2


@pytest.mark.asyncio
async def test_expire_sets_ttl(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:expire:{uuid4()}"
    key = f"{prefix}:k"

    await redis_client.set(key, "v")
    assert await redis_client.expire(key, 3600) is True
    assert await redis_client.pttl(key) is not None

    assert await redis_client.expire(f"{prefix}:missing", 10) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("script", "expected"),
    [
        # Lua ``true`` is converted to integer 1 by Redis.
        ("return true", "1"),
        ("return 42", "42"),
        ("return 'hello'", "hello"),
    ],
)
async def test_run_script_return_types(
    redis_client: RedisClient,
    script: str,
    expected: str,
) -> None:
    """run_script normalises Lua return values (int/string) to strings."""
    assert await redis_client.run_script(script, [], []) == expected


@pytest.mark.asyncio
async def test_run_script_inside_pipeline_raises(redis_client: RedisClient) -> None:
    """run_script is value-returning: inside a pipeline scope it fails loud."""
    prefix = f"it:redis-client:script-pipe:{uuid4()}"
    key = f"{prefix}:k"

    with pytest.raises(CoreException) as exc_info:
        async with redis_client.pipeline(transaction=True):
            await redis_client.run_script(
                "redis.call('SET', KEYS[1], ARGV[1]); return 1",
                [key],
                ["scripted"],
            )

    assert exc_info.value.code == "redis_read_in_pipeline"
    assert await redis_client.get(key) is None


@pytest.mark.asyncio
async def test_initialize_accepts_secret_str_dsn(redis_container) -> None:
    """initialize unwraps a SecretStr DSN before building the pool."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    dsn = SecretStr(f"redis://{host}:{port}/0")

    client = RedisClient()
    await client.initialize(dsn=dsn, config=RedisConfig(max_size=3))
    try:
        assert (await client.health())[1] is True
    finally:
        await client.close()

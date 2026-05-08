"""Integration tests for :class:`~forze_redis.kernel.platform.RoutedRedisClient`."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from datetime import timedelta
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

pytest.importorskip("redis")

from testcontainers.redis import RedisContainer

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import InfrastructureError, SecretNotFoundError

from forze_redis.kernel.platform import RoutedRedisClient
from forze_redis.kernel.platform.client import RedisClient, RedisConfig


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/redis")


def _dsn(redis_container: RedisContainer, db: int) -> str:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/{db}"


class _MemSecrets:
    def __init__(
        self,
        dsns: dict[UUID, str],
        *,
        missing_tenant: UUID | None = None,
        broken_tenant: UUID | None = None,
    ) -> None:
        self._dsns = dsns
        self._missing_tenant = missing_tenant
        self._broken_tenant = broken_tenant

    async def resolve_str(self, ref: SecretRef) -> str:
        if self._broken_tenant is not None:
            tid = self._tid_for_ref(ref)
            if tid == self._broken_tenant:
                raise RuntimeError("vault unavailable")
        if self._missing_tenant is not None:
            tid = self._tid_for_ref(ref)
            if tid == self._missing_tenant:
                raise SecretNotFoundError(
                    f"No secret for {ref.path!r}",
                    details={"ref": ref.path},
                )
        for tid, dsn in self._dsns.items():
            if ref.path == f"tenants/{tid}/redis":
                return dsn
        raise SecretNotFoundError(
            f"No secret for {ref.path!r}",
            details={"ref": ref.path},
        )

    def _tid_for_ref(self, ref: SecretRef) -> UUID | None:
        prefix = "tenants/"
        suffix = "/redis"
        if not ref.path.startswith(prefix) or not ref.path.endswith(suffix):
            return None
        try:
            return UUID(ref.path[len(prefix) : -len(suffix)])
        except ValueError:
            return None

    async def exists(self, ref: SecretRef) -> bool:
        tid = self._tid_for_ref(ref)
        return tid is not None and tid in self._dsns


def _tenant_holder() -> tuple[Callable[[], UUID | None], Callable[[UUID | None], None]]:
    slot: list[UUID | None] = [None]

    def getter() -> UUID | None:
        return slot[0]

    def setter(value: UUID | None) -> None:
        slot[0] = value

    return getter, setter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_redis_health_pipeline_kv(redis_container: RedisContainer) -> None:
    t1 = uuid4()
    secrets = _MemSecrets({t1: _dsn(redis_container, 0)})
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=RedisConfig(max_size=5),
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        status, ok = await routed.health()
        assert status == "ok" and ok is True

        prefix = f"it:routed:{uuid4().hex[:10]}"
        key = f"{prefix}:k"
        async with routed.pipeline(transaction=True):
            await routed.set(key, "v")
        assert await routed.get(key) == b"v"
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_redis_port_delegators(redis_container: RedisContainer) -> None:
    """Exercise routed wrappers over :class:`RedisClient` command helpers."""

    t1 = uuid4()
    secrets = _MemSecrets({t1: _dsn(redis_container, 0)})
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=RedisConfig(max_size=5),
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        p = f"it:routed-all:{uuid4().hex[:12]}"

        async with routed.pipeline(transaction=False):
            await routed.set(f"{p}:pipe", "1")

        assert await routed.get(f"{p}:pipe") == b"1"

        await routed.set(f"{p}:ttl", "x", ex=60)
        assert await routed.exists(f"{p}:ttl") is True
        assert await routed.pttl(f"{p}:ttl") is not None

        await routed.set(f"{p}:nx", "new", nx=True)
        assert await routed.set(f"{p}:nx", "no", nx=True) is False
        await routed.set(f"{p}:nx", "yes", xx=True)

        assert await routed.mset({})
        assert await routed.mset({f"{p}:m1": "a", f"{p}:m2": "b"}, ex=120)

        vals = await routed.mget([f"{p}:m1", f"{p}:missing"])
        assert vals[0] == b"a" and vals[1] is None

        script_echo = "return ARGV[1]"
        echo = await routed.run_script(script_echo, [], ["ping"])
        assert echo == "ping" or echo == b"ping"

        await routed.incr(f"{p}:ctr")
        assert await routed.incr(f"{p}:ctr", by=4) == 5
        assert await routed.decr(f"{p}:ctr", by=2) == 3
        prev = await routed.reset(f"{p}:ctr", 100)
        assert prev == 3
        assert await routed.get(f"{p}:ctr") == b"100"

        topic = f"{p}:chan"
        gen = routed.subscribe([topic], timeout=timedelta(seconds=4))
        recv_task = asyncio.create_task(anext(gen))
        await asyncio.sleep(0.05)
        subs = await routed.publish(topic, b"hi")
        assert isinstance(subs, int)
        try:
            ch, payload = await asyncio.wait_for(recv_task, timeout=3)
            assert ch == topic and payload == b"hi"
        finally:
            if not recv_task.done():
                recv_task.cancel()
                with suppress(asyncio.CancelledError):
                    await recv_task
            await gen.aclose()

        stream = f"{p}:stream"
        grp = f"{p}:grp"
        cons = f"{p}:cons"
        assert await routed.xgroup_create(stream, grp, id="0", mkstream=True) is True
        mid_a = await routed.xadd(stream, {"seq": "a"})
        mid_b = await routed.xadd(stream, {"seq": "b"})
        batch = await routed.xgroup_read(
            grp,
            cons,
            {stream: ">"},
            count=10,
            block_ms=2000,
        )
        assert len(batch) >= 1
        read_ids: list[str] = []
        for _sn, entries in batch:
            for eid, _fields in entries:
                read_ids.append(eid)
        assert {mid_a, mid_b}.issubset(read_ids)
        assert await routed.xack(stream, grp, read_ids) >= 1

        trim_stream = f"{p}:trim"
        await routed.xadd(trim_stream, {"x": "1"})
        await routed.xadd(trim_stream, {"x": "2"})
        assert await routed.xtrim_maxlen(trim_stream, 1, approx=False, limit=None) >= 0
        mid_for_min = await routed.xadd(trim_stream, {"x": "3"})
        assert await routed.xtrim_minid(trim_stream, mid_for_min, approx=True) >= 0

        drop_stream = f"{p}:xdel"
        doomed = await routed.xadd(drop_stream, {"d": "1"})
        assert await routed.xdel(drop_stream, [doomed]) >= 1

        async for _msg in routed.subscribe([], timeout=timedelta(seconds=1)):
            raise AssertionError("empty subscribe must not yield")

        await routed.delete(f"{p}:pipe", f"{p}:ttl")
        assert await routed.unlink(f"{p}:nx") >= 1

        await routed.set(f"{p}:exp", "z")
        assert await routed.expire(f"{p}:exp", 3600) is True
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_redis_lru_different_db_indexes(redis_container: RedisContainer) -> None:
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _MemSecrets(
        {
            t1: _dsn(redis_container, 0),
            t2: _dsn(redis_container, 1),
            t3: _dsn(redis_container, 2),
        }
    )
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=RedisConfig(max_size=5),
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = RedisClient.close

    async def counting_close(self: RedisClient) -> None:
        closes.append(1)
        await real_close(self)

    prefix = f"it:routed-lru:{uuid4().hex[:10]}"
    key = f"{prefix}:mark"

    try:
        with patch.object(RedisClient, "close", counting_close):
            tenant_set(t1)
            await routed.set(key, "a")
            tenant_set(t2)
            await routed.set(key, "b")
            tenant_set(t1)
            await routed.exists(key)
            tenant_set(t3)
            await routed.set(key, "c")
            assert sum(closes) == 1

        tenant_set(t1)
        assert await routed.get(key) == b"a"
        tenant_set(t2)
        assert await routed.get(key) == b"b"
        tenant_set(t3)
        assert await routed.get(key) == b"c"
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_redis_key_isolation_across_db_indexes(
    redis_container: RedisContainer,
) -> None:
    ta, tb = uuid4(), uuid4()
    secrets = _MemSecrets(
        {
            ta: _dsn(redis_container, 0),
            tb: _dsn(redis_container, 1),
        }
    )
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=RedisConfig(max_size=5),
        max_cached_tenants=4,
    )
    await routed.startup()
    try:
        key = f"it:routed-iso:{uuid4().hex[:10]}:same"
        tenant_set(ta)
        await routed.set(key, "only-a")
        tenant_set(tb)
        assert await routed.get(key) is None
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_redis_stream_touchpoint(redis_container: RedisContainer) -> None:
    t1 = uuid4()
    secrets = _MemSecrets({t1: _dsn(redis_container, 0)})
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=RedisConfig(max_size=5),
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        stream = f"it:routed-s:{uuid4().hex[:10]}"
        msg_id = await routed.xadd(stream, {"event": "hello"})
        assert msg_id
        resp = await routed.xread({stream: "0"}, count=10)
        assert len(resp) >= 1
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_redis_secret_errors(redis_container: RedisContainer) -> None:
    uri = _dsn(redis_container, 0)
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()
    secrets_miss = _MemSecrets({t_ok: uri}, missing_tenant=t_miss)
    tenant_get, tenant_set = _tenant_holder()
    routed_miss = RoutedRedisClient(
        secrets=secrets_miss,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await routed_miss.startup()
    try:
        tenant_set(t_miss)
        with pytest.raises(SecretNotFoundError):
            await routed_miss.health()
    finally:
        await routed_miss.close()

    secrets_break = _MemSecrets({t_ok: uri}, broken_tenant=t_break)
    routed_break = RoutedRedisClient(
        secrets=secrets_break,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await routed_break.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(InfrastructureError, match="Failed to resolve Redis secret"):
            await routed_break.health()
    finally:
        await routed_break.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_redis_evict_and_close(redis_container: RedisContainer) -> None:
    t1 = uuid4()
    secrets = _MemSecrets({t1: _dsn(redis_container, 0)})
    tenant_get, tenant_set = _tenant_holder()
    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        await routed.health()
        await routed.evict_tenant(t1)
        await routed.evict_tenant(uuid4())
        assert (await routed.health())[1] is True
        await routed.close()
        await routed.startup()
        assert (await routed.health())[1] is True
    finally:
        await routed.close()

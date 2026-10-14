"""Redis fleet membership: what only a real server shows — its clock, its limits, its keys."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import attrs
import pytest
import pytest_asyncio
from redis.asyncio import Redis
from testcontainers.redis import RedisContainer

from forze.application.contracts.fleet import FleetSpec, FleetView
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.primitives import bind_time_source
from forze_redis import RedisDepsModule, RedisFleetConfig
from forze_redis.adapters import RedisFleetAdapter
from forze_redis.kernel.client import RedisClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

_DAY = timedelta(days=1)


@pytest_asyncio.fixture
async def raw(redis_container: RedisContainer) -> AsyncIterator[Redis]:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = Redis.from_url(f"redis://{host}:{port}/0")

    yield client

    await client.aclose()


@pytest.fixture
def namespace() -> str:
    return f"it:fleet:{uuid4().hex[:12]}"


def _spec(**kwargs: object) -> FleetSpec:
    return FleetSpec(name=f"f{uuid4().hex[:8]}", **kwargs)  # type: ignore[arg-type]


def _keys(namespace: str, spec: FleetSpec, *, prefix: str = "") -> tuple[str, str]:
    tag = f"{{{prefix}fleet:{namespace}:{spec.name}}}"

    return f"{tag}:z", f"{tag}:h"


async def _server_now(raw: Redis) -> datetime:
    seconds, micros = await raw.time()

    return datetime.fromtimestamp(seconds + micros / 1e6, tz=UTC)


@attrs.define
class _SkewedClock:
    """A local clock off by *skew* — wall time, monotonic time, and ``time.time``."""

    skew: timedelta

    def now(self) -> datetime:
        return datetime.now(UTC) + self.skew

    def uuid(self) -> UUID:
        return uuid4()

    def monotonic(self) -> float:
        return time.monotonic() + self.skew.total_seconds()


# ....................... #


class TestServerClock:
    @pytest.mark.parametrize("skew", [_DAY, -_DAY])
    async def test_a_skewed_local_clock_sees_the_same_fleet(
        self,
        redis_client: RedisClient,
        raw: Redis,
        namespace: str,
        monkeypatch: pytest.MonkeyPatch,
        skew: timedelta,
    ) -> None:
        spec = _spec(ttl=timedelta(seconds=30))
        fleet = RedisFleetAdapter(client=redis_client, namespace=namespace, spec=spec)
        await fleet.beat("honest", "serving", {})

        real_time = time.time
        monkeypatch.setattr(time, "time", lambda: real_time() + skew.total_seconds())

        with bind_time_source(_SkewedClock(skew=skew)):
            beat = await fleet.beat("skewed", "serving", {})
            read = await fleet.view()

        server_now = await _server_now(raw)

        # A client deciding expiry from its own clock would have written "skewed" a day off:
        # already expired, or alive for a day.
        for view in (beat, read):
            assert [m.id for m in view.members] == ["honest", "skewed"]
            assert abs(view.at - server_now) < timedelta(seconds=5)

        z, _ = _keys(namespace, spec)
        expiry_ms = await raw.zscore(z, "skewed")
        expires = datetime.fromtimestamp(expiry_ms / 1000, tz=UTC)

        assert abs(expires - (server_now + spec.ttl)) < timedelta(seconds=5)


class TestScale:
    async def test_five_hundred_members_parse(
        self, redis_client: RedisClient, namespace: str
    ) -> None:
        fleet = RedisFleetAdapter(client=redis_client, namespace=namespace, spec=_spec())
        ids = [f"replica-{i:03d}" for i in range(500)]

        for n, member_id in enumerate(ids):  # one at a time: the test pool holds five
            await fleet.beat(member_id, "serving", {"load": n, "zone": "eu-west-1a"})

        view = await fleet.beat("replica-000", "serving", {"load": 0, "zone": "eu-west-1a"})

        assert [m.id for m in view.members] == ids
        assert [m.report["load"] for m in view.members] == list(range(500))

    async def test_thousands_of_expired_members_are_removed_in_one_beat(
        self, redis_client: RedisClient, raw: Redis, namespace: str
    ) -> None:
        # Past Lua's unpack limit (about 8000 values), so removing them in one call fails.
        spec = _spec()
        z, h = _keys(namespace, spec)
        stale = dict.fromkeys((f"gone-{i}" for i in range(10_000)), 1)

        await raw.zadd(z, stale)
        await raw.hset(h, mapping=dict.fromkeys(stale, "serving|1|{}"))

        fleet = RedisFleetAdapter(client=redis_client, namespace=namespace, spec=spec)
        view = await fleet.beat("alive", "serving", {})

        assert [m.id for m in view.members] == ["alive"]
        assert await raw.zcard(z) == 1
        assert await raw.hlen(h) == 1


class TestKeys:
    async def test_a_linger_beat_does_not_shorten_the_keys(
        self, redis_client: RedisClient, raw: Redis, namespace: str
    ) -> None:
        spec = _spec(ttl=timedelta(seconds=60))
        fleet = RedisFleetAdapter(client=redis_client, namespace=namespace, spec=spec)

        await fleet.beat("stays", "serving", {})
        await fleet.beat("lingers", "draining", {}, ttl=timedelta(milliseconds=200))

        for key in _keys(namespace, spec):
            assert await raw.pttl(key) > 55_000, key

    async def test_both_keys_expire_after_the_last_member(
        self, redis_client: RedisClient, raw: Redis, namespace: str
    ) -> None:
        spec = _spec(ttl=timedelta(seconds=60))
        fleet = RedisFleetAdapter(client=redis_client, namespace=namespace, spec=spec)

        await fleet.beat("a", "serving", {}, ttl=timedelta(seconds=2))

        for key in _keys(namespace, spec):
            assert 2_000 < await raw.pttl(key) <= 3_000, key

    async def test_view_writes_nothing(
        self, redis_client: RedisClient, raw: Redis, namespace: str
    ) -> None:
        spec = _spec()
        z, h = _keys(namespace, spec)
        fleet = RedisFleetAdapter(client=redis_client, namespace=namespace, spec=spec)

        await fleet.beat("brief", "serving", {}, ttl=timedelta(milliseconds=100))
        await asyncio.sleep(0.3)

        assert (await fleet.view()).members == ()
        assert (await raw.zcard(z), await raw.hlen(h)) == (1, 1)

    async def test_a_tenant_aware_route_keeps_tenants_apart(
        self, redis_client: RedisClient, raw: Redis, namespace: str
    ) -> None:
        spec = _spec()
        tenant: list[TenantIdentity | None] = [None]
        fleet = RedisFleetAdapter(
            client=redis_client,
            namespace=namespace,
            spec=spec,
            tenant_aware=True,
            tenant_provider=lambda: tenant[0],
        )
        one, two = uuid4(), uuid4()

        tenant[0] = TenantIdentity(tenant_id=one)
        await fleet.beat("a", "serving", {})
        tenant[0] = TenantIdentity(tenant_id=two)
        view = await fleet.beat("b", "serving", {})

        assert [m.id for m in view.members] == ["b"]
        z, _ = _keys(namespace, spec, prefix=f"tenant:{one}:")
        assert await raw.zscore(z, "a") is not None


class TestForeignEntries:
    async def test_an_entry_this_release_cannot_read_is_skipped(
        self, redis_client: RedisClient, raw: Redis, namespace: str
    ) -> None:
        spec = _spec()
        z, h = _keys(namespace, spec)
        fleet = RedisFleetAdapter(client=redis_client, namespace=namespace, spec=spec)
        await fleet.beat("known", "serving", {})

        far = int((await _server_now(raw) + timedelta(minutes=5)).timestamp() * 1000)
        await raw.zadd(z, {"newer": far, "garbled": far})
        await raw.hset(h, mapping={"newer": "paused|1|{}", "garbled": "serving|x"})

        view = await fleet.view()

        assert [m.id for m in view.members] == ["known"]


class TestWiring:
    async def test_the_module_resolves_the_fleet_port(
        self, redis_client: RedisClient, namespace: str
    ) -> None:
        spec = _spec()
        ctx = context_from_deps(
            RedisDepsModule(
                client=redis_client,
                fleets={str(spec.name): RedisFleetConfig(namespace=namespace)},
            )()
        )

        view: FleetView = await ctx.fleet(spec).beat("a", "starting", {"load": 1})

        assert isinstance(ctx.fleet(spec), RedisFleetAdapter)
        assert view.members[0].since == view.at

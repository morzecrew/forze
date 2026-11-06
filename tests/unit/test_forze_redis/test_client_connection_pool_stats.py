"""`RedisClient.connection_pool_stats` reads redis-py's public `get_connection_count()`.

A real redis-py pool, never connected: taking a connection with
`get_available_connection()` and handing it back with `release()` moves it between
redis-py's in-use and idle sets without a server, so the test pins the driver's labels
(`db.client.connection.state` = `used` / `idle`) as well as the mapping.
"""

import pytest

pytest.importorskip("redis")

from redis.asyncio.connection import BlockingConnectionPool, ConnectionPool

from forze.application.contracts.pools import (
    ConnectionPoolStats,
    SupportsConnectionPoolStats,
)
from forze_redis.kernel.client import RedisClient
from forze_redis.kernel.client.routed_client import RoutedRedisClient

pytestmark = pytest.mark.unit

# ----------------------- #


def _client(pool: ConnectionPool) -> RedisClient:
    client = RedisClient()
    client._RedisClient__pool = pool  # type: ignore[attr-defined]
    return client


# ....................... #


class TestConnectionPoolStats:
    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    async def test_counts_follow_checkout_and_release(
        self, pool_class: type[ConnectionPool]
    ) -> None:
        pool = pool_class(max_connections=3)
        client = _client(pool)

        assert client.connection_pool_stats() == ConnectionPoolStats(in_use=0, idle=0, max_size=3)

        first = pool.get_available_connection()
        second = pool.get_available_connection()

        assert client.connection_pool_stats() == ConnectionPoolStats(in_use=2, idle=0, max_size=3)

        await pool.release(first)

        assert client.connection_pool_stats() == ConnectionPoolStats(in_use=1, idle=1, max_size=3)

        await pool.release(second)

        assert client.connection_pool_stats() == ConnectionPoolStats(in_use=0, idle=2, max_size=3)

    def test_waiting_is_not_reported(self) -> None:
        stats = _client(BlockingConnectionPool(max_connections=1)).connection_pool_stats()

        assert stats is not None and stats.waiting is None

    def test_reports_nothing_without_an_open_pool(self) -> None:
        assert RedisClient().connection_pool_stats() is None

    def test_a_routed_client_does_not_report_one(self) -> None:
        """A routed client holds one pool per tenant; a sum would hide a saturated one."""

        assert issubclass(RedisClient, SupportsConnectionPoolStats)
        assert not issubclass(RoutedRedisClient, SupportsConnectionPoolStats)

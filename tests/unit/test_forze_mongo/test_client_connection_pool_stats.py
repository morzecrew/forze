"""`MongoClient.connection_pool_stats` counts pymongo's public pool events.

The listener is fed pymongo's own event objects, in the order a pool publishes them, so
the test pins the event attributes it reads as well as the counting.
"""

import threading

import pytest
from pymongo import monitoring

from forze.application.contracts.pools import (
    ConnectionPoolStats,
    SupportsConnectionPoolStats,
)
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.client.client import _PoolEvents
from forze_mongo.kernel.client.routed_client import RoutedMongoClient

pytestmark = pytest.mark.unit

PRIMARY = ("primary", 27017)
SECONDARY = ("secondary", 27017)

# ----------------------- #


def _open(events: _PoolEvents, address: tuple[str, int], conn_id: int) -> None:
    """A connection opened by the pool and left idle in it."""

    events.connection_created(monitoring.ConnectionCreatedEvent(address, conn_id))
    events.connection_ready(monitoring.ConnectionReadyEvent(address, conn_id, 0.0))


def _check_out(events: _PoolEvents, address: tuple[str, int], conn_id: int) -> None:
    events.connection_checked_out(monitoring.ConnectionCheckedOutEvent(address, conn_id, 0.0))


def _check_in(events: _PoolEvents, address: tuple[str, int], conn_id: int) -> None:
    events.connection_checked_in(monitoring.ConnectionCheckedInEvent(address, conn_id))


def _close(events: _PoolEvents, address: tuple[str, int], conn_id: int) -> None:
    events.connection_closed(monitoring.ConnectionClosedEvent(address, conn_id, "stale"))


def _stats(in_use: int, idle: int) -> ConnectionPoolStats:
    return ConnectionPoolStats(in_use=in_use, idle=idle, max_size=10)


# ....................... #


class TestPoolEvents:
    def test_a_connection_through_its_life(self) -> None:
        events = _PoolEvents(10)
        assert events.stats == _stats(0, 0)

        events.connection_created(monitoring.ConnectionCreatedEvent(PRIMARY, 1))
        assert events.stats == _stats(1, 0)  # being opened counts as in use

        events.connection_ready(monitoring.ConnectionReadyEvent(PRIMARY, 1, 0.0))
        assert events.stats == _stats(0, 1)

        _check_out(events, PRIMARY, 1)
        assert events.stats == _stats(1, 0)

        _check_in(events, PRIMARY, 1)
        assert events.stats == _stats(0, 1)

        _close(events, PRIMARY, 1)
        assert events.stats == _stats(0, 0)

    def test_a_connection_that_fails_to_open_is_gone(self) -> None:
        events = _PoolEvents(10)

        events.connection_created(monitoring.ConnectionCreatedEvent(PRIMARY, 1))
        _close(events, PRIMARY, 1)

        assert events.stats == _stats(0, 0)

    def test_reports_the_busiest_server_not_a_sum(self) -> None:
        """Idle secondaries would hide a saturated primary in a sum."""

        events = _PoolEvents(10)

        for conn_id in range(3):
            _open(events, PRIMARY, conn_id)

        for conn_id in range(2):
            _check_out(events, PRIMARY, conn_id)

        for conn_id in range(5):
            _open(events, SECONDARY, conn_id)

        assert events.stats == _stats(2, 1)

    def test_a_closed_pool_drops_its_connections(self) -> None:
        """A connection checked out and never returned goes with its server's pool."""

        events = _PoolEvents(10)
        _open(events, PRIMARY, 1)
        _check_out(events, PRIMARY, 1)
        _open(events, SECONDARY, 1)

        events.pool_closed(monitoring.PoolClosedEvent(PRIMARY))

        assert events.stats == _stats(0, 1)

        # A late check-in from the closed pool does not resurrect it as in use.
        _check_in(events, PRIMARY, 1)
        _close(events, PRIMARY, 1)

        assert events.stats == _stats(0, 1)

    def test_events_from_several_threads_keep_the_counts(self) -> None:
        events = _PoolEvents(10)
        start = threading.Barrier(4)

        def churn(worker: int) -> None:
            start.wait()

            for i in range(500):
                conn_id = worker * 1000 + i
                _open(events, PRIMARY, conn_id)
                _check_out(events, PRIMARY, conn_id)
                _check_in(events, PRIMARY, conn_id)
                _close(events, PRIMARY, conn_id)

        threads = [threading.Thread(target=churn, args=(w,)) for w in range(4)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        assert events.stats == _stats(0, 0)


# ....................... #


class TestMongoClientConnectionPoolStats:
    def test_reports_nothing_before_initialize(self) -> None:
        assert MongoClient().connection_pool_stats() is None

    def test_a_routed_client_does_not_report_one(self) -> None:
        """A routed client holds one client per tenant; a sum would hide a saturated one."""

        assert issubclass(MongoClient, SupportsConnectionPoolStats)
        assert not issubclass(RoutedMongoClient, SupportsConnectionPoolStats)

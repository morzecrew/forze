"""`instrument_connection_pools` exports each client's pool as OTel gauges."""

from __future__ import annotations

from typing import Any

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.pools import ConnectionPoolStats
from forze.application.execution import instrument_connection_pools
from forze.application.execution.observability import (
    CONNECTION_POOL_IDLE_GAUGE,
    CONNECTION_POOL_IN_USE_GAUGE,
    CONNECTION_POOL_MAX_GAUGE,
    CONNECTION_POOL_WAITING_GAUGE,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


class _FakeClient:
    """A client whose pool snapshot the test sets directly."""

    def __init__(self, stats: ConnectionPoolStats | None) -> None:
        self.stats = stats

    def connection_pool_stats(self) -> ConnectionPoolStats | None:
        return self.stats


def _meter() -> tuple[Any, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    return MeterProvider(metric_readers=[reader]).get_meter("test"), reader


def _values(reader: InMemoryMetricReader) -> dict[str, dict[str, Any]]:
    """Metric name to ``{forze.client: value}`` from one collection."""

    data = reader.get_metrics_data()
    out: dict[str, dict[str, Any]] = {}

    if data is None:
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                for dp in metric.data.data_points:
                    out.setdefault(metric.name, {})[dp.attributes["forze.client"]] = dp.value

    return out


# ----------------------- #


class TestInstrumentConnectionPools:
    def test_one_series_per_client_label(self) -> None:
        meter, reader = _meter()
        primary = _FakeClient(ConnectionPoolStats(in_use=4, idle=1, max_size=5, waiting=3))
        replica = _FakeClient(ConnectionPoolStats(in_use=0, idle=2, max_size=10, waiting=0))

        instrument_connection_pools({"primary": primary, "replica": replica}, meter=meter)
        values = _values(reader)

        assert values[CONNECTION_POOL_IN_USE_GAUGE] == {"primary": 4, "replica": 0}
        assert values[CONNECTION_POOL_IDLE_GAUGE] == {"primary": 1, "replica": 2}
        assert values[CONNECTION_POOL_MAX_GAUGE] == {"primary": 5, "replica": 10}
        assert values[CONNECTION_POOL_WAITING_GAUGE] == {"primary": 3, "replica": 0}

    # ....................... #

    def test_sampled_at_collection_not_at_registration(self) -> None:
        meter, reader = _meter()
        client = _FakeClient(ConnectionPoolStats(in_use=0, idle=1, max_size=1))

        instrument_connection_pools({"pg": client}, meter=meter)
        client.stats = ConnectionPoolStats(in_use=1, idle=0, max_size=1, waiting=2)

        values = _values(reader)

        assert values[CONNECTION_POOL_IN_USE_GAUGE] == {"pg": 1}
        assert values[CONNECTION_POOL_WAITING_GAUGE] == {"pg": 2}

    # ....................... #

    def test_a_closed_client_reports_nothing(self) -> None:
        meter, reader = _meter()
        open_client = _FakeClient(ConnectionPoolStats(in_use=1, idle=0, max_size=1))

        instrument_connection_pools(
            {"open": open_client, "closed": _FakeClient(None)},
            meter=meter,
        )
        values = _values(reader)

        assert values[CONNECTION_POOL_IN_USE_GAUGE] == {"open": 1}
        assert values[CONNECTION_POOL_MAX_GAUGE] == {"open": 1}

    # ....................... #

    def test_waiting_is_left_out_where_the_driver_does_not_expose_it(self) -> None:
        meter, reader = _meter()
        client = _FakeClient(ConnectionPoolStats(in_use=1, idle=2, max_size=3))

        instrument_connection_pools({"pg": client}, meter=meter)
        values = _values(reader)

        assert values[CONNECTION_POOL_IN_USE_GAUGE] == {"pg": 1}
        assert CONNECTION_POOL_WAITING_GAUGE not in values

    # ....................... #

    def test_refuses_a_client_without_the_method(self) -> None:
        """Refused at wiring: inside a callback the error would repeat on every scrape."""

        meter, _reader = _meter()

        class _TenantRouted:
            def pool_stats(self) -> object:
                return object()

        with pytest.raises(CoreException) as caught:
            instrument_connection_pools({"pg": _TenantRouted()}, meter=meter)  # type: ignore[dict-item]

        assert caught.value.kind == ExceptionKind.CONFIGURATION
        assert "'pg'" in str(caught.value)

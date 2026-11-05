"""Outbound requests in flight: counted at `HttpClient`, exported by `instrument_http_clients`.

httpx exposes no pool numbers, so the count is taken at the client's own methods: from the
moment a request is sent until its response is closed.
"""

import asyncio
from typing import Any

import httpx
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.base.exceptions import CoreException, ExceptionKind
from forze_http import HttpClient, RoutedHttpClient, instrument_http_clients
from forze_http.observability import HTTP_CLIENT_IN_FLIGHT_GAUGE

pytestmark = pytest.mark.unit

# ----------------------- #


async def _client(release: asyncio.Event | None = None, status: int = 200) -> HttpClient:
    """A client whose upstream answers *status*, after *release* is set when given."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        if release is not None:
            await release.wait()

        return httpx.Response(status, content=b"ok")

    client = HttpClient()
    await client.initialize(
        base_url="https://api.example.com",
        transport=httpx.MockTransport(handler),
    )

    return client


async def _until_in_flight(client: HttpClient, count: int) -> None:
    async with asyncio.timeout(2):
        while True:
            if client.in_flight_requests() == count:
                return

            await asyncio.sleep(0)


def _values(reader: InMemoryMetricReader) -> dict[str, Any]:
    """``{forze.client: value}`` for the in-flight gauge, from one collection."""

    data = reader.get_metrics_data()
    out: dict[str, Any] = {}

    if data is None:
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                assert metric.name == HTTP_CLIENT_IN_FLIGHT_GAUGE

                for dp in metric.data.data_points:
                    out[str(dp.attributes["forze.client"])] = dp.value

    return out


# ....................... #


class TestInFlightRequests:
    async def test_a_request_counts_until_its_response_is_read(self) -> None:
        release = asyncio.Event()
        client = await _client(release)
        assert client.in_flight_requests() == 0

        pending = asyncio.ensure_future(client.request("GET", "/x"))
        await _until_in_flight(client, 1)

        release.set()
        await pending

        assert client.in_flight_requests() == 0
        await client.close()

    async def test_a_failed_request_stops_counting(self) -> None:
        client = await _client(status=503)

        with pytest.raises(CoreException):
            await client.request("GET", "/x")

        assert client.in_flight_requests() == 0
        await client.close()

    async def test_a_stream_counts_until_the_caller_leaves_it(self) -> None:
        client = await _client()

        async with client.stream("GET", "/x"):
            assert client.in_flight_requests() == 1

        assert client.in_flight_requests() == 0
        await client.close()

    async def test_a_sent_request_counts_until_its_response_is_closed(self) -> None:
        client = await _client()

        async with client.send(httpx.Request("GET", "https://api.example.com/x")):
            assert client.in_flight_requests() == 1

        assert client.in_flight_requests() == 0
        await client.close()

    async def test_reports_nothing_unless_open(self) -> None:
        client = HttpClient()
        assert client.in_flight_requests() is None

        await client.initialize(base_url="https://api.example.com")
        await client.close()

        assert client.in_flight_requests() is None


# ....................... #


class TestInstrumentHttpClients:
    async def test_one_series_per_client_read_at_collection(self) -> None:
        reader = InMemoryMetricReader()
        release = asyncio.Event()
        busy, quiet, closed = await _client(release), await _client(), HttpClient()

        instrument_http_clients(
            {"busy": busy, "quiet": quiet, "closed": closed},
            meter=MeterProvider(metric_readers=[reader]).get_meter("test"),
        )

        pending = asyncio.ensure_future(busy.request("GET", "/x"))
        await _until_in_flight(busy, 1)

        assert _values(reader) == {"busy": 1, "quiet": 0}

        release.set()
        await pending

        assert _values(reader) == {"busy": 0, "quiet": 0}

        await busy.close()
        await quiet.close()

    def test_refuses_a_routed_client(self) -> None:
        """Refused at wiring: inside a callback the error would repeat on every scrape."""

        routed = RoutedHttpClient.__new__(RoutedHttpClient)  # only its type is checked

        with pytest.raises(CoreException) as caught:
            instrument_http_clients({"api": routed})  # type: ignore[dict-item]

        assert caught.value.kind == ExceptionKind.CONFIGURATION
        assert "'api'" in str(caught.value)

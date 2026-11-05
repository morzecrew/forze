"""Outbound HTTP requests in flight, exported per client.

httpx exposes no public pool numbers, so outbound HTTP has no connection pool gauges. What
:class:`~forze_http.kernel.client.HttpClient` can count honestly is the requests it has
sent and not finished. That is requests, not connections: a request still queued for a
pooled connection counts too. So it gets its own instrument rather than the
``forze.connection_pool.*`` names.
"""

from ._compat import require_http

require_http()

# ....................... #

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from forze.base.exceptions import exc
from forze_http.kernel.client import HttpClient

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

# ----------------------- #

HTTP_CLIENT_IN_FLIGHT_GAUGE = "forze.http.client.in_flight"

# ....................... #


def instrument_http_clients(
    clients: Mapping[str, HttpClient],
    *,
    meter: "Meter | None" = None,
) -> None:
    """Export each client's requests in flight as an OpenTelemetry observable gauge.

    *clients* maps a label you choose (e.g. ``"payments"``) to an
    :class:`~forze_http.kernel.client.HttpClient`. Emits ``forze.http.client.in_flight``,
    labelled ``forze.client``: requests sent and not finished, queued for a connection or
    not. ``request`` counts until it has read the response, ``stream`` and ``send`` until
    their block exits. A client before ``initialize`` or after ``close`` reports nothing.

    The alert that matters: a count that climbs under steady load and never falls back is a
    ``stream`` or ``send`` block that never exits, holding its connection.

    Emits via the global OTel meter unless *meter* is supplied. Call once at assembly time,
    alongside the other ``instrument_*`` calls.

    :raises CoreException: ``configuration`` when a client is not an ``HttpClient``. A
        ``RoutedHttpClient`` is refused: it keeps one client per tenant.
    """

    from opentelemetry import metrics

    clients = dict(clients)

    for label, client in clients.items():
        if not isinstance(client, HttpClient):
            raise exc.configuration(
                f"Client {label!r} ({type(client).__name__}) does not count requests in "
                "flight: only an HttpClient does.",
            )

    meter = meter or metrics.get_meter("forze")

    def callback(_options: "CallbackOptions") -> "Iterable[Observation]":
        for label, client in clients.items():
            count = client.in_flight_requests()

            if count is not None:
                yield metrics.Observation(count, {"forze.client": label})

    meter.create_observable_gauge(
        HTTP_CLIENT_IN_FLIGHT_GAUGE,
        callbacks=[callback],
        unit="1",
        description="Requests sent through the client and not finished: read, or block exited.",
    )

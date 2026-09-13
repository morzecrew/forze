"""The egress marker on the span: declared sensitivity becomes queryable, not just reviewable.

`forze_http` opens no span of its own — the per-call CLIENT span belongs to the generic
port proxy, and only when port spans are enabled. So the attribute goes on whichever span
is current, which is what these tests pin: present when the route declares sensitivity,
absent when it does not, and harmless when nothing is recording at all.
"""

from typing import Any

import httpx
import pytest
from pydantic import BaseModel

from forze.application.integrations.http import build_http_service_spec
from forze.application.integrations.http.descriptors import (
    BaseHttpIntegration,
    async_http_op,
)
from forze_http.adapters.http_service import HttpServiceAdapter
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.kernel.client import HttpClient

pytestmark = pytest.mark.unit

# ----------------------- #


class ListResponse(BaseModel):
    items: list[str] = []


class DemoClient(BaseHttpIntegration):
    list_items = async_http_op(
        request=None,
        response=ListResponse,
        method="GET",
        path="/items",
    )


async def _invoke_under_span(**config_overrides: Any) -> dict[str, Any]:
    """Run one invoke inside a recording span and return that span's attributes."""

    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    spec = build_http_service_spec(DemoClient, name="demo")
    transport = httpx.MockTransport(lambda _r: httpx.Response(200, json={"items": []}))

    client = HttpClient()
    await client.initialize(base_url="https://api.example.com", transport=transport)

    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://api.example.com", **config_overrides),
        spec=spec,
    )

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(InMemorySpanExporter()))
    tracer = provider.get_tracer("test")

    try:
        with tracer.start_as_current_span("caller") as span:
            await adapter.invoke("list_items")

            return dict(span.attributes or {})

    finally:
        await client.aclose()


# ....................... #


class TestTheEgressMarker:
    async def test_a_declared_route_marks_its_span(self) -> None:
        attributes = await _invoke_under_span(
            egress_sensitive=True, acknowledge_data_egress=True
        )

        assert attributes.get("forze.egress.sensitive") is True

    async def test_an_ordinary_route_marks_nothing(self) -> None:
        # The default must stay invisible in the trace as well as in the wiring.
        attributes = await _invoke_under_span()

        assert "forze.egress.sensitive" not in attributes

    async def test_the_call_works_with_no_span_recording(self) -> None:
        # Uninstrumented apps get a non-recording span; setting an attribute on it is a
        # no-op, and must not become an error on the request path.
        spec = build_http_service_spec(DemoClient, name="demo")
        transport = httpx.MockTransport(lambda _r: httpx.Response(200, json={"items": []}))

        client = HttpClient()
        await client.initialize(base_url="https://api.example.com", transport=transport)

        adapter = HttpServiceAdapter(
            client=client,
            config=HttpServiceConfig(
                base_url="https://api.example.com",
                egress_sensitive=True,
                acknowledge_data_egress=True,
            ),
            spec=spec,
        )

        try:
            result = await adapter.invoke("list_items")

        finally:
            await client.aclose()

        assert result.items == []

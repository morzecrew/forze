"""Tests for HttpServiceAdapter."""

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.http import HttpOperationSpec, HttpServiceSpec
from forze.application.integrations.http import build_http_service_spec
from forze.application.integrations.http.descriptors import (
    BaseHttpIntegration,
    async_http_op,
)
from forze.base.exceptions import CoreException, ExceptionKind, error_envelope
from forze_http.adapters.http_service import HttpServiceAdapter
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.kernel.client import HttpClient

# ----------------------- #


class Item(BaseModel):
    id: str


class ListResponse(BaseModel):
    items: list[Item]


class DemoClient(BaseHttpIntegration):
    list_items = async_http_op(
        request=None,
        response=ListResponse,
        method="GET",
        path="/items",
    )


@pytest.mark.asyncio
async def test_invoke_static_base_url() -> None:
    spec = build_http_service_spec(DemoClient, name="demo")

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://api.example.com/items"
        return httpx.Response(200, json={"items": [{"id": "1"}]})

    transport = httpx.MockTransport(handler)
    client = HttpClient()
    await client.initialize(
        base_url="https://api.example.com",
        transport=transport,
    )

    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://api.example.com"),
        spec=spec,
    )

    result = await adapter.invoke("list_items")

    assert result.items[0].id == "1"
    await client.aclose()


async def _capture_traceparent(*, in_span: bool) -> str | None:
    from opentelemetry.sdk.trace import TracerProvider

    spec = build_http_service_spec(DemoClient, name="demo")
    seen: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["traceparent"] = request.headers.get("traceparent")
        return httpx.Response(200, json={"items": []})

    client = HttpClient()
    await client.initialize(
        base_url="https://api.example.com", transport=httpx.MockTransport(handler)
    )
    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://api.example.com"),
        spec=spec,
    )

    tracer = TracerProvider().get_tracer("test")
    try:
        if in_span:
            with tracer.start_as_current_span("caller"):
                await adapter.invoke("list_items")
        else:
            await adapter.invoke("list_items")
    finally:
        await client.aclose()

    return seen["traceparent"]


@pytest.mark.asyncio
async def test_invoke_injects_traceparent_under_an_active_span() -> None:
    # A Forze service calling downstream continues the distributed trace via the W3C header.
    traceparent = await _capture_traceparent(in_span=True)
    assert traceparent is not None and traceparent.startswith("00-")


@pytest.mark.asyncio
async def test_invoke_omits_traceparent_without_a_span() -> None:
    # No active trace → nothing injected (zero impact for an uninstrumented app).
    assert await _capture_traceparent(in_span=False) is None


@pytest.mark.asyncio
async def test_invoke_invalid_response_is_validation_error() -> None:
    spec = build_http_service_spec(DemoClient, name="demo")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"not_items": []})

    transport = httpx.MockTransport(handler)
    client = HttpClient()
    await client.initialize(
        base_url="https://api.example.com",
        transport=transport,
    )

    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://api.example.com"),
        spec=spec,
    )

    with pytest.raises(CoreException) as raised:
        await adapter.invoke("list_items")

    assert raised.value.kind == ExceptionKind.VALIDATION
    assert raised.value.code == "http.response.validation"
    assert raised.value.details is not None
    assert "errors" in raised.value.details
    # The errors describe the counterparty's response: the code that called sees them,
    # a client does not.
    assert error_envelope(raised.value).context is None

    await client.aclose()


@pytest.mark.asyncio
async def test_invoke_unknown_operation() -> None:
    spec = HttpServiceSpec(
        name="demo",
        operations={
            "list": HttpOperationSpec(
                name="list",
                method="GET",
                path="/items",
                args_type=None,
                return_type=ListResponse,
            ),
        },
    )
    client = HttpClient()
    await client.initialize(base_url="https://api.example.com")

    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://api.example.com"),
        spec=spec,
    )

    with pytest.raises(CoreException):
        await adapter.invoke("missing")

    await client.aclose()


@pytest.mark.asyncio
async def test_invoke_propagates_deadline_budget_header() -> None:
    from forze.application.contracts.envelope import HTTP_HEADER_DEADLINE_BUDGET
    from forze.application.execution.context import bind_deadline

    spec = build_http_service_spec(DemoClient, name="demo")
    seen: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["budget"] = request.headers.get(HTTP_HEADER_DEADLINE_BUDGET)
        return httpx.Response(200, json={"items": [{"id": "1"}]})

    client = HttpClient()
    await client.initialize(
        base_url="https://api.example.com",
        transport=httpx.MockTransport(handler),
    )
    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://api.example.com"),
        spec=spec,
    )

    # No deadline bound -> header absent.
    await adapter.invoke("list_items")
    assert seen["budget"] is None

    # Bound deadline -> remaining budget forwarded.
    with bind_deadline(5.0):
        await adapter.invoke("list_items")

    assert seen["budget"] is not None
    assert 0.0 < float(seen["budget"]) <= 5.0

    # Opt-out via config.
    adapter_off = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(
            base_url="https://api.example.com", propagate_deadline=False
        ),
        spec=spec,
    )

    with bind_deadline(5.0):
        await adapter_off.invoke("list_items")

    assert seen["budget"] is None
    await client.aclose()


# ....................... #


class UpstreamError(BaseModel):
    error: str


class DeclaringClient(BaseHttpIntegration):
    list_items = async_http_op(
        request=None,
        response=ListResponse,
        method="GET",
        path="/items",
        error_type=UpstreamError,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("integration", [DemoClient, DeclaringClient])
async def test_an_upstream_404_reaches_the_client_without_the_upstream_context(
    integration: type[BaseHttpIntegration],
) -> None:
    spec = build_http_service_spec(integration, name="billing-internal")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "no such route"})

    client = HttpClient()
    await client.initialize(
        base_url="https://billing.internal.example",
        transport=httpx.MockTransport(handler),
    )
    adapter = HttpServiceAdapter(
        client=client,
        config=HttpServiceConfig(base_url="https://billing.internal.example"),
        spec=spec,
    )

    try:
        with pytest.raises(CoreException) as raised:
            await adapter.invoke("list_items")

    finally:
        await client.aclose()

    envelope = error_envelope(raised.value)

    assert envelope.status == 404
    assert envelope.context is None
    assert "billing" not in envelope.detail

    # Kept for the code that made the call, and for the server through the cause.
    assert raised.value.details is not None
    assert raised.value.details["url"] == "https://billing.internal.example/items"
    assert isinstance(raised.value.__cause__, httpx.HTTPStatusError)

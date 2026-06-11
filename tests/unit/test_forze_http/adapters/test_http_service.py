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
from forze.base.exceptions import CoreException, ExceptionKind
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

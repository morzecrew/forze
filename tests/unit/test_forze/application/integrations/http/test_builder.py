"""Tests for HTTP service spec builder and descriptors."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.http import HttpServicePort, HttpServiceSpec, RawBody
from forze.application.integrations.http import (
    BaseHttpIntegration,
    async_http_op,
    build_http_service_spec,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


class GetOrdersQuery(BaseModel):
    status: str | None = None


class OrdersListResponse(BaseModel):
    items: list[str]


class _FakePort:
    spec: HttpServiceSpec

    def __init__(self, spec: HttpServiceSpec) -> None:
        self.spec = spec

    async def invoke(self, op: str, args: BaseModel | None = None) -> BaseModel:
        return OrdersListResponse(items=[])


class _OrdersOpsMixin(BaseHttpIntegration):
    get_orders = async_http_op(
        request=GetOrdersQuery,
        response=OrdersListResponse,
        method="GET",
        path="/v1/orders",
        query_from=("status",),
    )


class OrdersClient(_OrdersOpsMixin):
    pass


def test_build_http_service_spec() -> None:
    spec = build_http_service_spec(OrdersClient, name="orders")

    assert spec.name == "orders"
    assert "get_orders" in spec.operations
    assert spec.operations["get_orders"].method == "GET"


def test_a_misspelled_response_decoding_is_refused_when_the_spec_is_built() -> None:
    # The descriptor passes the value through; the spec it builds refuses it, before any call.
    class _Csv(BaseHttpIntegration):
        export = async_http_op(
            request=None,
            response=RawBody,
            method="GET",
            path="/export.csv",
            response_decoding="RAW",  # type: ignore[arg-type]
        )

    with pytest.raises(CoreException, match="response_decoding must be one of") as raised:
        build_http_service_spec(_Csv, name="csv")

    assert raised.value.kind is ExceptionKind.CONFIGURATION


async def test_descriptor_invoke() -> None:
    spec = build_http_service_spec(OrdersClient, name="orders")
    port: HttpServicePort = _FakePort(spec)  # type: ignore[assignment]
    client = OrdersClient(port=port, spec=spec)

    result = await client.get_orders(GetOrdersQuery(status="open"))

    assert result.items == []


class _ForeignResponse(BaseModel):
    """A different model type carrying the same fields as ``OrdersListResponse``."""

    items: list[str]


class _ForeignPort:
    spec: HttpServiceSpec

    def __init__(self, spec: HttpServiceSpec) -> None:
        self.spec = spec

    async def invoke(self, op: str, args: BaseModel | None = None) -> BaseModel:
        # Not an ``OrdersListResponse`` instance, so the bound op must convert it.
        return _ForeignResponse(items=["x", "y"])


async def test_descriptor_invoke_converts_foreign_result() -> None:
    """A non-return-type result is validated into the return type via ``from_attributes``."""

    spec = build_http_service_spec(OrdersClient, name="orders")
    port: HttpServicePort = _ForeignPort(spec)  # type: ignore[assignment]
    client = OrdersClient(port=port, spec=spec)

    result = await client.get_orders(GetOrdersQuery(status="open"))

    assert isinstance(result, OrdersListResponse)
    assert result.items == ["x", "y"]

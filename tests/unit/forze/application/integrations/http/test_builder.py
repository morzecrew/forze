"""Tests for HTTP service spec builder and descriptors."""

from pydantic import BaseModel

from forze.application.contracts.http import HttpServiceSpec
from forze.application.integrations.http import (
    BaseHttpIntegration,
    async_http_op,
    build_http_service_spec,
)
from forze.application.contracts.http import HttpServicePort

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


async def test_descriptor_invoke() -> None:
    spec = build_http_service_spec(OrdersClient, name="orders")
    port: HttpServicePort = _FakePort(spec)  # type: ignore[assignment]
    client = OrdersClient(port=port, spec=spec)

    result = await client.get_orders(GetOrdersQuery(status="open"))

    assert result.items == []

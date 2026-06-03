"""Tests for HTTP request part splitting."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.http import HttpOperationSpec
from forze.application.integrations.http import request_parts
from forze.base.exceptions import CoreException
# ----------------------- #


class ListQuery(BaseModel):
    status: str | None = None
    limit: int = 50


class OrderPath(BaseModel):
    order_id: str


def test_get_query_from_fields() -> None:
    op = HttpOperationSpec(
        name="list",
        method="GET",
        path="/v1/orders",
        args_type=ListQuery,
        return_type=ListQuery,
        query_from=frozenset({"status", "limit"}),
    )

    path, query, body = request_parts(op, ListQuery(status="open", limit=10))

    assert path == "/v1/orders"
    assert query == {"status": "open", "limit": 10}
    assert body is None


def test_path_placeholders() -> None:
    op = HttpOperationSpec(
        name="get",
        method="GET",
        path="/v1/orders/{order_id}",
        args_type=OrderPath,
        return_type=OrderPath,
    )

    path, query, body = request_parts(op, OrderPath(order_id="abc"))

    assert path == "/v1/orders/abc"
    assert query is None
    assert body is None


def test_post_body_remainder() -> None:
    class CreateBody(BaseModel):
        name: str
        qty: int

    op = HttpOperationSpec(
        name="create",
        method="POST",
        path="/v1/orders",
        args_type=CreateBody,
        return_type=CreateBody,
    )

    path, query, body = request_parts(op, CreateBody(name="x", qty=2))

    assert path == "/v1/orders"
    assert query is None
    assert body == {"name": "x", "qty": 2}


def test_missing_args_for_path_raises() -> None:
    op = HttpOperationSpec(
        name="get",
        method="GET",
        path="/v1/orders/{order_id}",
        args_type=OrderPath,
        return_type=OrderPath,
    )

    with pytest.raises(CoreException):
        request_parts(op, None)

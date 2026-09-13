"""Tests for HTTP request part splitting."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.http import HttpOperationSpec
from forze.application.integrations.http import form_fields, request_parts
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


def test_path_placeholders_are_url_encoded() -> None:
    op = HttpOperationSpec(
        name="get",
        method="GET",
        path="/v1/orders/{order_id}",
        args_type=OrderPath,
        return_type=OrderPath,
    )

    path, _, _ = request_parts(op, OrderPath(order_id="a/b"))

    assert path == "/v1/orders/a%2Fb"


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


# ....................... #


class TokenRequest(BaseModel):
    grant_type: str
    code: str | None = None
    expires_in: int | None = None
    offline: bool = False
    weight: float = 1.5
    extra: dict[str, str] | None = None


def _form_op() -> HttpOperationSpec[TokenRequest, OrderPath]:
    return HttpOperationSpec(
        name="token",
        method="POST",
        path="/token",
        args_type=TokenRequest,
        return_type=OrderPath,
        body_encoding="form",
    )


class TestFormFields:
    """A form body is flat by definition, so the flattening rule is the contract."""

    def test_scalars_become_strings(self) -> None:
        fields = form_fields(_form_op(), {"grant_type": "authorization_code", "expires_in": 3600})

        assert fields == {"grant_type": "authorization_code", "expires_in": "3600"}

    def test_booleans_are_lowercase(self) -> None:
        # A provider reading "True" rejects the request, and the rejection looks like a
        # credential problem rather than an encoding one — so the spelling is pinned.
        assert form_fields(_form_op(), {"offline": True})["offline"] == "true"
        assert form_fields(_form_op(), {"offline": False})["offline"] == "false"

    def test_none_is_omitted_not_sent_empty(self) -> None:
        # "unset" and "set to empty text" are different requests to a token endpoint.
        assert form_fields(_form_op(), {"grant_type": "x", "code": None}) == {"grant_type": "x"}

    def test_a_float_keeps_its_own_repr(self) -> None:
        assert form_fields(_form_op(), {"weight": 1.5})["weight"] == "1.5"

    def test_a_nested_value_is_refused_by_name(self) -> None:
        # The alternative is httpx stringifying the mapping into "{'a': 'b'}" and sending a
        # request no server can parse — a wiring mistake arriving as a provider error.
        with pytest.raises(CoreException) as raised:
            form_fields(_form_op(), {"grant_type": "x", "extra": {"a": "b"}})

        assert "extra" in str(raised.value)
        assert "scalars only" in str(raised.value)

    def test_a_list_is_refused_too(self) -> None:
        # RFC 0051 §10 leaves repeated keys open; until something decides them, a list is
        # refused rather than silently becoming one comma-joined value.
        with pytest.raises(CoreException):
            form_fields(_form_op(), {"grant_type": "x", "extra": ["a", "b"]})

    def test_a_non_finite_number_is_refused(self) -> None:
        # `str(float("nan"))` is "nan" — a scalar by type, and not a value a form endpoint
        # can read. Refused for the same reason a mapping is.
        for value in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(CoreException) as raised:
                form_fields(_form_op(), {"weight": value})

            assert "weight" in str(raised.value)
            assert "finite" in str(raised.value)

    def test_ordinary_numbers_still_pass(self) -> None:
        # The guard reads `isfinite`, so it must not catch what it is not for.
        assert form_fields(_form_op(), {"weight": 0.0, "expires_in": 0}) == {
            "weight": "0.0",
            "expires_in": "0",
        }

    def test_an_empty_body_stays_empty(self) -> None:
        assert form_fields(_form_op(), {}) == {}

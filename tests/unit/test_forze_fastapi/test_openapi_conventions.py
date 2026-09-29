"""``apply_openapi_conventions``: the schema documents what a Forze app actually serves.

The exception handlers answer every error with the Forze envelope, yet FastAPI documents its
own 422 body on every route that takes input. And descriptions come from reST docstrings, so
Sphinx roles reached the rendered docs verbatim.
"""

from __future__ import annotations

from typing import Annotated, Any

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI, Query
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnSpec
from forze.domain.models import BaseDTO
from forze_fastapi import apply_openapi_conventions
from forze_fastapi.openapi import _markdown  # pyright: ignore[reportPrivateUsage]
from forze_fastapi.security import (
    AuthnRequirement,
    HeaderTokenAuthn,
    apply_openapi_security,
)
from forze_kits.dto.paginated import Pagination

pytestmark = pytest.mark.unit

_ENVELOPE = {"$ref": "#/components/schemas/ErrorResponse"}


class _Order(BaseDTO):
    """An order, see :class:`~shop.orders.Order`.

    :param sku: Stock keeping unit.
    """

    sku: str
    """The ``SKU`` as printed on the label."""


class _Custom422(BaseModel):
    reason: str


def _app() -> FastAPI:
    app = FastAPI()

    @app.post("/orders")
    async def create(order: _Order) -> _Order:
        """Create an order via :meth:`~shop.Orders.create`.

        :param order: The order.
        :returns: The order.
        """

        return order

    @app.get("/page")
    async def page(query: Annotated[Pagination, Query()]) -> int:
        return query.size

    @app.get("/custom", responses={422: {"model": _Custom422}})
    async def custom(n: int) -> int:
        return n

    return app


def _response(schema: dict[str, Any], path: str, method: str, status: str) -> dict[str, Any]:
    return schema["paths"][path][method]["responses"][status]


def _schema_ref(response: dict[str, Any]) -> dict[str, Any]:
    return response["content"]["application/json"]["schema"]


# ....................... #


class TestTheErrorEnvelope:
    def test_fastapis_422_becomes_the_envelope(self) -> None:
        app = _app()
        apply_openapi_conventions(app)
        schema = app.openapi()

        for path, method in (("/orders", "post"), ("/page", "get")):
            response = _response(schema, path, method, "422")
            assert _schema_ref(response) == _ENVELOPE
            assert "X-Error-Code" in response["headers"]

        envelope = schema["components"]["schemas"]["ErrorResponse"]
        assert envelope["required"] == ["detail"]
        assert set(envelope["properties"]) == {"detail", "context"}

    def test_every_operation_gains_a_default_error(self) -> None:
        app = _app()
        apply_openapi_conventions(app)
        schema = app.openapi()

        for path, method in (("/orders", "post"), ("/page", "get"), ("/custom", "get")):
            assert _schema_ref(_response(schema, path, method, "default")) == _ENVELOPE

    def test_a_422_the_app_declared_is_its_own(self) -> None:
        app = _app()
        apply_openapi_conventions(app)
        schema = app.openapi()

        assert _schema_ref(_response(schema, "/custom", "get", "422")) == {
            "$ref": "#/components/schemas/_Custom422"
        }

    def test_fastapis_schemas_go_once_unreferenced(self) -> None:
        app = _app()
        apply_openapi_conventions(app)
        schemas = app.openapi()["components"]["schemas"]

        assert "HTTPValidationError" not in schemas
        assert "ValidationError" not in schemas

    def test_fastapis_schemas_stay_while_the_app_references_them(self) -> None:
        app = _app()
        fastapis_body = {"$ref": "#/components/schemas/HTTPValidationError"}

        @app.get(
            "/still",
            responses={
                400: {"description": "Bad", "content": {"application/json": {"schema": fastapis_body}}}
            },
        )
        async def still(n: int) -> int:
            return n

        apply_openapi_conventions(app)
        schema = app.openapi()

        assert _schema_ref(_response(schema, "/still", "get", "400")) == {
            "$ref": "#/components/schemas/HTTPValidationError"
        }
        assert {"HTTPValidationError", "ValidationError"} <= set(schema["components"]["schemas"])


class TestApplying:
    def test_idempotent(self) -> None:
        app = _app()
        apply_openapi_conventions(app)
        apply_openapi_conventions(app)

        first = app.openapi()
        second = app.openapi()

        assert first is second
        assert _schema_ref(_response(second, "/orders", "post", "422")) == _ENVELOPE

    @pytest.mark.parametrize("security_first", [True, False])
    def test_composes_with_security_in_either_order(self, security_first: bool) -> None:
        app = _app()
        requirement = AuthnRequirement(
            ingress=(
                HeaderTokenAuthn(
                    authn_spec=AuthnSpec(name="api", enabled_methods=frozenset({"token"})),
                    header_name="Authorization",
                ),
            ),
        )

        if security_first:
            apply_openapi_security(app, requirement)
            apply_openapi_conventions(app)
        else:
            apply_openapi_conventions(app)
            apply_openapi_security(app, requirement)

        schema = app.openapi()

        assert schema["components"]["securitySchemes"]
        assert _schema_ref(_response(schema, "/orders", "post", "422")) == _ENVELOPE


class TestMarkupInTheSchema:
    def test_operation_and_model_descriptions_are_markdown(self) -> None:
        app = _app()
        apply_openapi_conventions(app)
        schema = app.openapi()

        operation = schema["paths"]["/orders"]["post"]["description"]
        assert operation == "Create an order via `create`."

        order = schema["components"]["schemas"]["_Order"]
        assert order["description"] == "An order, see `Order`."
        assert order["properties"]["sku"]["description"] == "The `SKU` as printed on the label."

    def test_a_forze_dto_comes_out_clean(self) -> None:
        app = _app()
        apply_openapi_conventions(app)
        parameters = app.openapi()["paths"]["/page"]["get"]["parameters"]
        size = next(p for p in parameters if p["name"] == "size")

        assert size["description"] == (
            "Page size (number of records per page), at most `MAX_PAGE_SIZE`."
        )


# ....................... #


class TestMarkdown:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            (":class:`~a.b.C` here", "`C` here"),
            (":py:meth:`a.b.c()`", "`a.b.c()`"),
            (":doc:`the guide <guides/start>`", "`the guide`"),
            (":data:`MAX_PAGE_SIZE`", "`MAX_PAGE_SIZE`"),
        ],
        ids=["tilde", "domain", "explicit-title", "plain"],
    )
    def test_roles(self, text: str, expected: str) -> None:
        assert _markdown(text) == expected

    def test_double_backtick_literals(self) -> None:
        assert _markdown("Returns ``None`` or ``(a, b)``.") == "Returns `None` or `(a, b)`."

    def test_field_lists_are_dropped_with_their_continuations(self) -> None:
        text = """Summary.

:param x: The x,
    continued here.
:type x: int
:returns: Something.
:rtype: str
:raises CoreException: When bad.

Trailing paragraph."""

        assert _markdown(text) == "Summary.\n\nTrailing paragraph."

    def test_note_and_warning_become_blockquotes(self) -> None:
        text = """Intro.

.. note::
   Careful with ``x``.

.. warning:: Watch out.

Outro."""

        assert _markdown(text) == (
            "Intro.\n\n> **Note:** Careful with `x`.\n\n> **Warning:** Watch out.\n\nOutro."
        )

    def test_other_directives_are_dropped(self) -> None:
        text = """Intro.

.. code-block:: python

   x = 1

Outro."""

        assert _markdown(text) == "Intro.\n\nOutro."

    def test_markdown_is_left_alone(self) -> None:
        text = "Use `x` and **bold**; see [docs](https://example.com)."

        assert _markdown(text) == text

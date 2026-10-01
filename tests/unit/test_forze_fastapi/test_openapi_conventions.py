"""``apply_openapi_conventions``: the schema documents what a Forze app actually serves.

The exception handlers answer every error with the Forze envelope, yet FastAPI documents its
own 422 body on every route that takes input. And descriptions come from reST docstrings, so
Sphinx roles reached the rendered docs verbatim.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Annotated, Any

import pytest

pytest.importorskip("fastapi")

from fastapi import Body, FastAPI, Query
from pydantic import BaseModel, Field

from forze.application.contracts.authn import AuthnSpec
from forze.base.exceptions import CoreException
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

_ENVELOPE = {"$ref": "#/components/schemas/ForzeErrorResponse"}


class _Order(BaseDTO):
    """An order, see :class:`~shop.orders.Order`.

    :param sku: Stock keeping unit.
    """

    sku: str
    """The ``SKU`` as printed on the label."""


class _Custom422(BaseModel):
    reason: str


class _Tagged(BaseDTO):
    meta: dict[str, str] = {"description": "keep ``this``"}


class _Sampled(BaseModel):
    one: dict[str, str] = Field(json_schema_extra={"example": {"description": "keep ``one``"}})
    many: dict[str, str] = Field(examples=[{"summary": "keep ``s``"}])


class ErrorResponse(BaseModel):
    """The app's own error body, sharing the envelope's common name."""

    code: int
    message: str


class _DataNamed(BaseModel):
    example: str = Field(description="The ``example``.")
    default: str = Field(description="The ``default``.")
    enum: str = Field(description="The ``enum``.")
    const: str = Field(description="The ``const``.")
    examples: str = Field(description="The ``examples``.")


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

        envelope = schema["components"]["schemas"]["ForzeErrorResponse"]
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

    def test_an_apps_own_error_response_model_is_untouched(self) -> None:
        app = _app()

        @app.get("/mine", responses={400: {"model": ErrorResponse}})
        async def mine(n: int) -> int:
            return n

        before = deepcopy(FastAPI.openapi(app)["components"]["schemas"]["ErrorResponse"])
        app.openapi_schema = None
        apply_openapi_conventions(app)
        schema = app.openapi()

        assert schema["components"]["schemas"]["ErrorResponse"] == before
        assert _schema_ref(_response(schema, "/mine", "get", "400")) == {
            "$ref": "#/components/schemas/ErrorResponse"
        }

    def test_a_different_body_under_the_envelopes_name_is_refused(self) -> None:
        app = _app()
        ForzeErrorResponse = type("ForzeErrorResponse", (BaseModel,), {"__annotations__": {"x": int}})

        @app.get("/clash", responses={400: {"model": ForzeErrorResponse}})
        async def clash(n: int) -> int:
            return n

        apply_openapi_conventions(app)

        # Routers may be attached after the call, so the conflict fails the schema request.
        with pytest.raises(CoreException) as caught:
            app.openapi()

        assert caught.value.kind.value == "configuration"

        # Refused before anything was rewritten: the schema FastAPI cached is still its own.
        cached = app.openapi_schema
        assert cached is not None
        assert "default" not in cached["paths"]["/clash"]["get"]["responses"]
        assert _schema_ref(_response(cached, "/orders", "post", "422")) == {
            "$ref": "#/components/schemas/HTTPValidationError"
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

    def test_applied_last_it_converts_what_security_added(self) -> None:
        app = _app()
        requirement = AuthnRequirement(
            ingress=(
                HeaderTokenAuthn(
                    authn_spec=AuthnSpec(name="api", enabled_methods=frozenset({"token"})),
                    header_name="X-Key",
                    description="A key per :class:`~a.Key`.",
                ),
            ),
        )
        apply_openapi_security(app, requirement)
        apply_openapi_conventions(app)

        [scheme] = app.openapi()["components"]["securitySchemes"].values()

        assert scheme["description"] == "A key per `Key`."

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

    def test_instance_data_is_not_prose(self) -> None:
        # A default or example is data the client sends back; rewriting it would change it.
        app = FastAPI()

        @app.post("/tagged")
        async def tagged(body: _Tagged) -> None:
            return None

        apply_openapi_conventions(app)
        meta = app.openapi()["components"]["schemas"]["_Tagged"]["properties"]["meta"]

        assert meta["default"] == {"description": "keep ``this``"}

    def test_fields_named_like_data_keys_are_prose(self) -> None:
        app = FastAPI()

        @app.post("/named")
        async def named(body: _DataNamed) -> None:
            return None

        apply_openapi_conventions(app)
        properties = app.openapi()["components"]["schemas"]["_DataNamed"]["properties"]

        for name in ("example", "default", "enum", "const", "examples"):
            assert properties[name]["description"] == f"The `{name}`."

    def test_example_objects_have_prose_and_data(self) -> None:
        app = FastAPI()
        value = {"description": "keep ``this``", "summary": ":class:`~a.B`"}
        examples: dict[str, Any] = {
            "one": {"summary": "Uses ``x``", "description": "See :class:`~a.B`.", "value": value}
        }

        @app.post("/ex")
        async def ex(body: _Tagged = Body(openapi_examples=examples)) -> None:  # noqa: B008
            return None

        apply_openapi_conventions(app)
        media = app.openapi()["paths"]["/ex"]["post"]["requestBody"]["content"]
        example = media["application/json"]["examples"]["one"]

        assert example == {"summary": "Uses `x`", "description": "See `B`.", "value": value}

    def test_schema_examples_are_data(self) -> None:
        app = FastAPI()

        @app.post("/sample")
        async def sample(body: _Sampled) -> None:
            return None

        apply_openapi_conventions(app)
        properties = app.openapi()["components"]["schemas"]["_Sampled"]["properties"]

        assert properties["one"]["example"] == {"description": "keep ``one``"}
        assert properties["many"]["examples"] == [{"summary": "keep ``s``"}]

    def test_server_variables_named_like_data_keys_are_prose(self) -> None:
        variables = {"default": {"default": "prod", "description": "The ``env``."}}
        app = FastAPI(servers=[{"url": "https://{default}.x", "variables": variables}])
        apply_openapi_conventions(app)

        [server] = app.openapi()["servers"]

        assert server["variables"]["default"] == {"default": "prod", "description": "The `env`."}

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
            (":class:`.Foo` here", "`Foo` here"),
            (":meth:`.Foo.bar`", "`Foo.bar`"),
            (":class:`~.a.Foo`", "`Foo`"),
        ],
        ids=["tilde", "domain", "explicit-title", "plain", "relative", "relative-dotted", "relative-tilde"],
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

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            (".. versionadded:: 2.0\n   The ``x`` field.", "> **Added in:** 2.0\n> The `x` field."),
            (
                ".. admonition:: Heads up\n\n   Body.",
                "> **Admonition:** Heads up\n>\n> Body.",
            ),
            (".. custom:: arg\n   Body ``x``.", "> **Custom:** arg\n> Body `x`."),
        ],
        ids=["versionadded", "generic", "unknown"],
    )
    def test_every_directive_keeps_its_prose(self, text: str, expected: str) -> None:
        assert _markdown(text) == expected

    def test_an_admonition_body_is_converted_like_the_top_level(self) -> None:
        text = """Intro.

.. note::
   Example:

   .. code-block:: python

      x = ``a``

   And ``b`` via :class:`~a.C`::

      y = ``c``

After."""

        assert _markdown(text) == (
            "Intro.\n\n> **Note:** Example:\n>\n> ```python\n> x = ``a``\n> ```\n>\n"
            "> And `b` via `C`:\n>\n> ```\n> y = ``c``\n> ```\n\nAfter."
        )

    def test_a_fence_in_an_admonition_is_verbatim(self) -> None:
        text = ".. note::\n   Use:\n\n   ```python\n   x = ``a``\n   ```"

        assert _markdown(text) == "> **Note:** Use:\n>\n> ```python\n> x = ``a``\n> ```"

    def test_a_rest_comment_stays_as_text(self) -> None:
        text = "Items:\n.. and so on\n\nOutro."

        assert _markdown(text) == text

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            (
                "Send::\n\n    :method: GET\n    :path: /orders",
                "Send:\n\n```\n:method: GET\n:path: /orders\n```",
            ),
            (
                ".. code-block:: yaml\n\n   :key: value\n   other: 1",
                "```yaml\n:key: value\nother: 1\n```",
            ),
        ],
        ids=["literal", "code-after-blank"],
    )
    def test_code_that_looks_like_options_is_code(self, text: str, expected: str) -> None:
        assert _markdown(text) == expected

    def test_a_generated_fence_outlasts_the_backticks_inside(self) -> None:
        text = "Example::\n\n    ```\n    x\n    ```"

        assert _markdown(text) == "Example:\n\n````\n```\nx\n```\n````"

    @pytest.mark.parametrize(
        ("name", "label"),
        [
            ("deprecated", "Deprecated"),
            ("important", "Important"),
            ("danger", "Danger"),
            ("caution", "Caution"),
            ("attention", "Attention"),
            ("tip", "Tip"),
            ("hint", "Hint"),
            ("seealso", "See also"),
        ],
    )
    def test_every_admonition_keeps_its_content(self, name: str, label: str) -> None:
        text = f"Intro.\n\n.. {name}::\n   Use ``v2``.\n\nOutro."

        assert _markdown(text) == f"Intro.\n\n> **{label}:** Use `v2`.\n\nOutro."

    def test_an_admonition_argument_leads_its_body(self) -> None:
        text = ".. deprecated:: 2.0\n   Use ``/v2/orders`` instead."

        assert _markdown(text) == "> **Deprecated:** 2.0\n> Use `/v2/orders` instead."

    def test_a_code_block_becomes_a_fence(self) -> None:
        text = """Example:

.. code-block:: python

   def f(x):
       return ``x``

Outro."""

        assert _markdown(text) == (
            "Example:\n\n```python\ndef f(x):\n    return ``x``\n```\n\nOutro."
        )

    def test_a_code_blocks_options_are_not_code(self) -> None:
        text = ".. code-block:: python\n   :linenos:\n\n   x = 1"

        assert _markdown(text) == "```python\nx = 1\n```"

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("Example::\n\n    x = ``1``\n\nOutro.", "Example:\n\n```\nx = ``1``\n```\n\nOutro."),
            ("Intro.\n\n::\n\n    x = 1", "Intro.\n\n```\nx = 1\n```"),
        ],
        ids=["trailing", "expanded"],
    )
    def test_a_literal_block_becomes_a_fence(self, text: str, expected: str) -> None:
        assert _markdown(text) == expected

    def test_a_double_colon_with_no_block_is_text(self) -> None:
        assert _markdown("See http://h::1 and a::") == "See http://h::1 and a::"

    @pytest.mark.parametrize("fence", ["```", "~~~"])
    def test_fenced_blocks_are_verbatim(self, fence: str) -> None:
        inside = ':type: order\n:param x: The x.\n.. note::\n   Keep.\nUse ``lit`` and :class:`~a.B`.'
        text = f"Config:\n\n{fence}yaml\n{inside}\n{fence}\n\nAfter ``x``."

        assert _markdown(text) == f"Config:\n\n{fence}yaml\n{inside}\n{fence}\n\nAfter `x`."

    def test_inline_triple_backticks_open_no_fence(self) -> None:
        # A backtick fence's info string cannot hold a backtick, so this is a code span.
        assert _markdown("```x``` inline\nand ``y``.") == "```x``` inline\nand `y`."

    def test_hard_line_breaks_survive(self) -> None:
        assert _markdown("Line one  \nline two") == "Line one  \nline two"

    def test_an_unpaired_double_backtick_pairs_with_nothing(self) -> None:
        assert _markdown("Use `` for nothing, then ``x``.") == "Use `` for nothing, then `x`."

    def test_a_role_needs_a_word_boundary(self) -> None:
        assert _markdown("Keep foo:bar:`baz` as is.") == "Keep foo:bar:`baz` as is."

    def test_markdown_is_left_alone(self) -> None:
        text = "Use `x` and **bold**; see [docs](https://example.com)."

        assert _markdown(text) == text

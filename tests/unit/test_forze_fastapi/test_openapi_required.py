"""A response schema marks required what its routes always send.

In serialization mode pydantic leaves every field with a default optional, so a generated client
types an always-sent field as optional. A route that keeps nulls sends every field; one that drops
them still sends every field that cannot be null.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from forze_fastapi.openapi import apply_openapi_conventions

# ----------------------- #


class _Child(BaseModel):
    note: str | None = None
    size: int = 0


class _Kept(BaseModel):
    """Returned only by a route that keeps nulls."""

    label: str | None = None
    count: int = 0
    child: _Child


class _Leaf(BaseModel):
    size: int = 0


class _Dropped(BaseModel):
    """Returned only by a route that drops nulls."""

    label: str | None = None
    count: int = 0
    tags: list[str] = []
    anything: Any = None
    cursor: str | None
    leaf: _Leaf = _Leaf()
    either: int | str = 0


class _OnlyNullable(BaseModel):
    """Returned by a route that drops nulls, with nothing that cannot be null."""

    cursor: str | None


class _Trimmed(BaseModel):
    """Returned by a route that leaves unset fields out."""

    count: int = 0


class _Both(BaseModel):
    """Returned by both kinds of route."""

    label: str | None = None
    count: int = 0


class _Shared(BaseModel):
    """Taken as a request body and returned as a response."""

    label: str | None = None
    count: int = 0


def _app() -> FastAPI:
    app = FastAPI()

    @app.get("/kept", response_model=_Kept, response_model_exclude_none=False)
    async def kept() -> Any: ...

    # Included under a prefix, as routers are: it is found at its mounted path.
    api = APIRouter(prefix="/api")

    @api.get("/dropped", response_model=_Dropped, response_model_exclude_none=True)
    async def dropped() -> Any: ...

    app.include_router(api)

    @app.get("/both-kept", response_model=_Both, response_model_exclude_none=False)
    async def both_kept() -> Any: ...

    @app.get("/both-dropped", response_model=_Both, response_model_exclude_none=True)
    async def both_dropped() -> Any: ...

    @app.post("/shared", response_model=_Shared, response_model_exclude_none=False)
    async def shared(body: _Shared) -> Any: ...

    @app.get("/trimmed", response_model=_Trimmed, response_model_exclude_unset=True)
    async def trimmed() -> Any: ...

    # Left out of the schema, as the JWKS and readiness routes are: nothing to mark.
    @app.get("/hidden", response_model=_Trimmed, include_in_schema=False)
    async def hidden() -> Any: ...

    @app.get("/only-nullable", response_model=_OnlyNullable, response_model_exclude_none=True)
    async def only_nullable() -> Any: ...

    return app


def _required(schema: dict[str, Any], name: str) -> set[str]:
    return set(schema["components"]["schemas"][name].get("required", []))


# ....................... #


def test_a_route_that_keeps_nulls_requires_every_field_it_sends() -> None:
    app = _app()
    apply_openapi_conventions(app, require_sent_fields=True)
    schema = app.openapi()

    assert _required(schema, "_Kept") == {"label", "count", "child"}
    # A model nested in that response is sent whole too.
    assert _required(schema, "_Child") == {"note", "size"}


def test_a_route_that_drops_nulls_requires_what_cannot_be_null() -> None:
    app = _app()
    apply_openapi_conventions(app, require_sent_fields=True)
    schema = app.openapi()

    # A null cursor is dropped too, though pydantic lists it required for having no default.
    assert _required(schema, "_Dropped") == {"count", "tags", "leaf", "either"}
    assert _required(schema, "_Leaf") == {"size"}
    assert "required" not in schema["components"]["schemas"]["_OnlyNullable"]
    # Returned by both kinds of route: only what survives dropping the nulls.
    assert _required(schema, "_Both") == {"count"}


def test_a_schema_a_request_also_uses_or_a_route_trims_is_left_alone() -> None:
    app = _app()
    apply_openapi_conventions(app, require_sent_fields=True)
    schema = app.openapi()

    assert _required(schema, "_Shared") == set()
    assert _required(schema, "_Trimmed") == set()


def test_left_out_unless_asked_for() -> None:
    app = _app()
    apply_openapi_conventions(app)
    schema = app.openapi()

    assert _required(schema, "_Kept") == {"child"}
    assert _required(schema, "_Dropped") == {"cursor"}


# ....................... #


type _MaybeName = str | None
"""A named alias pydantic documents as a component of its own, referenced by ``$ref``."""

type _Tree = _Node | None


class _Node(BaseModel):
    child: _Tree = None


class _Aliased(BaseModel):
    name: _MaybeName = None
    tree: _Tree = None
    count: int = 0


class _Count(BaseModel):
    count: int = 0


class _WithExample(BaseModel):
    model_config = {"json_schema_extra": {"examples": [{"$ref": "#/components/schemas/Nope"}]}}

    count: int = 0


def _single(model: type[BaseModel], *, exclude_none: bool) -> FastAPI:
    app = FastAPI()

    @app.get("/one", response_model=model, response_model_exclude_none=exclude_none)
    async def one() -> Any: ...

    return app


def test_a_reference_is_as_nullable_as_what_it_names() -> None:
    """A named alias that admits null is left out when null, behind its ``$ref``; an alias that
    refers to itself is followed once."""

    app = _single(_Aliased, exclude_none=True)
    apply_openapi_conventions(app, require_sent_fields=True)

    assert _required(app.openapi(), "_Aliased") == {"count"}


def test_a_component_that_refers_to_itself_is_followed_once() -> None:
    """pydantic does not write such a loop, but an app's own component can."""

    app = _single(_Count, exclude_none=True)
    original = app.openapi

    def with_loop() -> dict[str, Any]:
        schema = original()
        loop = {"anyOf": [{"$ref": "#/components/schemas/_Loop"}, {"type": "integer"}]}
        schema["components"]["schemas"]["_Loop"] = loop
        properties = schema["components"]["schemas"]["_Count"]["properties"]
        properties["loop"] = {"$ref": "#/components/schemas/_Loop"}
        # A reference this schema cannot resolve may name anything, null included.
        properties["elsewhere"] = {"$ref": "https://example.com/schemas.json#/Thing"}
        return schema

    app.openapi = with_loop  # type: ignore[method-assign]
    apply_openapi_conventions(app, require_sent_fields=True)

    assert _required(app.openapi(), "_Count") == {"count", "loop"}


def test_a_reference_written_as_example_data_is_not_followed() -> None:
    app = _single(_WithExample, exclude_none=False)
    apply_openapi_conventions(app, require_sent_fields=True)

    assert _required(app.openapi(), "_WithExample") == {"count"}


def test_asking_again_with_require_sent_fields_takes_effect() -> None:
    app = _single(_Child, exclude_none=False)
    apply_openapi_conventions(app)
    apply_openapi_conventions(app, require_sent_fields=True)

    assert _required(app.openapi(), "_Child") == {"note", "size"}

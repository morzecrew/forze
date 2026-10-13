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

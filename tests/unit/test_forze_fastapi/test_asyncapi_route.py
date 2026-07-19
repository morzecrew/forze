"""Serving the AsyncAPI contract over HTTP — the egress twin of ``/openapi.json``.

# covers: forze_fastapi.routes.asyncapi (attach_asyncapi_route, wiring refusal,
#         openapi-schema exclusion, composition with the forze_socketio generator)

The route helper serves a caller-built document (integration packages cannot import
each other, so the app composes the socketio generator with the fastapi route); this
test does exactly that composition, end to end.
"""

from __future__ import annotations

import pytest
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient

from forze.base.exceptions import CoreException
from forze_fastapi.routes import attach_asyncapi_route

# ----------------------- #


class _OrderView(BaseModel):
    order_id: str


def _document() -> dict:
    from forze.application.contracts.realtime import RealtimeEvent, RealtimeEventCatalog
    from forze_socketio import asyncapi_document

    catalog = RealtimeEventCatalog.of(RealtimeEvent(name="order.updated", payload_type=_OrderView))

    return asyncapi_document(catalog, title="Orders realtime", version="1.0.0")


def _client(document: dict, **attach_kwargs: str) -> TestClient:
    router = APIRouter()
    attach_asyncapi_route(router, document=document, **attach_kwargs)

    app = FastAPI()
    app.include_router(router)

    return TestClient(app)


# ----------------------- #


def test_serves_the_generated_document_verbatim() -> None:
    document = _document()
    client = _client(document)

    response = client.get("/asyncapi.json")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == document  # verbatim — codegen tooling consumes this URL

    served = response.json()
    assert served["asyncapi"] == "3.0.0"
    assert "order.updated" in served["channels"]


def test_custom_path_and_openapi_exclusion() -> None:
    client = _client(_document(), path="/contracts/realtime.json")

    assert client.get("/contracts/realtime.json").status_code == 200
    # mirrors /openapi.json: the contract endpoint documents the app, not itself
    assert "/contracts/realtime.json" not in client.get("/openapi.json").json()["paths"]


def test_non_asyncapi_dict_is_refused_at_attach() -> None:
    with pytest.raises(CoreException):
        attach_asyncapi_route(APIRouter(), document={"openapi": "3.1.0"})


def test_served_document_is_a_snapshot() -> None:
    document = _document()
    client = _client(document)

    document["channels"].clear()  # a later caller-side mutation must not leak

    assert "order.updated" in client.get("/asyncapi.json").json()["channels"]

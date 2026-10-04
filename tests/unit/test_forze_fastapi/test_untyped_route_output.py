"""A route whose operation names no output type writes a model result with pydantic.

Without a response model FastAPI encoded every result through ``jsonable_encoder``. Now a model
result is written by pydantic, as a typed route's is; any other result keeps that encoding.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, BackgroundTasks, FastAPI, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel, Field

from forze.application.contracts.execution import Handler
from forze.application.contracts.search import FederatedSearchReadModel
from forze.application.execution.operations import OperationDescriptor, OperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import BaseDTO
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import RouteBinding, attach_operation_routes, body_endpoint
from forze_fastapi.routes._attach import OperationRunner, require_input_type
from forze_kits.aggregates.search.dto import SearchPaginated
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

NS = StrKeyNamespace(prefix="out")


class _Ask(BaseDTO):
    n: int = 1


class _Kind(StrEnum):
    A = "a"


class _Child(BaseModel):
    x: int | None = None
    y: int = 2


class _Base(BaseModel):
    a: int


class _Sub(_Base):
    secret: str


class _Row(BaseModel):
    id: UUID
    label: str = Field(alias="Label")
    at: datetime
    price: Decimal
    span: timedelta
    kind: _Kind
    note: str | None = None
    child: _Child
    base: _Base
    meta: dict[str, Any] = {}


@dataclasses.dataclass
class _Point:
    x: int
    at: datetime


class _Plain:
    def __init__(self) -> None:
        self.a = 1


class _Return(Handler[Any, Any]):
    def __init__(self, result: Any) -> None:
        self.result = result

    async def __call__(self, args: Any) -> Any:
        return self.result


_AT = datetime(2026, 10, 4, 12, 0, tzinfo=UTC)


def _row(**changes: Any) -> _Row:
    fields: dict[str, Any] = {
        "id": UUID(int=7),
        "Label": "seven",
        "at": _AT,
        "price": Decimal("1.50"),
        "span": timedelta(seconds=90),
        "kind": _Kind.A,
        "child": _Child(),
        "base": _Sub(a=1, secret="s"),
        "meta": {"ok": 2},
    }

    return _Row(**(fields | changes))


def _app(
    result: Any,
    *,
    build: Any = body_endpoint,
    typed: type[BaseModel] | None = None,
    status_code: int | None = None,
    exclude_none: bool = True,
) -> FastAPI:
    router = APIRouter()
    attach_operation_routes(
        router,
        registry=OperationRegistry(
            handlers={NS.key("run"): lambda _ctx: _Return(result)},
            descriptors={NS.key("run"): OperationDescriptor(input_type=_Ask, output_type=typed)},
        ).freeze(),
        ns=NS,
        ctx_dep=lambda: context_from_modules(MockDepsModule()),
        bindings={
            "run": RouteBinding(method="POST", path="/run", build=build, status_code=status_code)
        },
        exclude_none=exclude_none,
    )
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return app


async def _post(app: FastAPI) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://r") as client:
        return await client.post("/run", json={"n": 1})


def _untyped_encoding(value: Any) -> Any:
    """What a route without a response model answered: ``jsonable_encoder``, then JSON."""

    return json.loads(json.dumps(jsonable_encoder(value), allow_nan=False))


# ....................... #


class TestAResultThatIsNotAModel:
    """Keeps the untyped encoding exactly, down to bare decimals and durations."""

    @pytest.mark.parametrize(
        "value",
        [
            {"d": Decimal("1.50"), "td": timedelta(seconds=90), "n": None, "_sale": 1},
            [_row(), _row(note="x")],
            _Point(x=1, at=_AT),
            {1, 2},
            _Plain(),
            "text",
            None,
        ],
        ids=["dict", "list_of_models", "dataclass", "set", "plain_object", "str", "none"],
    )
    async def test_is_encoded_as_without_a_response_model(self, value: Any) -> None:
        response = await _post(_app(value))

        assert response.status_code == 200
        assert response.json() == _untyped_encoding(value)

    async def test_a_response_passes_through_untouched(self) -> None:
        response = await _post(_app(Response(b"raw", media_type="text/plain", status_code=202)))

        assert (response.status_code, response.text) == (202, "raw")

    async def test_a_streaming_response_passes_through_untouched(self) -> None:
        async def chunks() -> Any:
            yield b"a"
            yield b"b"

        response = await _post(_app(StreamingResponse(chunks(), media_type="text/plain")))

        assert (response.status_code, response.text) == (200, "ab")


class TestAModelResult:
    """Is written as a typed route writes it."""

    async def test_matches_the_untyped_encoding_apart_from_none_fields(self) -> None:
        row = _row()

        response = await _post(_app(row))

        assert response.json() == _untyped_encoding(
            row.model_dump(mode="json", by_alias=True, exclude_none=True)
        )
        assert response.json() == (await _post(_app(row, typed=_Row))).json()

    async def test_drops_none_fields_at_every_depth(self) -> None:
        body = (await _post(_app(_row()))).json()

        assert "note" not in body
        assert body["child"] == {"y": 2}

    async def test_keeps_none_fields_when_the_attacher_does(self) -> None:
        row = _row()

        response = await _post(_app(row, exclude_none=False))

        assert response.json() == _untyped_encoding(row)

    async def test_keeps_an_alias_and_writes_a_subclass_as_its_declared_field(self) -> None:
        body = (await _post(_app(_row()))).json()

        assert body["Label"] == "seven"
        assert body["base"] == {"a": 1}

    async def test_writes_infinity_and_nan_as_null(self) -> None:
        # The untyped encoding refused them with a 500; a typed route writes null.
        row = _row(meta={"inf": math.inf, "nan": math.nan})

        response = await _post(_app(row))

        assert response.status_code == 200
        assert response.json()["meta"] == {"inf": None, "nan": None}

    async def test_keeps_a_key_starting_with_sa(self) -> None:
        # ``jsonable_encoder`` dropped such keys as SQLAlchemy state; a typed route keeps them.
        body = (await _post(_app(_row(meta={"_sale": 1, "ok": 2})))).json()

        assert body["meta"] == {"_sale": 1, "ok": 2}

    async def test_a_federated_page_matches_the_untyped_encoding(self) -> None:
        page = SearchPaginated[FederatedSearchReadModel[_Row]](
            hits=[FederatedSearchReadModel(hit=_row(), member="rows") for _ in range(3)],
            page=1,
            size=20,
            count=3,
        )

        response = await _post(_app(page))

        assert response.json() == _untyped_encoding(
            page.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


# ....................... #


def _builder(post: Any = None, *, header: bool = False, task: list[str] | None = None) -> Any:
    """An endpoint builder of an app's own: it may read the result and use the injected response."""

    def build(runner: OperationRunner, input_type: Any, op: str) -> Any:
        dto = require_input_type(input_type, op)

        async def endpoint(payload: Any, response: Response, background: BackgroundTasks) -> Any:
            result = await runner(payload)

            if header:
                response.headers["X-Custom"] = "yes"
                response.set_cookie("sid", "abc")

            if task is not None:
                background.add_task(task.append, "ran")

            return post(result) if post is not None else result

        kinds = (
            ("payload", dto),
            ("response", Response),
            ("background", BackgroundTasks),
        )
        endpoint.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
            [
                inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=kind)
                for name, kind in kinds
            ]
        )

        return endpoint

    return build


class TestAnEndpointBuilder:
    async def test_receives_the_operations_result_itself(self) -> None:
        build = _builder(lambda result: {"label": result.label})

        assert (await _post(_app(_row(), build=build))).json() == {"label": "seven"}

    async def test_keeps_headers_and_cookies_it_sets_on_the_injected_response(self) -> None:
        response = await _post(_app(_row(), build=_builder(header=True)))

        assert response.headers["x-custom"] == "yes"
        assert "sid=abc" in response.headers["set-cookie"]

    async def test_keeps_the_bindings_status_code_and_background_tasks(self) -> None:
        ran: list[str] = []

        response = await _post(_app(_row(), build=_builder(task=ran), status_code=201))

        assert response.status_code == 201
        assert ran == ["ran"]

    async def test_a_status_without_a_body_takes_no_response_model(self) -> None:
        # FastAPI refuses a response model on a 204 when the route is added.
        response = await _post(_app(None, status_code=204))

        assert (response.status_code, response.content) == (204, b"")


class TestTheSchema:
    def test_an_untyped_operation_documents_an_untitled_any(self) -> None:
        schema = _app(None).openapi()["paths"]["/run"]["post"]["responses"]["200"]

        content = schema["content"]["application/json"]["schema"]
        assert set(content) == {"title"}

    def test_a_typed_operation_keeps_its_model(self) -> None:
        schema = _app(None, typed=_Row).openapi()["paths"]["/run"]["post"]["responses"]["200"]

        assert schema["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/_Row"
        }

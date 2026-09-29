"""Hand-written operation routes: an app's own binding table through ``attach_operation_routes``.

A route an attacher does not ship (``stock.levels``, ``stock.add``) is a binding in the app's
table, not a hand-written endpoint: the schema, the ``operation_id`` and the dispatch come from
the catalog. ``query_endpoint`` covers the GET whose input arrives as query parameters.
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4

import attrs
import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient

from forze.application.contracts.execution import Handler
from forze.application.execution.operations import OperationDescriptor, OperationRegistry
from forze.base.exceptions import CoreException
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import BaseDTO
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.routes import (
    RouteBinding,
    attach_operation_routes,
    body_endpoint,
    id_endpoint,
    query_endpoint,
)
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

pytestmark = pytest.mark.unit

# ----------------------- #

STOCK = StrKeyNamespace(prefix="stock")


class _LevelsQuery(BaseDTO):
    sku: str
    limit: int = 10
    tags: list[str] = []


class _Levels(BaseDTO):
    sku: str
    limit: int
    tags: list[str]


class _Add(BaseDTO):
    sku: str
    qty: int


class _ById(BaseDTO):
    id: UUID


class _Nested(BaseDTO):
    a: int


class _NestedQuery(BaseDTO):
    inner: _Nested


class _UnionQuery(BaseDTO):
    inner: int | _Nested


class _ListOfModelsQuery(BaseDTO):
    inner: list[_Nested] = []


class _MappingQuery(BaseDTO):
    inner: dict[str, str] = {}


class _Kind(StrEnum):
    RAW = "raw"
    DONE = "done"


class _ScalarsQuery(BaseDTO):
    sku: str
    kind: _Kind = _Kind.RAW
    mode: Literal["a", "b"] = "a"
    since: date | None = None
    owner: UUID | None = None
    limit: int | None = None
    tags: frozenset[str] = frozenset()


@attrs.define(slots=True, kw_only=True)
class _Echo(Handler[Any, Any]):
    out: type[BaseDTO]

    async def __call__(self, args: Any) -> Any:
        return self.out.model_validate(args.model_dump())


_BINDINGS = {
    "levels": RouteBinding(method="GET", path="/levels", build=query_endpoint),
    "add": RouteBinding(method="POST", path="/add", build=body_endpoint, status_code=201),
    "get": RouteBinding(method="GET", path="/{id}", build=id_endpoint),
}


def _registry(levels_input: type[BaseDTO] = _LevelsQuery) -> Any:
    return OperationRegistry(
        handlers={
            STOCK.key("levels"): lambda _c: _Echo(out=_Levels),
            STOCK.key("add"): lambda _c: _Echo(out=_Add),
            STOCK.key("get"): lambda _c: _Echo(out=_ById),
        },
        descriptors={
            STOCK.key("levels"): OperationDescriptor(
                input_type=levels_input, output_type=_Levels, description="Stock levels."
            ),
            STOCK.key("add"): OperationDescriptor(
                input_type=_Add, output_type=_Add, description="Add stock."
            ),
            STOCK.key("get"): OperationDescriptor(
                input_type=_ById, output_type=_ById, description="One item."
            ),
        },
    ).freeze()


def _app(levels_input: type[BaseDTO] = _LevelsQuery) -> FastAPI:
    router = APIRouter(prefix="/stock")
    attach_operation_routes(
        router,
        registry=_registry(levels_input),
        ns=STOCK,
        ctx_dep=lambda: context_from_modules(MockDepsModule()),
        bindings=_BINDINGS,
    )
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return app


def _client(app: FastAPI) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://stock")


# ....................... #


class TestTheBindingTable:
    def test_each_route_is_named_by_its_namespaced_key(self) -> None:
        paths = _app().openapi()["paths"]

        assert paths["/stock/levels"]["get"]["operationId"] == "stock.levels"
        assert paths["/stock/add"]["post"]["operationId"] == "stock.add"
        assert paths["/stock/{id}"]["get"]["operationId"] == "stock.get"

    async def test_the_body_and_path_routes_dispatch(self) -> None:
        async with _client(_app()) as client:
            added = await client.post("/stock/add", json={"sku": "a-1", "qty": 3})
            item = uuid4()
            fetched = await client.get(f"/stock/{item}")

        assert added.status_code == 201
        assert added.json() == {"sku": "a-1", "qty": 3}
        assert fetched.json() == {"id": str(item)}


class TestTheQueryRoute:
    def test_every_field_is_a_query_parameter(self) -> None:
        params = _app().openapi()["paths"]["/stock/levels"]["get"]["parameters"]

        assert {(p["name"], p["in"], p["required"]) for p in params} == {
            ("sku", "query", True),
            ("limit", "query", False),
            ("tags", "query", False),
        }

    async def test_the_query_dispatches_and_a_list_repeats(self) -> None:
        async with _client(_app()) as client:
            response = await client.get(
                "/stock/levels", params=[("sku", "a-1"), ("tags", "x"), ("tags", "y")]
            )

        assert response.status_code == 200
        assert response.json() == {"sku": "a-1", "limit": 10, "tags": ["x", "y"]}

    @pytest.mark.parametrize(
        "params",
        [{}, {"sku": "a-1", "limit": "many"}],
        ids=["missing-required", "not-an-int"],
    )
    async def test_a_bad_query_answers_the_forze_envelope(self, params: dict[str, str]) -> None:
        async with _client(_app()) as client:
            response = await client.get("/stock/levels", params=params)

        assert response.status_code == 422
        assert isinstance(response.json()["detail"], str)
        assert response.headers[ERROR_CODE_HEADER] == "request_validation_error"

    @pytest.mark.parametrize(
        "dto",
        [_NestedQuery, _UnionQuery, _ListOfModelsQuery, _MappingQuery],
        ids=["model", "union-with-a-model", "list-of-models", "mapping"],
    )
    def test_a_field_a_query_cannot_carry_is_refused_when_attached(
        self, dto: type[BaseDTO]
    ) -> None:
        # FastAPI accepts a nested model on a query model at route creation and fails every
        # request; the attacher refuses it up front instead.
        with pytest.raises(CoreException, match=r"Field 'inner'") as caught:
            _app(dto)

        assert caught.value.kind.value == "configuration"

    def test_optional_literal_enum_and_set_scalars_are_carried(self) -> None:
        params = _app(_ScalarsQuery).openapi()["paths"]["/stock/levels"]["get"]["parameters"]

        assert {p["name"] for p in params} == {
            "sku",
            "kind",
            "mode",
            "since",
            "owner",
            "limit",
            "tags",
        }

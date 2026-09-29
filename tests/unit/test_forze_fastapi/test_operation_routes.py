"""Hand-written operation routes: an app's own binding table through ``attach_operation_routes``.

A route an attacher does not ship (``stock.levels``, ``stock.add``) is a binding in the app's
table, not a hand-written endpoint: the schema, the ``operation_id`` and the dispatch come from
the catalog. ``query_endpoint`` covers the GET whose input arrives as query parameters.
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Annotated, Any, Literal, NewType
from uuid import UUID, uuid4

import attrs
import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import AwareDatetime as PydanticAwareDatetime
from pydantic import Field, HttpUrl

from forze.application.contracts.execution import Handler
from forze.application.execution.operations import OperationDescriptor, OperationRegistry
from forze.base.exceptions import CoreException
from forze.base.primitives import AwareDatetime, StrKeyNamespace
from forze.domain.models import BaseDTO
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.routes import (
    EndpointBuilder,
    RouteBinding,
    attach_operation_routes,
    body_endpoint,
    id_endpoint,
    id_rev_body_endpoint,
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


_Sku = NewType("_Sku", str)
type _Code = str
type _Tags = list[str]


class _CarriedQuery(BaseDTO):
    sku: _Sku
    code: _Code = "c"
    tags: list[_Code] = []
    at: PydanticAwareDatetime | None = None
    ours: AwareDatetime | None = None
    home: HttpUrl | None = None
    pair: tuple[str, ...] = ()
    size: Annotated[int, Field(ge=1)] | None = None


class _NestedListQuery(BaseDTO):
    inner: list[list[int]] = []


class _AliasedListQuery(BaseDTO):
    # FastAPI reads an alias as one value, so an alias of a list would fail every request.
    inner: _Tags = []


class _UnresolvedQuery(BaseDTO):
    inner: _NeverDefined | None = None  # noqa: F821


class _IntId(BaseDTO):
    id: int


class _StrId(BaseDTO):
    id: str


class _Rename(BaseDTO):
    name: str | None = None
    enabled: bool = True


class _IntIdUpdate(BaseDTO):
    id: int
    rev: int
    dto: _Rename


class _Filter(BaseDTO):
    sku: str
    limit: int = 10
    tags: list[str] = []


@attrs.define(slots=True, kw_only=True)
class _Seen(Handler[Any, Any]):
    """What reached the operation: the values, and which of them the caller set."""

    async def __call__(self, args: Any) -> Any:
        return {"dump": args.model_dump(mode="json"), "set": sorted(args.model_fields_set)}


def _one(
    input_type: type[BaseDTO],
    *,
    build: EndpointBuilder,
    path: str,
    method: str = "GET",
    bindings: dict[str, RouteBinding] | None = None,
    **attach: Any,
) -> FastAPI:
    """One operation, ``stock.one``, echoing what reached it."""

    router = APIRouter(prefix="/stock")
    attach_operation_routes(
        router,
        registry=OperationRegistry(
            handlers={STOCK.key("one"): lambda _c: _Seen()},
            descriptors={
                STOCK.key("one"): OperationDescriptor(input_type=input_type, output_type=None)
            },
        ).freeze(),
        ns=STOCK,
        ctx_dep=lambda: context_from_modules(MockDepsModule()),
        bindings=bindings or {"one": RouteBinding(method=method, path=path, build=build)},
        **attach,
    )
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return app


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

    @pytest.mark.parametrize(
        "dto",
        [_NestedListQuery, _AliasedListQuery],
        ids=["list-of-lists", "alias-of-a-list"],
    )
    def test_a_nested_or_aliased_sequence_is_refused(self, dto: type[BaseDTO]) -> None:
        with pytest.raises(CoreException, match=r"Field 'inner'"):
            _app(dto)

    def test_aliases_newtypes_and_string_parsed_scalars_are_carried(self) -> None:
        params = _app(_CarriedQuery).openapi()["paths"]["/stock/levels"]["get"]["parameters"]

        assert {p["name"] for p in params} == {
            "sku",
            "code",
            "tags",
            "at",
            "ours",
            "home",
            "pair",
            "size",
        }

    async def test_the_carried_scalars_parse_from_the_query(self) -> None:
        async with _client(_one(_CarriedQuery, build=query_endpoint, path="/one")) as client:
            response = await client.get(
                "/stock/one",
                params=[
                    ("sku", "a-1"),
                    ("tags", "x"),
                    ("ours", "2026-01-01T10:00:00+02:00"),
                    ("pair", "p"),
                    ("pair", "q"),
                    ("size", "3"),
                ],
            )

        assert response.status_code == 200
        assert response.json()["dump"]["pair"] == ["p", "q"]
        assert response.json()["dump"]["ours"] == "2026-01-01T10:00:00+02:00"

    def test_an_unresolved_forward_reference_says_so(self) -> None:
        with pytest.raises(CoreException, match=r"forward reference") as caught:
            _app(_UnresolvedQuery)

        assert caught.value.kind.value == "configuration"

    async def test_only_the_parameters_sent_count_as_set(self) -> None:
        # A patch encoder writes only what the caller set; a query route must not report
        # every defaulted field as set, where the same DTO through a body route would not.
        query = _one(_Filter, build=query_endpoint, path="/one")
        body = _one(_Filter, build=body_endpoint, path="/one", method="POST")

        async with _client(query) as client:
            from_query = (await client.get("/stock/one", params={"sku": "a-1"})).json()

        async with _client(body) as client:
            from_body = (await client.post("/stock/one", json={"sku": "a-1"})).json()

        assert from_query == from_body
        assert from_query["set"] == ["sku"]


# ....................... #


class TestTheIdRoutes:
    @pytest.mark.parametrize(
        ("dto", "raw", "value"),
        [(_IntId, "5", 5), (_StrId, "a-1", "a-1")],
        ids=["int-id", "str-id"],
    )
    async def test_the_id_takes_the_dtos_own_type(
        self, dto: type[BaseDTO], raw: str, value: Any
    ) -> None:
        async with _client(_one(dto, build=id_endpoint, path="/{id}")) as client:
            response = await client.get(f"/stock/{raw}")

        assert response.status_code == 200
        assert response.json()["dump"] == {"id": value}

    async def test_an_update_takes_the_dtos_own_id_type(self) -> None:
        app = _one(_IntIdUpdate, build=id_rev_body_endpoint, path="/{id}", method="PATCH")

        async with _client(app) as client:
            response = await client.patch("/stock/7", params={"rev": 2}, json={"name": "n"})

        assert response.status_code == 200
        assert response.json()["dump"]["id"] == 7

    def test_a_document_route_keeps_its_uuid_id_and_int_rev(self) -> None:
        params = _one(
            _IntIdUpdate, build=id_rev_body_endpoint, path="/{id}", method="PATCH"
        ).openapi()["paths"]["/stock/{id}"]["patch"]["parameters"]
        by_name = {p["name"]: p["schema"] for p in params}

        assert by_name["id"]["type"] == "integer"

        uuid_params = _app().openapi()["paths"]["/stock/{id}"]["get"]["parameters"]
        assert uuid_params[0]["schema"] == {"type": "string", "format": "uuid", "title": "Id"}

    def test_an_id_outside_the_path_is_a_query_parameter(self) -> None:
        # The RPC-style document routes rely on it: ``GET /notes.get?id=``.
        params = _one(_ById, build=id_endpoint, path="/one").openapi()["paths"]["/stock/one"][
            "get"
        ]["parameters"]

        assert [(p["name"], p["in"]) for p in params] == [("id", "query")]


# ....................... #


class TestTheBindingIsChecked:
    @pytest.mark.parametrize(
        ("build", "path"),
        [
            (query_endpoint, "/{sku}/levels"),
            (body_endpoint, "/{sku}"),
            (id_endpoint, "/{id}/{other}"),
        ],
        ids=["query-with-a-placeholder", "body-with-a-placeholder", "id-with-an-extra"],
    )
    def test_a_placeholder_the_builder_does_not_fill_is_refused(
        self, build: EndpointBuilder, path: str
    ) -> None:
        with pytest.raises(CoreException, match=r"placeholder") as caught:
            _one(_Filter if build is not id_endpoint else _ById, build=build, path=path)

        assert caught.value.kind.value == "configuration"

    def test_a_builder_that_declares_nothing_is_not_checked(self) -> None:
        def custom(runner: Any, input_type: Any, op: str) -> Any:
            async def endpoint(sku: str) -> Any:
                return await runner(_Filter(sku=sku))

            return endpoint

        app = _one(_Filter, build=custom, path="/{sku}")

        assert "/stock/{sku}" in app.openapi()["paths"]

    def test_a_binding_for_an_unregistered_operation_is_refused(self) -> None:
        bindings = {
            "one": RouteBinding(method="GET", path="/one", build=query_endpoint),
            "levles": RouteBinding(method="GET", path="/levles", build=query_endpoint),
        }

        with pytest.raises(CoreException, match=r"stock\.levles") as caught:
            _one(_Filter, build=query_endpoint, path="", bindings=bindings)

        assert caught.value.kind.value == "configuration"

    def test_an_attacher_may_skip_unregistered_operations(self) -> None:
        bindings = {
            "one": RouteBinding(method="GET", path="/one", build=query_endpoint),
            "absent": RouteBinding(method="GET", path="/absent", build=query_endpoint),
        }

        app = _one(_Filter, build=query_endpoint, path="", bindings=bindings, skip_unregistered=True)

        assert set(app.openapi()["paths"]) == {"/stock/one"}

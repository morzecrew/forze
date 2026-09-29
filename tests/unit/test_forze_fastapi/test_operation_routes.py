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
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    FutureDate,
    FutureDatetime,
    HttpUrl,
    NaiveDatetime,
    PastDate,
    PastDatetime,
    PrivateAttr,
    model_validator,
)
from pydantic.alias_generators import to_camel

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.execution import Handler
from forze.application.contracts.search import SearchSpec
from forze.application.execution.operations import OperationDescriptor, OperationRegistry
from forze.base.exceptions import CoreException
from forze.base.primitives import AwareDatetime, StrKeyNamespace
from forze.domain.models import BaseDTO, ReadDocument
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.routes import (
    EndpointBuilder,
    RouteBinding,
    attach_operation_routes,
    attach_search_routes,
    attach_tenancy_admin_routes,
    attach_tenancy_routes,
    body_endpoint,
    id_endpoint,
    id_rev_body_endpoint,
    id_rev_endpoint,
    query_endpoint,
)
from forze_kits.aggregates.search import build_search_registry
from forze_kits.aggregates.tenancy import build_tenancy_registry
from forze_kits.aggregates.tenancy_admin import build_tenancy_admin_registry
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

pytestmark = pytest.mark.unit

# ----------------------- #

STOCK = StrKeyNamespace(prefix="stock")
_AUTHN = AuthnSpec(name="main", enabled_methods=frozenset({"token"}))
_AUTHN_NS = _AUTHN.default_namespace


def _search_registry() -> Any:
    class _Hit(ReadDocument):
        title: str

    return build_search_registry(SearchSpec(name="notes", model_type=_Hit, fields=["title"]))


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
    naive: NaiveDatetime | None = None
    past: PastDate | None = None
    later: FutureDate | None = None
    was: PastDatetime | None = None
    will: FutureDatetime | None = None


class _NestedListQuery(BaseDTO):
    inner: list[list[int]] = []


class _AliasedListQuery(BaseDTO):
    # FastAPI reads an alias as one value, so an alias of a list would fail every request.
    inner: _Tags = []


class _BytesQuery(BaseDTO):
    # A query value is text: ``%FF`` arrives as a replacement character, not the byte.
    inner: bytes = b""


class _UnresolvedQuery(BaseDTO):
    inner: _NeverDefined | None = None  # noqa: F821


class _IntId(BaseDTO):
    id: int


class _StrId(BaseDTO):
    id: str


class _ListId(BaseDTO):
    id: list[int]


class _OptionalRev(BaseDTO):
    id: int
    rev: int | None = None


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


class _Repeated(BaseDTO):
    tag_list: list[str] = Field(default_factory=list, alias="tag")
    maybe: list[str] | None = None
    code: list[str] = Field(default_factory=list, validation_alias="c")
    picked: list[str] = Field(
        default_factory=list, alias="pick", validation_alias=AliasChoices("pick", "p")
    )
    sku: str = ""


class _AliasedFilter(BaseDTO):
    sku_code: str = Field(alias="skuCode")
    limit: int = 10


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
        [_NestedQuery, _UnionQuery, _ListOfModelsQuery, _MappingQuery, _BytesQuery],
        ids=["model", "union-with-a-model", "list-of-models", "mapping", "bytes"],
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
            "naive",
            "past",
            "later",
            "was",
            "will",
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

    async def test_each_parameter_is_read_as_fastapi_reads_it(self) -> None:
        # A list read from its alias or validation alias, or behind an Optional, collects every
        # value even when one is sent; a repeated scalar takes the last, as FastAPI's own
        # model does.
        async with _client(_one(_Repeated, build=query_endpoint, path="/one")) as client:
            seen = (
                await client.get(
                    "/stock/one",
                    params=[
                        ("tag", "x"),
                        ("maybe", "y"),
                        ("c", "z"),
                        ("pick", "w"),
                        ("sku", "a"),
                        ("sku", "b"),
                    ],
                )
            ).json()

        assert seen["dump"] == {
            "tag_list": ["x"],
            "maybe": ["y"],
            "code": ["z"],
            "picked": ["w"],
            "sku": "b",
        }

    async def test_a_parameter_sent_by_its_alias_counts_as_set(self) -> None:
        async with _client(_one(_AliasedFilter, build=query_endpoint, path="/one")) as client:
            seen = (await client.get("/stock/one", params={"skuCode": "a-1"})).json()

        assert seen["set"] == ["sku_code"]


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

    def test_the_published_parameters_take_the_dtos_types(self) -> None:
        params = _one(
            _IntIdUpdate, build=id_rev_body_endpoint, path="/{id}", method="PATCH"
        ).openapi()["paths"]["/stock/{id}"]["patch"]["parameters"]
        by_name = {p["name"]: p["schema"] for p in params}

        assert by_name["id"]["type"] == "integer"
        assert by_name["rev"]["type"] == "integer"

        uuid_params = _app().openapi()["paths"]["/stock/{id}"]["get"]["parameters"]
        assert uuid_params[0]["schema"] == {"type": "string", "format": "uuid", "title": "Id"}

    def test_an_id_that_is_not_one_value_is_refused(self) -> None:
        with pytest.raises(CoreException, match=r"single path or query value") as caught:
            _one(_ListId, build=id_endpoint, path="/{id}")

        assert caught.value.kind.value == "configuration"

    async def test_an_optional_rev_is_published_and_read_as_optional(self) -> None:
        app = _one(_OptionalRev, build=id_rev_endpoint, path="/{id}")
        params = app.openapi()["paths"]["/stock/{id}"]["get"]["parameters"]

        assert {p["name"]: p["required"] for p in params} == {"id": True, "rev": False}

        async with _client(app) as client:
            response = await client.get("/stock/5")

        assert response.status_code == 200
        assert response.json()["dump"] == {"id": 5, "rev": None}

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

    @pytest.mark.parametrize(
        ("attach", "full"),
        [
            (attach_search_routes, lambda: _search_registry()),
            (attach_tenancy_routes, lambda: build_tenancy_registry(_AUTHN)),
            (attach_tenancy_admin_routes, lambda: build_tenancy_admin_registry(_AUTHN_NS)),
        ],
        ids=["search", "tenancy", "tenancy-admin"],
    )
    def test_a_shipped_attacher_skips_what_its_registry_omits(
        self, attach: Any, full: Any
    ) -> None:
        catalog = full().freeze().catalog()
        kept = sorted(map(str, catalog))[0]
        registry = OperationRegistry(
            handlers={kept: lambda _c: _Seen()},
            descriptors={kept: catalog[kept].descriptor},
        ).freeze()
        ns = StrKeyNamespace(prefix=kept.rsplit(".", 1)[0])
        router = APIRouter()

        attach(router, registry=registry, ns=ns, ctx_dep=lambda: None)

        assert [route.name for route in router.routes] == [kept]  # type: ignore[attr-defined]


# ....................... #


class _ExtraAliased(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    sku_code: str = Field("NON", alias="skuCode", pattern=r"^[A-Z]{3}$")
    limit: int = Field(10, alias="lim")


class _Collide(BaseDTO):
    a: int = Field(0, alias="b_alias")
    b_alias: int = Field(7, alias="zz")


class _Camel(BaseDTO):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, frozen=True)

    sku_code: str = "x"
    max_qty: int = 5


class _Filled(BaseModel):
    a: int = 0
    b: int = 0

    @model_validator(mode="after")
    def _fill(self) -> _Filled:
        if self.b == 0:
            self.b = self.a * 2

        return self


class _Private(BaseDTO):
    a: int = 0
    _norm: str = PrivateAttr(default="unset")

    @model_validator(mode="after")
    def _normalize(self) -> _Private:
        self._norm = f"norm-{self.a}"
        return self


_RECEIVED: list[Any] = []


@attrs.define(slots=True, kw_only=True)
class _Record(Handler[Any, Any]):
    async def __call__(self, args: Any) -> Any:
        _RECEIVED.append(args)
        return None


async def _twin(dto: type[BaseModel], query: list[tuple[str, str]], body: dict[str, Any]) -> Any:
    """What one operation receives from a query route and from a body route, same DTO."""

    router = APIRouter()
    descriptor = OperationDescriptor(input_type=dto, output_type=None)
    attach_operation_routes(
        router,
        registry=OperationRegistry(
            handlers={STOCK.key("q"): lambda _c: _Record(), STOCK.key("b"): lambda _c: _Record()},
            descriptors={STOCK.key("q"): descriptor, STOCK.key("b"): descriptor},
        ).freeze(),
        ns=STOCK,
        ctx_dep=lambda: context_from_modules(MockDepsModule()),
        bindings={
            "q": RouteBinding(method="GET", path="/q", build=query_endpoint),
            "b": RouteBinding(method="POST", path="/b", build=body_endpoint),
        },
    )
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    _RECEIVED.clear()

    async with _client(app) as client:
        from_query = await client.get("/q", params=query)
        from_body = await client.post("/b", json=body)

    assert from_query.status_code == from_body.status_code == 200, (
        from_query.text,
        from_body.text,
    )

    return _RECEIVED


class TestTheQueryRouteBuildsWhatABodyWould:
    """The operation receives the same value from a query route as from a body route."""

    @pytest.mark.parametrize(
        ("dto", "query", "body"),
        [
            (
                _ExtraAliased,
                [("skuCode", "ABC"), ("sku_code", "not-valid-at-all"), ("limit", "abc")],
                {"skuCode": "ABC", "sku_code": "not-valid-at-all", "limit": "abc"},
            ),
            (_Collide, [("b_alias", "3"), ("zz", "9")], {"b_alias": "3", "zz": "9"}),
            (_Camel, [("sku_code", "q"), ("maxQty", "2")], {"sku_code": "q", "maxQty": "2"}),
            (_Filter, [("sku", "a"), ("tags", "x"), ("tags", "y")], {"sku": "a", "tags": ["x", "y"]}),
            (_Filled, [("a", "3")], {"a": "3"}),
            (_Private, [("a", "3")], {"a": "3"}),
        ],
        ids=["extra-and-alias", "alias-collision", "populate-by-name", "list", "filled", "private"],
    )
    async def test_the_same_value_arrives(
        self, dto: type[BaseModel], query: list[tuple[str, str]], body: dict[str, Any]
    ) -> None:
        from_query, from_body = await _twin(dto, query, body)

        assert from_query == from_body
        assert from_query.model_dump() == from_body.model_dump()
        assert from_query.model_dump(exclude_unset=True) == from_body.model_dump(exclude_unset=True)
        assert from_query.model_fields_set == from_body.model_fields_set
        assert from_query.__pydantic_private__ == from_body.__pydantic_private__
        assert from_query.model_extra == from_body.model_extra

    async def test_a_field_sent_by_its_name_stays_validated(self) -> None:
        # ``sku_code`` is read from ``skuCode``; under its own name it is an extra, and never
        # replaces the validated value.
        from_query, _ = await _twin(
            _ExtraAliased,
            [("skuCode", "ABC"), ("sku_code", "not-valid-at-all")],
            {"skuCode": "ABC", "sku_code": "not-valid-at-all"},
        )

        assert from_query.sku_code == "ABC"
        assert from_query.model_extra == {"sku_code": "not-valid-at-all"}

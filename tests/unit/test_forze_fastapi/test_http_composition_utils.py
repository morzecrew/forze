"""Tests for HTTP composition helpers (utils, attach, signature)."""

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import APIRouter
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field

from forze.application.execution import Deps, ExecutionContext, FacadeOpRef, UsecaseRegistry
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO
from forze_fastapi.endpoints.http import BodyAsIsMapper, EmptyMapper
from forze_fastapi.endpoints.http.composition.attach import (
    _has_route,
    _join_paths,
    attach_http_endpoint,
    attach_http_endpoints,
)
from forze_fastapi.endpoints.http.composition.signature import build_http_endpoint_signature
from forze_fastapi.endpoints.http.composition.utils import (
    build_body_parameters,
    build_dependency_parameter,
    build_form_parameter,
    common_kwargs,
    default_from_field,
    header_from_field,
    iter_model_field_names,
    model_from_kwargs,
    path_from_field,
    query_from_field,
    snake_to_header,
    validate_http_param_name_conflicts,
)
from forze_fastapi.endpoints.http.contracts import HTTP_BODY_KEY, HTTP_REQUEST_KEY
from forze_fastapi.endpoints.http.contracts.specs import HttpEndpointSpec
from forze_fastapi.endpoints.http.features.etag import ETagFeature
from forze_fastapi.endpoints.http.features.etag.constants import IF_NONE_MATCH_HEADER_KEY

# ----------------------- #


class TestIterModelFieldNames:
    def test_none_yields_empty(self) -> None:
        assert list(iter_model_field_names(None)) == []

    def test_model_keys(self) -> None:
        class M(BaseModel):
            a: int
            b: str

        assert set(iter_model_field_names(M)) == {"a", "b"}


class TestDefaultFromField:
    def test_required_override_ellipsis(self) -> None:
        f = Field(description="x")
        assert default_from_field(f, required_override=True) is ...

    def test_optional_override_returns_default(self) -> None:
        f = Field(default=7)
        assert default_from_field(f, required_override=False) == 7

    def test_optional_override_no_default_returns_none(self) -> None:
        class M(BaseModel):
            y: int

        assert default_from_field(M.model_fields["y"], required_override=False) is None

    def test_no_override_returns_default(self) -> None:
        f = Field(default="z")
        assert default_from_field(f) == "z"

    def test_no_default_returns_ellipsis(self) -> None:
        class M(BaseModel):
            z: int

        assert default_from_field(M.model_fields["z"]) is ...


class TestCommonKwargs:
    def test_extracts_schema_extra_deprecated(self) -> None:
        f = Field(json_schema_extra={"deprecated": True})
        k = common_kwargs(f)
        assert k["deprecated"] is True


class TestModelFromKwargs:
    def test_none_model(self) -> None:
        assert model_from_kwargs(model_type=None, kwargs={"x": 1}) is None

    def test_partial_assigns(self) -> None:
        class M(BaseModel):
            a: int = 0
            b: str = "n"

        m = model_from_kwargs(model_type=M, kwargs={"a": 3})
        assert m is not None
        assert m.a == 3
        assert m.b == "n"


class TestValidateHttpParamNameConflicts:
    def test_duplicate_path_query(self) -> None:
        class P(BaseModel):
            item_id: str

        class Q(BaseModel):
            item_id: str

        with pytest.raises(CoreError, match="conflicts detected"):
            validate_http_param_name_conflicts(
                path_model=P,
                query_model=Q,
                body_mode="json",
            )

    def test_reserved_request_name(self) -> None:
        class P(BaseModel):
            request: str

        with pytest.raises(CoreError, match="reserved"):
            validate_http_param_name_conflicts(
                path_model=P,
                query_model=None,
                body_mode="json",
            )

    def test_form_body_overlaps_path(self) -> None:
        class P(BaseModel):
            name: str

        class B(BaseModel):
            name: str

        with pytest.raises(CoreError, match="conflicts detected"):
            validate_http_param_name_conflicts(
                path_model=P,
                query_model=None,
                body_model=B,
                body_mode="form",
            )


class TestSnakeToHeader:
    def test_joins_capitalized_parts(self) -> None:
        assert snake_to_header("if_none_match") == "If-None-Match"


class TestBuildParameters:
    def test_query_path_header_form_smoke(self) -> None:
        class M(BaseModel):
            x: int = Field(ge=1)

        f = M.model_fields["x"]
        assert query_from_field(f) is not None
        assert path_from_field(f) is not None
        assert header_from_field(f, "if_none_match") is not None
        assert build_form_parameter("x", f) is not None

    def test_build_body_json_vs_form(self) -> None:
        class B(BaseModel):
            title: str

        json_params = build_body_parameters(B, "json")
        assert len(json_params) == 1
        assert json_params[0].name == HTTP_BODY_KEY

        form_params = build_body_parameters(B, "form")
        assert len(form_params) == 1
        assert form_params[0].name == "title"

    def test_build_dependency_parameter(self) -> None:
        def dep() -> int:
            return 1

        p = build_dependency_parameter("d", int, dep)
        assert p.name == "d"
        assert p.annotation is int


class TestJoinPathsAndHasRoute:
    def test_join_paths(self) -> None:
        assert _join_paths("", "/a") == "/a"
        assert _join_paths("/api", "x") == "/api/x"
        assert _join_paths("/api/", "/x") == "/api/x"
        assert _join_paths("", "") == ""

    def test_has_route(self) -> None:
        r = APIRouter(prefix="/v1")

        @r.get("/items")
        async def _items() -> None:
            return None

        assert _has_route(r, path="/items", method="GET") is True
        assert _has_route(r, path="/missing", method="GET") is False


class TestBuildHttpEndpointSignatureEtag:
    def test_if_none_match_header_param(self) -> None:
        class Facade:
            pass

        spec = HttpEndpointSpec(
            http={"method": "GET", "path": "/r"},
            features=[ETagFeature(provider=lambda _b: '"t"')],
            request=None,
            response=None,
            mapper=EmptyMapper(),
            facade_type=Facade,
            call=FacadeOpRef(op="test.read"),
        )

        def ctx_dep() -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        def facade_dep(_ctx: ExecutionContext) -> Facade:
            return Facade()

        sig = build_http_endpoint_signature(
            spec=spec,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
        )
        inm = [p for p in sig.parameters.values() if p.name == "__if_none_match"]
        assert len(inm) == 1
        assert getattr(inm[0].default, "alias", None) == IF_NONE_MATCH_HEADER_KEY


def _minimal_get_spec(
    *,
    path: str = "/ping",
    metadata: dict[str, str] | None = None,
    features: Any = None,
) -> HttpEndpointSpec[Any, Any, Any, Any, Any, Any, Any, Any]:
    class Facade:
        pass

    return HttpEndpointSpec(
        http={"method": "GET", "path": path},
        metadata=metadata,
        features=features,
        request=None,
        response=None,
        mapper=EmptyMapper(),
        facade_type=Facade,
        call=FacadeOpRef(op="ns.op"),
    )


class TestAttachHttpEndpoint:
    def test_duplicate_route_raises(self) -> None:
        router = APIRouter()

        @router.get("/ping")
        async def _existing() -> None:
            return None

        reg = MagicMock(spec=UsecaseRegistry)
        reg.qualify_operation = MagicMock(return_value="ns.ping")

        spec = _minimal_get_spec(path="/ping")

        def ctx_dep() -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        with pytest.raises(CoreError, match="Route already exists"):
            attach_http_endpoint(
                router,
                spec=spec,
                registry=reg,
                ctx_dep=ctx_dep,
            )

    def test_sets_docstring_from_metadata(self) -> None:
        router = APIRouter()
        reg = MagicMock(spec=UsecaseRegistry)
        reg.qualify_operation = MagicMock(return_value="ns.ping")

        spec = _minimal_get_spec(
            metadata={"description": "## Hello"},
        )

        def ctx_dep() -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        attach_http_endpoint(
            router,
            spec=spec,
            registry=reg,
            ctx_dep=ctx_dep,
        )
        route = next(
            x for x in router.routes if isinstance(x, APIRoute) and x.path == "/ping"
        )
        assert route.endpoint.__doc__ == "## Hello"

    def test_attach_http_endpoints_iteration(self) -> None:
        router = APIRouter()
        reg = MagicMock(spec=UsecaseRegistry)
        reg.qualify_operation = MagicMock(side_effect=["ns.a", "ns.b"])

        class Facade:
            pass

        s1 = HttpEndpointSpec(
            http={"method": "GET", "path": "/a"},
            mapper=EmptyMapper(),
            facade_type=Facade,
            call=FacadeOpRef(op="a"),
        )
        s2 = HttpEndpointSpec(
            http={"method": "GET", "path": "/b"},
            mapper=EmptyMapper(),
            facade_type=Facade,
            call=FacadeOpRef(op="b"),
        )

        def ctx_dep() -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        attach_http_endpoints(router, specs=(s1, s2), registry=reg, ctx_dep=ctx_dep)
        paths = {r.path for r in router.routes if isinstance(r, APIRoute)}
        assert paths == {"/a", "/b"}


class TestSignaturePathQueryHeaderCookie:
    def test_builds_params_for_all_request_parts(self) -> None:
        class Q(BaseModel):
            q: str

        class P(BaseModel):
            item_id: str

        class H(BaseModel):
            trace: str | None = None

        class C(BaseModel):
            sid: str | None = None

        class B(BaseModel):
            title: str

        class In(BaseDTO):
            title: str

        class Facade:
            pass

        spec = HttpEndpointSpec(
            http={"method": "POST", "path": "/items/{item_id}"},
            request={
                "query_type": Q,
                "path_type": P,
                "header_type": H,
                "cookie_type": C,
                "body_type": B,
                "body_mode": "json",
            },
            response=None,
            mapper=BodyAsIsMapper(out=In),
            facade_type=Facade,
            call=FacadeOpRef(op="x.create"),
        )

        def ctx_dep() -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        def facade_dep(_ctx: ExecutionContext) -> Facade:
            return Facade()

        sig = build_http_endpoint_signature(
            spec=spec,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
        )
        names = {p.name for p in sig.parameters.values()}
        assert {"q", "item_id", "trace", "sid", HTTP_BODY_KEY, HTTP_REQUEST_KEY}.issubset(
            names
        )

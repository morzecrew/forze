"""Unit tests for forze_fastapi.routing.routes.feature (composable route features)."""

import pytest

from collections.abc import Sequence

from fastapi import FastAPI, Request, Response
from fastapi.params import Depends
from fastapi.routing import APIRoute
from starlette.testclient import TestClient

from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze_fastapi.routing.routes.etag import ETagFeature, ETagRouteConfig
from forze_fastapi.routing.routes.feature import (
    RouteFeature,
    RouteHandler,
    compose_route_class,
)
from forze_fastapi.routing.router import ForzeAPIRouter


# ----------------------- #


def _ctx_factory() -> ExecutionContext:
    return ExecutionContext()


class _UppercaseFeature:
    """Test feature that uppercases response body."""

    def wrap(self, handler: RouteHandler) -> RouteHandler:
        async def wrapped(request: Request) -> Response:
            resp = await handler(request)
            body = getattr(resp, "body", b"")
            if isinstance(body, (bytes, bytearray)):
                upper = body.upper()
                return Response(
                    content=upper,
                    status_code=resp.status_code,
                    headers=dict(resp.headers),
                    media_type=resp.media_type,
                )
            return resp

        return wrapped

    @property
    def extra_dependencies(self) -> Sequence[Depends]:
        return ()


class _HeaderFeature:
    """Test feature that adds a custom header."""

    def __init__(self, name: str, value: str) -> None:
        self._name = name
        self._value = value

    def wrap(self, handler: RouteHandler) -> RouteHandler:
        name, value = self._name, self._value

        async def wrapped(request: Request) -> Response:
            resp = await handler(request)
            resp.headers[name] = value
            return resp

        return wrapped

    @property
    def extra_dependencies(self) -> Sequence[Depends]:
        return ()


class _FixedProvider:
    """ETag provider that always returns a fixed tag."""

    def __init__(self, tag: str) -> None:
        self._tag = tag

    def generate(self, response_body: bytes) -> str | None:
        return self._tag


# ----------------------- #


class TestRouteFeatureProtocol:
    """Tests for RouteFeature protocol conformance."""

    def test_uppercase_feature_satisfies_protocol(self) -> None:
        """_UppercaseFeature satisfies the RouteFeature protocol."""
        feature = _UppercaseFeature()
        assert isinstance(feature, RouteFeature)

    def test_header_feature_satisfies_protocol(self) -> None:
        """_HeaderFeature satisfies the RouteFeature protocol."""
        feature = _HeaderFeature("X-Custom", "test")
        assert isinstance(feature, RouteFeature)

    def test_etag_feature_satisfies_protocol(self) -> None:
        """ETagFeature satisfies the RouteFeature protocol."""
        cfg = ETagRouteConfig(provider=_FixedProvider("v1"), auto_304=True)
        feature = ETagFeature(config=cfg)
        assert isinstance(feature, RouteFeature)


class TestComposeRouteClass:
    """Tests for compose_route_class factory."""

    def test_returns_api_route_subclass(self) -> None:
        """Factory returns a subclass of APIRoute."""
        cls = compose_route_class(_UppercaseFeature())
        assert issubclass(cls, APIRoute)

    def test_custom_base_class(self) -> None:
        """Factory respects a custom base class."""

        class CustomRoute(APIRoute):
            pass

        cls = compose_route_class(_UppercaseFeature(), base=CustomRoute)
        assert issubclass(cls, CustomRoute)

    def test_single_feature_wraps_handler(self) -> None:
        """Single feature wraps the route handler."""
        from fastapi.routing import APIRouter

        app = FastAPI()
        router = APIRouter()
        route_cls = compose_route_class(_HeaderFeature("X-Feature", "active"))

        async def endpoint() -> dict:
            return {"hello": "world"}

        router.add_api_route(
            "/test", endpoint, methods=["GET"], route_class_override=route_cls
        )
        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/test")

        assert resp.status_code == 200
        assert resp.headers.get("x-feature") == "active"

    def test_multiple_features_compose_in_order(self) -> None:
        """Features are applied outermost-first: first feature wraps last."""
        from fastapi.routing import APIRouter

        app = FastAPI()
        router = APIRouter()
        route_cls = compose_route_class(
            _HeaderFeature("X-Outer", "1"),
            _HeaderFeature("X-Inner", "2"),
        )

        async def endpoint() -> dict:
            return {"ok": True}

        router.add_api_route(
            "/test", endpoint, methods=["GET"], route_class_override=route_cls
        )
        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/test")

        assert resp.status_code == 200
        assert resp.headers.get("x-outer") == "1"
        assert resp.headers.get("x-inner") == "2"

    def test_etag_and_header_features_compose(self) -> None:
        """ETagFeature and a custom header feature can compose on one route."""
        from fastapi.routing import APIRouter

        app = FastAPI()
        router = APIRouter()
        etag_cfg = ETagRouteConfig(provider=_FixedProvider("composed"), auto_304=True)
        route_cls = compose_route_class(
            ETagFeature(config=etag_cfg),
            _HeaderFeature("X-Trace", "abc"),
        )

        async def endpoint() -> dict:
            return {"id": 1}

        router.add_api_route(
            "/test", endpoint, methods=["GET"], route_class_override=route_cls
        )
        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/test")

        assert resp.status_code == 200
        assert resp.headers.get("etag") == '"composed"'
        assert resp.headers.get("x-trace") == "abc"

    def test_etag_304_works_in_composition(self) -> None:
        """ETag 304 still works when composed with other features."""
        from fastapi.routing import APIRouter

        app = FastAPI()
        router = APIRouter()
        etag_cfg = ETagRouteConfig(provider=_FixedProvider("tag1"), auto_304=True)
        route_cls = compose_route_class(
            ETagFeature(config=etag_cfg),
            _HeaderFeature("X-Composed", "yes"),
        )

        async def endpoint() -> dict:
            return {"id": 1}

        router.add_api_route(
            "/test", endpoint, methods=["GET"], route_class_override=route_cls
        )
        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/test", headers={"If-None-Match": '"tag1"'})

        assert resp.status_code == 304


class TestForzeAPIRouterRouteFeatures:
    """Tests for route_features parameter on ForzeAPIRouter."""

    def test_get_with_custom_route_feature(self) -> None:
        """Custom route feature is applied to GET route."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get(
            "/items",
            route_features=[_HeaderFeature("X-Custom", "value")],
        )
        async def get_items() -> dict:
            return {"items": []}

        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/api/items")

        assert resp.status_code == 200
        assert resp.headers.get("x-custom") == "value"

    def test_post_with_custom_route_feature(self) -> None:
        """Custom route feature is applied to POST route."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.post(
            "/items",
            route_features=[_HeaderFeature("X-Source", "test")],
        )
        async def create_item() -> dict:
            return {"created": True}

        app.include_router(router)
        client = TestClient(app)
        resp = client.post("/api/items")

        assert resp.status_code == 200
        assert resp.headers.get("x-source") == "test"

    def test_etag_and_custom_feature_compose_via_router(self) -> None:
        """ETag flag and explicit route_features compose together."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get(
            "/item",
            etag=True,
            etag_config={"provider": _FixedProvider("router-composed")},
            route_features=[_HeaderFeature("X-Trace-Id", "abc-123")],
        )
        async def get_item() -> dict:
            return {"value": 42}

        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/api/item")

        assert resp.status_code == 200
        assert resp.headers.get("etag") == '"router-composed"'
        assert resp.headers.get("x-trace-id") == "abc-123"

    def test_multiple_custom_features_via_router(self) -> None:
        """Multiple custom features compose correctly via router."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get(
            "/item",
            route_features=[
                _HeaderFeature("X-A", "1"),
                _HeaderFeature("X-B", "2"),
            ],
        )
        async def get_item() -> dict:
            return {"value": 42}

        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/api/item")

        assert resp.status_code == 200
        assert resp.headers.get("x-a") == "1"
        assert resp.headers.get("x-b") == "2"

    def test_no_features_route_is_plain(self) -> None:
        """Route without features uses plain APIRoute."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get("/items")
        async def get_items() -> dict:
            return {"items": []}

        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/api/items")

        assert resp.status_code == 200
        assert "x-custom" not in resp.headers


class TestFeatureExtraDependencies:
    """Tests for feature.extra_dependencies injection."""

    def test_extra_dependencies_are_injected(self) -> None:
        """Feature's extra_dependencies are added to the route."""

        collected: list[str] = []

        async def tracking_dep() -> None:
            collected.append("called")

        class _DepFeature:
            def wrap(self, handler: RouteHandler) -> RouteHandler:
                return handler

            @property
            def extra_dependencies(self) -> Sequence[Depends]:
                return (Depends(tracking_dep),)

        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get(
            "/items",
            route_features=[_DepFeature()],
        )
        async def get_items() -> dict:
            return {"items": []}

        app.include_router(router)
        client = TestClient(app)
        resp = client.get("/api/items")

        assert resp.status_code == 200
        assert collected == ["called"]


class TestFeatureCompositionOrder:
    """Tests verifying the outermost-first composition order."""

    def test_outermost_feature_sees_request_first(self) -> None:
        """First feature in the list processes the request before others."""

        call_order: list[str] = []

        class _TrackingFeature:
            def __init__(self, name: str) -> None:
                self._name = name

            def wrap(self, handler: RouteHandler) -> RouteHandler:
                name = self._name

                async def wrapped(request: Request) -> Response:
                    call_order.append(f"{name}:before")
                    resp = await handler(request)
                    call_order.append(f"{name}:after")
                    return resp

                return wrapped

            @property
            def extra_dependencies(self) -> Sequence[Depends]:
                return ()

        from fastapi.routing import APIRouter

        app = FastAPI()
        router = APIRouter()
        route_cls = compose_route_class(
            _TrackingFeature("outer"),
            _TrackingFeature("inner"),
        )

        async def endpoint() -> dict:
            return {}

        router.add_api_route(
            "/test", endpoint, methods=["GET"], route_class_override=route_cls
        )
        app.include_router(router)
        client = TestClient(app)
        client.get("/test")

        assert call_order == [
            "outer:before",
            "inner:before",
            "inner:after",
            "outer:after",
        ]

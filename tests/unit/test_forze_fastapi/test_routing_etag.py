"""Unit tests for forze_fastapi.routing.routes.etag."""

import orjson
import pytest

from fastapi import FastAPI
from starlette.testclient import TestClient

from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze_fastapi.routers.document import DocumentETagProvider
from forze_fastapi.routing.routes.etag import (
    ETagProvider,
    ETagRoute,
    ETagRouteConfig,
    _ensure_quoted,
    _etag_matches,
    _normalize_for_comparison,
    make_etag_route_class,
)
from forze_fastapi.routing.router import (
    ForzeAPIRouter,
    RouteETagConfig,
    RouterETagConfig,
)


# ----------------------- #


def _ctx_factory() -> ExecutionContext:
    return ExecutionContext()


class _FixedProvider:
    """Provider that always returns a fixed tag."""

    def __init__(self, tag: str) -> None:
        self._tag = tag

    def generate(self, response_body: bytes) -> str | None:
        return self._tag


class _NoneProvider:
    """Provider that always returns None."""

    def generate(self, response_body: bytes) -> str | None:
        return None


# ----------------------- #


class TestEnsureQuoted:
    """Tests for _ensure_quoted helper."""

    def test_wraps_bare_value(self) -> None:
        """Bare values are wrapped in double-quotes."""
        assert _ensure_quoted("abc") == '"abc"'

    def test_preserves_already_quoted(self) -> None:
        """Already-quoted values are returned unchanged."""
        assert _ensure_quoted('"abc"') == '"abc"'

    def test_preserves_weak_etag(self) -> None:
        """Weak ETags starting with W/ are returned unchanged."""
        assert _ensure_quoted('W/"abc"') == 'W/"abc"'


class TestNormalizeForComparison:
    """Tests for _normalize_for_comparison helper."""

    def test_strips_weak_indicator(self) -> None:
        """Weak indicator W/ is removed for comparison."""
        assert _normalize_for_comparison('W/"abc"') == '"abc"'

    def test_strong_unchanged(self) -> None:
        """Strong ETag is returned unchanged."""
        assert _normalize_for_comparison('"abc"') == '"abc"'

    def test_strips_whitespace(self) -> None:
        """Surrounding whitespace is stripped."""
        assert _normalize_for_comparison('  "abc"  ') == '"abc"'


class TestETagMatches:
    """Tests for _etag_matches helper."""

    def test_star_matches_any(self) -> None:
        """Wildcard * matches any ETag."""
        assert _etag_matches('"abc"', "*") is True

    def test_exact_match(self) -> None:
        """Exact match returns True."""
        assert _etag_matches('"abc"', '"abc"') is True

    def test_no_match(self) -> None:
        """Non-matching returns False."""
        assert _etag_matches('"abc"', '"def"') is False

    def test_comma_separated_match(self) -> None:
        """Matching one tag in a comma-separated list returns True."""
        assert _etag_matches('"abc"', '"def", "abc", "ghi"') is True

    def test_comma_separated_no_match(self) -> None:
        """No match in a comma-separated list returns False."""
        assert _etag_matches('"abc"', '"def", "ghi"') is False

    def test_weak_comparison(self) -> None:
        """Weak and strong tags with same opaque value match."""
        assert _etag_matches('"abc"', 'W/"abc"') is True
        assert _etag_matches('W/"abc"', '"abc"') is True


class TestETagProviderProtocol:
    """Tests for ETagProvider protocol conformance."""

    def test_fixed_provider_satisfies_protocol(self) -> None:
        """_FixedProvider satisfies the ETagProvider protocol."""
        provider = _FixedProvider("v1")
        assert isinstance(provider, ETagProvider)

    def test_none_provider_satisfies_protocol(self) -> None:
        """_NoneProvider satisfies the ETagProvider protocol."""
        provider = _NoneProvider()
        assert isinstance(provider, ETagProvider)


class TestMakeETagRouteClass:
    """Tests for make_etag_route_class factory."""

    def test_returns_etag_route_subclass(self) -> None:
        """Factory returns a subclass of ETagRoute."""
        cls = make_etag_route_class(provider=_FixedProvider("v1"))
        assert issubclass(cls, ETagRoute)


def _make_etag_app(provider, auto_304=True):
    """Build a FastAPI app with a single ETag-enabled GET route."""

    from fastapi.routing import APIRouter

    app = FastAPI()
    router = APIRouter()
    route_class = make_etag_route_class(provider=provider, auto_304=auto_304)

    async def get_item() -> dict:
        return {"id": "123", "name": "test"}

    router.add_api_route(
        "/item", get_item, methods=["GET"], route_class_override=route_class
    )
    app.include_router(router)

    return app


class TestETagRouteIntegration:
    """Integration tests for ETag route handling via TestClient."""

    def test_etag_header_present_in_response(self) -> None:
        """Enabled route includes ETag header."""
        app = _make_etag_app(_FixedProvider("abc123"))
        client = TestClient(app)
        response = client.get("/item")

        assert response.status_code == 200
        assert response.headers.get("etag") == '"abc123"'

    def test_304_on_matching_if_none_match(self) -> None:
        """Matching If-None-Match results in 304 Not Modified."""
        app = _make_etag_app(_FixedProvider("abc123"))
        client = TestClient(app)
        response = client.get("/item", headers={"If-None-Match": '"abc123"'})

        assert response.status_code == 304
        assert response.headers.get("etag") == '"abc123"'

    def test_200_on_non_matching_if_none_match(self) -> None:
        """Non-matching If-None-Match returns normal 200 response."""
        app = _make_etag_app(_FixedProvider("abc123"))
        client = TestClient(app)
        response = client.get("/item", headers={"If-None-Match": '"old-tag"'})

        assert response.status_code == 200
        assert response.headers.get("etag") == '"abc123"'

    def test_auto_304_disabled_skips_conditional_check(self) -> None:
        """With auto_304=False, matching If-None-Match still returns 200."""
        app = _make_etag_app(_FixedProvider("abc123"), auto_304=False)
        client = TestClient(app)
        response = client.get("/item", headers={"If-None-Match": '"abc123"'})

        assert response.status_code == 200
        assert response.headers.get("etag") == '"abc123"'

    def test_none_provider_skips_etag(self) -> None:
        """When provider returns None, no ETag header is set."""
        app = _make_etag_app(_NoneProvider())
        client = TestClient(app)
        response = client.get("/item")

        assert response.status_code == 200
        assert "etag" not in response.headers

    def test_weak_etag_preserved(self) -> None:
        """Weak ETags starting with W/ are preserved correctly."""
        app = _make_etag_app(_FixedProvider('W/"abc"'))
        client = TestClient(app)
        response = client.get("/item")

        assert response.status_code == 200
        assert response.headers.get("etag") == 'W/"abc"'

    def test_304_with_weak_if_none_match(self) -> None:
        """Weak If-None-Match matches via weak comparison."""
        app = _make_etag_app(_FixedProvider("abc"))
        client = TestClient(app)
        response = client.get("/item", headers={"If-None-Match": 'W/"abc"'})

        assert response.status_code == 304

    def test_304_with_wildcard_if_none_match(self) -> None:
        """Wildcard If-None-Match: * results in 304."""
        app = _make_etag_app(_FixedProvider("abc"))
        client = TestClient(app)
        response = client.get("/item", headers={"If-None-Match": "*"})

        assert response.status_code == 304

    def test_304_with_comma_separated_if_none_match(self) -> None:
        """Matching one tag in comma-separated If-None-Match returns 304."""
        app = _make_etag_app(_FixedProvider("v2"))
        client = TestClient(app)
        response = client.get("/item", headers={"If-None-Match": '"v1", "v2", "v3"'})

        assert response.status_code == 304


class TestForzeAPIRouterETag:
    """Tests for ETag support in ForzeAPIRouter."""

    def test_get_with_etag_includes_header(self) -> None:
        """GET route with etag=True includes ETag header."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get(
            "/item",
            etag=True,
            etag_config={"provider": _FixedProvider("tag1")},
        )
        async def get_item() -> dict:
            return {"value": 42}

        app.include_router(router)
        client = TestClient(app)
        response = client.get("/api/item")

        assert response.status_code == 200
        assert response.headers.get("etag") == '"tag1"'

    def test_get_with_etag_304(self) -> None:
        """GET route returns 304 when If-None-Match matches."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get(
            "/item",
            etag=True,
            etag_config={"provider": _FixedProvider("tag1")},
        )
        async def get_item() -> dict:
            return {"value": 42}

        app.include_router(router)
        client = TestClient(app)
        response = client.get("/api/item", headers={"If-None-Match": '"tag1"'})

        assert response.status_code == 304

    def test_router_level_etag_config(self) -> None:
        """Router-level ETag config is used as default for routes."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
            etag_config={"provider": _FixedProvider("router-tag")},
        )

        @router.get("/item", etag=True)
        async def get_item() -> dict:
            return {"value": 42}

        app.include_router(router)
        client = TestClient(app)
        response = client.get("/api/item")

        assert response.status_code == 200
        assert response.headers.get("etag") == '"router-tag"'

    def test_route_level_overrides_router_config(self) -> None:
        """Route-level ETag config overrides router default."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
            etag_config={"provider": _FixedProvider("router-tag")},
        )

        @router.get(
            "/item",
            etag=True,
            etag_config={"provider": _FixedProvider("route-tag")},
        )
        async def get_item() -> dict:
            return {"value": 42}

        app.include_router(router)
        client = TestClient(app)
        response = client.get("/api/item")

        assert response.status_code == 200
        assert response.headers.get("etag") == '"route-tag"'

    def test_etag_requires_provider(self) -> None:
        """Enabling ETag without a provider raises CoreError."""
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        with pytest.raises(CoreError, match="ETag provider is required"):

            @router.get("/item", etag=True)
            async def get_item() -> dict:
                return {"value": 42}

    def test_non_etag_route_has_no_header(self) -> None:
        """Route without etag=True has no ETag header."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get("/item")
        async def get_item() -> dict:
            return {"value": 42}

        app.include_router(router)
        client = TestClient(app)
        response = client.get("/api/item")

        assert response.status_code == 200
        assert "etag" not in response.headers

    def test_route_auto_304_override(self) -> None:
        """Route-level auto_304=False overrides router default."""
        app = FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
            etag_config={
                "provider": _FixedProvider("tag1"),
                "auto_304": True,
            },
        )

        @router.get(
            "/item",
            etag=True,
            etag_config={"auto_304": False},
        )
        async def get_item() -> dict:
            return {"value": 42}

        app.include_router(router)
        client = TestClient(app)
        response = client.get("/api/item", headers={"If-None-Match": '"tag1"'})

        assert response.status_code == 200
        assert response.headers.get("etag") == '"tag1"'


class TestDocumentETagProvider:
    """Tests for DocumentETagProvider."""

    def test_generates_id_rev_tag(self) -> None:
        """Provider generates tag from id and rev fields."""
        provider = DocumentETagProvider()
        body = orjson.dumps({"id": "abc-123", "rev": 5, "name": "test"})
        tag = provider.generate(body)

        assert tag == "abc-123:5"

    def test_returns_none_for_missing_id(self) -> None:
        """Returns None when id field is missing."""
        provider = DocumentETagProvider()
        body = orjson.dumps({"rev": 5, "name": "test"})
        tag = provider.generate(body)

        assert tag is None

    def test_returns_none_for_missing_rev(self) -> None:
        """Returns None when rev field is missing."""
        provider = DocumentETagProvider()
        body = orjson.dumps({"id": "abc-123", "name": "test"})
        tag = provider.generate(body)

        assert tag is None

    def test_returns_none_for_invalid_json(self) -> None:
        """Returns None for non-JSON body."""
        provider = DocumentETagProvider()
        tag = provider.generate(b"not-json")

        assert tag is None

    def test_satisfies_etag_provider_protocol(self) -> None:
        """DocumentETagProvider satisfies the ETagProvider protocol."""
        provider = DocumentETagProvider()
        assert isinstance(provider, ETagProvider)

"""Unit tests for ETag helpers and the HTTP ETag feature."""

import orjson
import pytest
from fastapi import Response
from pydantic import BaseModel
from starlette.requests import Request

from forze_fastapi.endpoints.document.features import document_etag
from forze_fastapi.endpoints.http.contracts.context import HttpEndpointContext
from forze_fastapi.endpoints.http.contracts.specs import HttpEndpointSpec
from forze_fastapi.endpoints.http.features.etag import ETagFeature
from forze_fastapi.endpoints.http.features.etag.constants import IF_NONE_MATCH_HEADER_KEY
from forze_fastapi.endpoints.http.features.etag.ports import ETagProviderPort
from forze_fastapi.endpoints.http.features.etag.utils import (
    ensure_quoted_etag,
    etag_matches,
    normalize_etag_for_comparison,
)

# ----------------------- #


class _FixedProvider:
    """Callable provider that always returns a fixed tag."""

    def __init__(self, tag: str) -> None:
        self._tag = tag

    def __call__(self, response_body: bytes) -> str | None:
        return self._tag


class _NoneProvider:
    """Callable provider that always returns None."""

    def __call__(self, response_body: bytes) -> str | None:
        return None


# ----------------------- #


class TestEnsureQuoted:
    """Tests for ensure_quoted_etag helper."""

    def test_wraps_bare_value(self) -> None:
        """Bare values are wrapped in double-quotes."""
        assert ensure_quoted_etag("abc") == '"abc"'

    def test_preserves_already_quoted(self) -> None:
        """Already-quoted values are returned unchanged."""
        assert ensure_quoted_etag('"abc"') == '"abc"'

    def test_preserves_weak_etag(self) -> None:
        """Weak ETags starting with W/ are returned unchanged."""
        assert ensure_quoted_etag('W/"abc"') == 'W/"abc"'


class TestNormalizeForComparison:
    """Tests for normalize_etag_for_comparison helper."""

    def test_strips_weak_indicator(self) -> None:
        """Weak indicator W/ is removed for comparison."""
        assert normalize_etag_for_comparison('W/"abc"') == '"abc"'

    def test_strong_unchanged(self) -> None:
        """Strong ETag is returned unchanged."""
        assert normalize_etag_for_comparison('"abc"') == '"abc"'

    def test_strips_whitespace(self) -> None:
        """Surrounding whitespace is stripped."""
        assert normalize_etag_for_comparison('  "abc"  ') == '"abc"'


class TestETagMatches:
    """Tests for etag_matches helper."""

    def test_star_matches_any(self) -> None:
        """Wildcard * matches any ETag."""
        assert etag_matches('"abc"', "*") is True

    def test_exact_match(self) -> None:
        """Exact match returns True."""
        assert etag_matches('"abc"', '"abc"') is True

    def test_no_match(self) -> None:
        """Non-matching returns False."""
        assert etag_matches('"abc"', '"def"') is False

    def test_comma_separated_match(self) -> None:
        """Matching one tag in a comma-separated list returns True."""
        assert etag_matches('"abc"', '"def", "abc", "ghi"') is True

    def test_comma_separated_no_match(self) -> None:
        """No match in a comma-separated list returns False."""
        assert etag_matches('"abc"', '"def", "ghi"') is False

    def test_weak_comparison(self) -> None:
        """Weak and strong tags with same opaque value match."""
        assert etag_matches('"abc"', 'W/"abc"') is True
        assert etag_matches('W/"abc"', '"abc"') is True


class TestETagProviderProtocol:
    """Tests for ETagProviderPort protocol conformance."""

    def test_fixed_provider_satisfies_protocol(self) -> None:
        """_FixedProvider satisfies the ETagProviderPort protocol."""
        provider = _FixedProvider("v1")
        assert isinstance(provider, ETagProviderPort)

    def test_none_provider_satisfies_protocol(self) -> None:
        """_NoneProvider satisfies the ETagProviderPort protocol."""
        provider = _NoneProvider()
        assert isinstance(provider, ETagProviderPort)


def _request_with_headers(headers: dict[str, str]) -> Request:
    raw = [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()]
    scope: dict = {"type": "http", "headers": raw}
    return Request(scope)


class TestETagFeature:
    """Async behavior of ETagFeature around a handler."""

    @pytest.mark.asyncio
    async def test_adds_etag_header(self) -> None:
        from forze.application.contracts.mapping import MapperPort
        from forze.application.execution import Deps, ExecutionContext, FacadeOpRef
        from forze_fastapi.endpoints.http.contracts import HttpRequestDTO

        class Item(BaseModel):
            id: int
            name: str

        class _PassMapper(MapperPort[HttpRequestDTO, Item]):
            async def __call__(self, dto: HttpRequestDTO) -> Item:  # type: ignore[override]
                raise NotImplementedError

        spec = HttpEndpointSpec(
            http={"method": "GET", "path": "/item"},
            response=Item,
            mapper=_PassMapper(),
            facade_type=object,
            call=FacadeOpRef(op="x"),
        )

        ctx = HttpEndpointContext(
            raw_request=_request_with_headers({}),
            raw_kwargs={},
            exec_ctx=ExecutionContext(deps=Deps()),
            facade=object(),
            dto=HttpRequestDTO(),
            input=Item(id=1, name="a"),
            spec=spec,
            operation_id="test.op",
        )

        async def handler(c: HttpEndpointContext) -> Item:
            return Item(id=1, name="test")

        feature = ETagFeature(provider=_FixedProvider("abc123"), auto_304=True)
        wrapped = feature.wrap(handler)
        result = await wrapped(ctx)

        assert isinstance(result, Response)
        assert result.headers["etag"] == '"abc123"'

    @pytest.mark.asyncio
    async def test_304_when_if_none_match_matches(self) -> None:
        class Item(BaseModel):
            id: int

        from forze.application.contracts.mapping import MapperPort
        from forze.application.execution import Deps, ExecutionContext, FacadeOpRef
        from forze_fastapi.endpoints.http.contracts import HttpRequestDTO

        class _PassMapper(MapperPort[HttpRequestDTO, Item]):
            async def __call__(self, dto: HttpRequestDTO) -> Item:  # type: ignore[override]
                raise NotImplementedError

        spec = HttpEndpointSpec(
            http={"method": "GET", "path": "/item"},
            response=Item,
            mapper=_PassMapper(),
            facade_type=object,
            call=FacadeOpRef(op="x"),
        )

        ctx = HttpEndpointContext(
            raw_request=_request_with_headers({IF_NONE_MATCH_HEADER_KEY: '"abc123"'}),
            raw_kwargs={},
            exec_ctx=ExecutionContext(deps=Deps()),
            facade=object(),
            dto=HttpRequestDTO(),
            input=Item(id=1),
            spec=spec,
            operation_id="test.op",
        )

        async def handler(c: HttpEndpointContext) -> Item:
            return Item(id=1)

        feature = ETagFeature(provider=_FixedProvider("abc123"), auto_304=True)
        result = await feature.wrap(handler)(ctx)

        assert isinstance(result, Response)
        assert result.status_code == 304
        assert result.headers["etag"] == '"abc123"'

    @pytest.mark.asyncio
    async def test_auto_304_disabled_returns_200_with_body(self) -> None:
        class Item(BaseModel):
            id: int

        from forze.application.contracts.mapping import MapperPort
        from forze.application.execution import Deps, ExecutionContext, FacadeOpRef
        from forze_fastapi.endpoints.http.contracts import HttpRequestDTO

        class _PassMapper(MapperPort[HttpRequestDTO, Item]):
            async def __call__(self, dto: HttpRequestDTO) -> Item:  # type: ignore[override]
                raise NotImplementedError

        spec = HttpEndpointSpec(
            http={"method": "GET", "path": "/item"},
            response=Item,
            mapper=_PassMapper(),
            facade_type=object,
            call=FacadeOpRef(op="x"),
        )

        ctx = HttpEndpointContext(
            raw_request=_request_with_headers({IF_NONE_MATCH_HEADER_KEY: '"abc123"'}),
            raw_kwargs={},
            exec_ctx=ExecutionContext(deps=Deps()),
            facade=object(),
            dto=HttpRequestDTO(),
            input=Item(id=1),
            spec=spec,
            operation_id="test.op",
        )

        async def handler(c: HttpEndpointContext) -> Item:
            return Item(id=1)

        feature = ETagFeature(provider=_FixedProvider("abc123"), auto_304=False)
        result = await feature.wrap(handler)(ctx)

        assert isinstance(result, Response)
        assert result.status_code == 200


class TestDocumentEtagFn:
    """Behavior of document_etag (replaces legacy DocumentETagProvider)."""

    def test_generates_id_rev_tag(self) -> None:
        """Tag is derived from id and rev JSON fields."""
        body = orjson.dumps({"id": "abc-123", "rev": 5, "name": "test"})
        tag = document_etag(body)
        assert tag == "abc-123:5"

    def test_returns_none_for_missing_id(self) -> None:
        body = orjson.dumps({"rev": 5, "name": "test"})
        assert document_etag(body) is None

    def test_returns_none_for_missing_rev(self) -> None:
        body = orjson.dumps({"id": "abc-123", "name": "test"})
        assert document_etag(body) is None

    def test_returns_none_for_invalid_json(self) -> None:
        assert document_etag(b"not-json") is None

    def test_satisfies_etag_provider_protocol(self) -> None:
        assert isinstance(document_etag, ETagProviderPort)

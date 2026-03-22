"""Unit tests for HTTP endpoint feature validation and composition."""

import pytest

from forze.base.errors import CoreError
from forze_fastapi.endpoints.http.composition.helpers import (
    compose_endpoint_features,
    validate_http_features,
)
from forze_fastapi.endpoints.http.contracts import HttpEndpointHandlerPort
from forze_fastapi.endpoints.http.features import ETagFeature, IdempotencyFeature

# ----------------------- #


class TestValidateHttpFeatures:
    """Rules tying HTTP method to optional endpoint features."""

    def test_post_with_idempotency_ok(self) -> None:
        validate_http_features(
            {"method": "POST", "path": "/x"},
            [IdempotencyFeature()],
        )

    def test_get_with_etag_ok(self) -> None:
        validate_http_features(
            {"method": "GET", "path": "/x"},
            [ETagFeature(provider=lambda b: "t")],
        )

    def test_get_with_idempotency_raises(self) -> None:
        with pytest.raises(CoreError, match="Idempotent endpoints must be POST"):
            validate_http_features(
                {"method": "GET", "path": "/x"},
                [IdempotencyFeature()],
            )

    def test_post_with_etag_raises(self) -> None:
        with pytest.raises(CoreError, match="ETag endpoints must be GET"):
            validate_http_features(
                {"method": "POST", "path": "/x"},
                [ETagFeature(provider=lambda b: "t")],
            )


class TestComposeEndpointFeatures:
    """Feature wrappers apply outermost-first around the base handler."""

    @pytest.mark.asyncio
    async def test_features_wrap_in_declaration_order(self) -> None:
        order: list[str] = []

        class _Mark:
            def __init__(self, name: str) -> None:
                self._name = name

            def wrap(self, handler: HttpEndpointHandlerPort) -> HttpEndpointHandlerPort:
                name = self._name

                async def wrapped(ctx):  # type: ignore[no-untyped-def]
                    order.append(f"{name}:in")
                    result = await handler(ctx)
                    order.append(f"{name}:out")
                    return result

                return wrapped

        async def base(ctx):  # type: ignore[no-untyped-def]
            order.append("base")
            return "ok"

        composed = compose_endpoint_features(
            base,
            [_Mark("a"), _Mark("b")],
        )
        assert await composed(object()) == "ok"  # type: ignore[arg-type]
        # compose_endpoint_features applies each feature.wrap in order; the last
        # listed feature becomes the outermost wrapper around the handler.
        assert order == ["b:in", "a:in", "base", "a:out", "b:out"]

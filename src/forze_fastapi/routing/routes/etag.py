"""ETag route support for read endpoints.

Provides a route-level ETag capability that generates ``ETag`` headers
from response bodies and handles conditional ``If-None-Match`` requests,
returning *304 Not Modified* when the resource has not changed.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Sequence
from typing import Any, Protocol, TypedDict, final, runtime_checkable

from fastapi import Request, Response
from fastapi.params import Depends
from fastapi.routing import APIRoute

from .feature import RouteHandler

# ----------------------- #


@runtime_checkable
class ETagProvider(Protocol):
    """Strategy for deriving an ETag value from a serialized response.

    Implementations receive the raw response body bytes and must return
    a stable, opaque tag string (without surrounding quotes) or ``None``
    when the response is not eligible for ETag generation.
    """

    def generate(self, response_body: bytes) -> str | None:
        """Derive a stable tag string from *response_body*.

        :param response_body: Serialized response payload.
        :returns: Raw ETag value or ``None`` to skip ETag injection.
        """
        ...


# ....................... #


class ETagRouteConfig(TypedDict):
    """Resolved configuration for a single ETag-enabled route."""

    provider: ETagProvider
    """Provider used to generate the tag value."""

    auto_304: bool
    """Automatically return *304 Not Modified* when ``If-None-Match`` matches."""


# ....................... #


def _ensure_quoted(etag: str) -> str:
    """Wrap *etag* in double-quotes if it is not already properly quoted."""

    if etag.startswith('"') or etag.startswith('W/"'):
        return etag

    return f'"{etag}"'


def _normalize_for_comparison(etag: str) -> str:
    """Strip the weak indicator for comparison purposes (RFC 9110 §8.8.3.2)."""

    tag = etag.strip()

    if tag.startswith("W/"):
        tag = tag[2:]

    return tag


def _etag_matches(current: str, if_none_match: str) -> bool:
    """Return ``True`` when *current* matches any entry in *if_none_match*.

    Uses weak comparison as specified by RFC 9110 for conditional GET.
    """

    header = if_none_match.strip()

    if header == "*":
        return True

    normalized = _normalize_for_comparison(current)

    return any(
        _normalize_for_comparison(t) == normalized
        for t in header.split(",")
    )


# ....................... #


@final
class ETagFeature:
    """Composable :class:`~.feature.RouteFeature` that injects ETag headers.

    When wrapped around a route handler, generates an ``ETag`` from the
    response body via the configured :class:`ETagProvider` and optionally
    returns *304 Not Modified* for matching ``If-None-Match`` requests.
    """

    __slots__ = ("_config",)

    def __init__(self, *, config: ETagRouteConfig) -> None:
        self._config = config

    # ....................... #

    def wrap(self, handler: RouteHandler) -> RouteHandler:
        """Wrap *handler* with ETag generation and conditional response logic.

        :param handler: The next handler in the chain.
        :returns: A handler that adds ``ETag`` headers to responses.
        """

        config = self._config

        async def wrapped(request: Request) -> Response:
            resp = await handler(request)

            body = getattr(resp, "body", None)

            if not isinstance(body, (bytes, bytearray)):
                return resp

            raw_tag = config["provider"].generate(bytes(body))

            if raw_tag is None:
                return resp

            etag = _ensure_quoted(raw_tag)
            resp.headers["ETag"] = etag

            if config["auto_304"]:
                if_none_match = request.headers.get("if-none-match")

                if if_none_match and _etag_matches(etag, if_none_match):
                    return Response(
                        status_code=304,
                        headers={"ETag": etag},
                    )

            return resp

        return wrapped

    # ....................... #

    @property
    def extra_dependencies(self) -> Sequence[Depends]:
        """ETag requires no extra dependencies."""
        return ()


# ....................... #


class ETagRoute(APIRoute):
    """Custom :class:`APIRoute` that injects ``ETag`` headers and handles conditional GET.

    Before returning a response, generates an ETag via the configured
    :class:`ETagProvider`. When ``auto_304`` is enabled and the request
    carries a matching ``If-None-Match`` header, a *304 Not Modified*
    response is returned instead of the full body.
    """

    def __init__(
        self,
        *args: Any,
        etag_config: ETagRouteConfig,
        **kwargs: Any,
    ) -> None:
        self._feature = ETagFeature(config=etag_config)
        super().__init__(*args, **kwargs)

    # ....................... #

    def get_route_handler(self):  # type: ignore[no-untyped-def]
        """Return a handler that wraps the original with ETag logic."""

        return self._feature.wrap(super().get_route_handler())


# ....................... #


def make_etag_route_class(
    *,
    provider: ETagProvider,
    auto_304: bool = True,
) -> type[ETagRoute]:
    """Create a route class pre-configured with ETag settings.

    :param provider: Strategy used to generate the tag value.
    :param auto_304: Whether to return *304* on ``If-None-Match`` match.
    :returns: A subclass of :class:`ETagRoute` ready for use as
        ``route_class_override``.
    """

    cfg = ETagRouteConfig(provider=provider, auto_304=auto_304)

    class _Route(ETagRoute):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, etag_config=cfg, **kwargs)

    return _Route

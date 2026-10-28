"""Async HTTP client wrapper using httpx."""

import zlib
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, Final, cast, final
from urllib.parse import SplitResult, unquote, urlsplit, urlunsplit

from forze_http._compat import require_http

require_http()

# ....................... #

import attrs
import httpx

from forze.base.exceptions import exc
from forze.base.exceptions._utils import reraise_mapped
from forze.base.primitives import GuardedLifecycle, JsonDict

from .._logger import logger
from .errors import exc_interceptor
from .port import HttpClientPort
from .value_objects import HttpConfig

# ----------------------- #


def _merge_url(base_url: str | None, url: str) -> str:
    if url.startswith(("http://", "https://")):
        return url

    if base_url is None:
        raise exc.configuration("Relative HTTP URL requires a configured base_url")

    return f"{base_url.rstrip('/')}/{url.lstrip('/')}"


# ....................... #


def _redact_url(url: str | None) -> str | None:
    """Strip any ``user:pass@`` userinfo from *url* so credentials never reach logs."""

    if not url:
        return url

    parts = urlsplit(url)

    if not (parts.username or parts.password):
        return url

    host = parts.hostname or ""
    netloc = f"{host}:{parts.port}" if parts.port else host

    return urlunsplit(parts._replace(netloc=netloc))


# ....................... #


_INFLATE_WBITS: Final = {"gzip": zlib.MAX_WBITS | 16, "deflate": zlib.MAX_WBITS}
"""The codings a cap can be held on as they decode, with their zlib window bits."""

_DECODED_CODINGS: Final = frozenset({*_INFLATE_WBITS, "br", "zstd"})
"""Every content coding httpx decodes; it passes any other through as it arrived."""

_ACCEPT_ENCODING_UNDER_CAP: Final = "gzip, deflate"
"""What a client with ``max_response_bytes`` set asks for: the codings it can bound."""


# ....................... #


def _raw(response: httpx.Response) -> AsyncIterator[bytes]:
    """*response*'s body as it arrived, not decoded.

    The stream itself rather than ``aiter_raw``, which refuses a body already read: an
    in-memory response (a mock, an ASGI app) arrives read, and its stream still holds the
    bytes as they were sent.
    """

    return aiter(cast(httpx.AsyncByteStream, response.stream))


# ....................... #


@final
class _DecodedCap:
    """A body's decoded size, counted as it arrives and refused once it passes *max_bytes*.

    Decodes as httpx does — gzip, and deflate with or without its zlib header — but never
    more than one byte past the cap in a step, so a small compressed chunk cannot expand
    far past it first. A coding it cannot bound (``br``, ``zstd``, or a stack of codings)
    is refused rather than left for httpx to decode unbounded.
    """

    def __init__(self, headers: httpx.Headers, max_bytes: int) -> None:
        declared = headers.get("content-encoding", "")
        codings = [c.strip().lower() for c in declared.split(",")]
        decoded = [c for c in codings if c in _DECODED_CODINGS]

        if len(decoded) > 1 or (decoded and decoded[0] not in _INFLATE_WBITS):
            raise exc.infrastructure(
                f"HTTP response content coding {declared!r} cannot be held to "
                f"max_response_bytes {max_bytes}"
            )

        self.__coding = decoded[0] if decoded else None
        self.__inflate = (
            zlib.decompressobj(_INFLATE_WBITS[self.__coding]) if self.__coding else None
        )
        self.__first = True
        self.__max_bytes = max_bytes
        self.__total = 0

    # ....................... #

    def feed(self, chunk: bytes) -> bytes:
        """*chunk* decoded, refused once the body's decoded total passes the cap."""

        decoded = chunk if self.__inflate is None else self.__inflate_step(chunk)
        self.__total += len(decoded)

        if self.__total > self.__max_bytes:
            raise exc.infrastructure(
                f"HTTP response body exceeded max_response_bytes {self.__max_bytes}"
            )

        return decoded

    # ....................... #

    def __inflate_step(self, chunk: bytes) -> bytes:
        # One byte of room past the cap: output that fills it is over the cap and refused,
        # and output that does not means the whole chunk was consumed, so nothing is left
        # pending for a later step or a flush.
        room = self.__max_bytes - self.__total + 1
        first, self.__first = self.__first, False
        inflate = cast(Any, self.__inflate)

        try:
            return cast(bytes, inflate.decompress(chunk, room))

        except zlib.error as error:
            if first and self.__coding == "deflate":
                # A raw deflate stream with no zlib header, which httpx accepts as well.
                self.__inflate = zlib.decompressobj(-zlib.MAX_WBITS)

                return self.__inflate_step(chunk)

            raise exc.infrastructure("HTTP response body could not be decoded") from error


# ....................... #


async def _buffered(streaming: httpx.Response, max_bytes: int | None) -> httpx.Response:
    """*streaming* read whole, refused once it crosses *max_bytes*, as an in-memory response."""

    _refuse_declared_oversize(streaming, max_bytes)
    body = bytearray()

    if max_bytes is None:
        async for chunk in streaming.aiter_bytes():
            body.extend(chunk)

    else:
        cap = _DecodedCap(streaming.headers, max_bytes)

        async for chunk in _raw(streaming):
            body.extend(cap.feed(chunk))

    # The body is decoded now, so the headers stop describing an encoding: left in, httpx
    # decodes it a second time. Content-Length is dropped too and recomputed from the body.
    headers = streaming.headers.copy()
    headers.pop("content-encoding", None)
    headers.pop("content-length", None)

    return httpx.Response(
        status_code=streaming.status_code,
        headers=headers,
        content=bytes(body),
        request=streaming.request,
    )


# ....................... #


def _refuse_declared_oversize(streaming: httpx.Response, max_bytes: int | None) -> None:
    """Refuse *streaming* before its body is read when it declares more than *max_bytes*."""

    if max_bytes is not None:
        declared = streaming.headers.get("content-length")

        if declared is not None and int(declared) > max_bytes:
            raise exc.infrastructure(
                f"HTTP response Content-Length {declared} exceeds max_response_bytes {max_bytes}"
            )


# ....................... #


def _origin(parts: SplitResult) -> tuple[str, str | None, int | None]:
    """Scheme, host and port of *parts*, the port filled in from the scheme when omitted."""

    return (
        parts.scheme,
        parts.hostname,
        parts.port or {"http": 80, "https": 443}.get(parts.scheme),
    )


# ....................... #


def _is_under(url: str, base_url: str) -> bool:
    """Whether *url* is *base_url* or a path below it, on the same scheme, host and port.

    Compared part by part after parsing, never as text: ``https://api.example.com.evil.net``
    starts with ``https://api.example.com`` and is somewhere else. Fails closed on whatever
    could read differently at the far end — userinfo, a dot segment hidden behind percent
    encoding, a backslash some servers take for a slash, a port that does not parse, a
    ``;`` path parameter (servers that strip ``..;`` resolve it upward), and an escaped
    ``%`` (a server that decodes twice reads ``%252e%252e`` as ``..``).
    """

    try:
        target, base = urlsplit(url), urlsplit(base_url)

        if _origin(target) != _origin(base):
            return False

    except ValueError:
        return False

    if target.username is not None or target.password is not None:
        return False

    if "%25" in target.path:
        return False

    decoded = unquote(target.path)

    if ";" in decoded or {".", ".."} & set(decoded.replace("\\", "/").split("/")):
        return False

    # Segment by segment, on the path as sent: `/v1` must not admit `/v1-admin`, and an
    # encoded slash in a segment stays part of that segment rather than opening a new one.
    prefix = base.path.rstrip("/").split("/")

    return target.path.split("/")[: len(prefix)] == prefix


# ....................... #


@final
class _WireRelay(httpx.AsyncByteStream):
    """*upstream*'s body as it arrived — not decoded — cut off once *cap* refuses it.

    Each chunk is decoded into *cap* before it is passed on, so whoever decodes it next
    never holds more than the cap allows: the decoded size is what costs memory.
    """

    def __init__(self, upstream: httpx.Response, cap: _DecodedCap | None) -> None:
        self.__upstream = upstream
        self.__cap = cap

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in _raw(self.__upstream):
            if self.__cap is not None:
                self.__cap.feed(chunk)

            yield chunk

    async def aclose(self) -> None:
        await self.__upstream.aclose()


# ....................... #


@final
@attrs.define(slots=True)
class HttpClient(HttpClientPort):
    """Thin wrapper around :class:`httpx.AsyncClient`."""

    __client: httpx.AsyncClient | None = attrs.field(default=None, init=False)
    __base_url: str | None = attrs.field(default=None, init=False)
    __max_response_bytes: int | None = attrs.field(default=None, init=False)
    __default_headers: dict[str, str] = attrs.field(factory=dict, init=False)
    __lifecycle: GuardedLifecycle = attrs.field(factory=GuardedLifecycle, init=False)

    # ....................... #

    async def initialize(
        self,
        base_url: str | None = None,
        *,
        config: HttpConfig | None = None,
        default_headers: Mapping[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        async def setup() -> None:
            cfg = config or HttpConfig()
            self.__base_url = base_url
            self.__max_response_bytes = cfg.max_response_bytes
            self.__default_headers = dict(default_headers or {})

            # A cap holds only on codings decoded here step by step, so ask for no other.
            if cfg.max_response_bytes is not None and not any(
                name.lower() == "accept-encoding" for name in self.__default_headers
            ):
                self.__default_headers["Accept-Encoding"] = _ACCEPT_ENCODING_UNDER_CAP

            timeout = httpx.Timeout(cfg.timeout.total_seconds())
            client_kwargs: dict[str, Any] = {
                "timeout": timeout,
                "follow_redirects": cfg.follow_redirects,
                "headers": self.__default_headers,
            }

            if base_url is not None:
                client_kwargs["base_url"] = base_url

            if transport is not None:
                client_kwargs["transport"] = transport

            self.__client = httpx.AsyncClient(**client_kwargs)
            logger.trace("HTTP client connected", base_url=_redact_url(base_url))

        await self.__lifecycle.initialize(
            setup,
            ready=lambda: self.__client is not None,
        )

    # ....................... #

    def _require_client(self) -> httpx.AsyncClient:
        if self.__client is None:
            raise exc.internal("HttpClient is not initialized")

        return self.__client

    # ....................... #

    @exc_interceptor.coroutine("httpx.aclose")  # type: ignore[untyped-decorator]
    async def aclose(self) -> None:
        if self.__client is not None:
            await self.__client.aclose()
            self.__client = None
            self.__base_url = None
            logger.trace("HTTP client closed")

    # ....................... #

    async def startup(self) -> None:
        return None

    async def close(self) -> None:
        await self.__lifecycle.close(self.aclose)

    async def evict_tenant(self, tenant_id: Any) -> None:
        return None

    # ....................... #

    @exc_interceptor.coroutine("httpx.request")  # type: ignore[untyped-decorator]
    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: JsonDict | None = None,
        data: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        client = self._require_client()

        if url.startswith(("http://", "https://")):
            request_url = url
        elif self.__base_url is not None:
            request_url = _merge_url(self.__base_url, url)
        else:
            raise exc.configuration("Relative HTTP URL requires a configured base_url")

        request_timeout = timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT
        max_bytes = self.__max_response_bytes

        # Stream the response so an oversized body can be refused before (or while)
        # it is buffered, instead of httpx materializing the whole body in memory
        # first. With no cap configured this reads the full body exactly as before.
        async with client.stream(
            method,
            request_url,
            params=params,
            json=json,
            data=data,
            headers=headers,
            timeout=request_timeout,
        ) as streaming:
            response = await _buffered(streaming, max_bytes)

        if raise_for_status:
            response.raise_for_status()

        return response

    # ....................... #

    @exc_interceptor.asynccontextmanager("httpx.stream")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def stream(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: JsonDict | None = None,
        data: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        raise_for_status: bool = True,
    ) -> AsyncGenerator[httpx.Response]:
        client = self._require_client()
        request_timeout = timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT

        async with client.stream(
            method,
            _merge_url(self.__base_url, url),
            params=params,
            json=json,
            data=data,
            headers=headers,
            timeout=request_timeout,
        ) as streaming:
            # A rejected response is read whole under the same cap as `request`, so the
            # error carries its body; only a success is handed over unread.
            if not streaming.is_success:
                response = await _buffered(streaming, self.__max_response_bytes)

                if raise_for_status:
                    response.raise_for_status()

                yield response
                return

            try:
                yield streaming

            except httpx.HTTPError as error:
                # A failed read surfaces in the caller's block, past the interceptor's
                # enter, so it is mapped here the way `request` maps one.
                reraise_mapped(exc_interceptor.mapper, error, site="httpx.stream")

    # ....................... #

    @asynccontextmanager
    async def send(
        self,
        request: httpx.Request,
        *,
        base_url: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[httpx.Response]:
        client = self._require_client()
        pinned = base_url if base_url is not None else self.__base_url

        if pinned is None:
            raise exc.configuration("A prepared HTTP request needs a base_url to be held to")

        if not _is_under(str(request.url), pinned):
            parts = urlsplit(str(request.url))

            raise exc.configuration(
                "HTTP request refused: its URL is not under the service's base URL",
                code="http_request_off_route",
                # Origin and path only: a query can carry a key, and userinfo is a credential.
                details={"url": f"{parts.scheme}://{parts.netloc.rpartition('@')[2]}{parts.path}"},
            )

        # The client's own headers — a routed tenant's credential — and then the caller's
        # replace whatever the request brought, so a placeholder key in an SDK never leaves.
        request.headers.update(self.__default_headers)
        request.headers.update(headers or {})

        # Never followed here: a redirect goes back to whoever built the request, and its
        # next hop is held to the base URL again on the way out.
        upstream = await client.send(request, stream=True, follow_redirects=False)

        try:
            max_bytes = self.__max_response_bytes
            _refuse_declared_oversize(upstream, max_bytes)
            cap = None if max_bytes is None else _DecodedCap(upstream.headers, max_bytes)

            yield httpx.Response(
                status_code=upstream.status_code,
                headers=upstream.headers,
                stream=_WireRelay(upstream, cap),
                extensions=upstream.extensions,
            )

        finally:
            await upstream.aclose()

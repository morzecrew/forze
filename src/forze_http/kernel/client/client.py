"""Async HTTP client wrapper using httpx."""

from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from typing import Any, final
from urllib.parse import urlsplit, urlunsplit

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


async def _buffered(streaming: httpx.Response, max_bytes: int | None) -> httpx.Response:
    """*streaming* read whole, refused once it crosses *max_bytes*, as an in-memory response."""

    if max_bytes is not None:
        declared = streaming.headers.get("content-length")

        if declared is not None and int(declared) > max_bytes:
            raise exc.infrastructure(
                f"HTTP response Content-Length {declared} exceeds max_response_bytes {max_bytes}"
            )

    body = bytearray()

    async for chunk in streaming.aiter_bytes():
        body.extend(chunk)

        if max_bytes is not None and len(body) > max_bytes:
            raise exc.infrastructure(f"HTTP response body exceeded max_response_bytes {max_bytes}")

    return httpx.Response(
        status_code=streaming.status_code,
        headers=streaming.headers,
        content=bytes(body),
        request=streaming.request,
    )


# ....................... #


@final
@attrs.define(slots=True)
class HttpClient(HttpClientPort):
    """Thin wrapper around :class:`httpx.AsyncClient`."""

    __client: httpx.AsyncClient | None = attrs.field(default=None, init=False)
    __base_url: str | None = attrs.field(default=None, init=False)
    __max_response_bytes: int | None = attrs.field(default=None, init=False)
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
            timeout = httpx.Timeout(cfg.timeout.total_seconds())
            client_kwargs: dict[str, Any] = {
                "timeout": timeout,
                "follow_redirects": cfg.follow_redirects,
                "headers": dict(default_headers or {}),
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

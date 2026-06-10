"""Async HTTP client wrapper using httpx."""

from typing import Any, Mapping, final

from forze_http._compat import require_http

require_http()

# ....................... #

import asyncio

import httpx

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .errors import exc_interceptor
from .port import HttpxClientPort
from .value_objects import HttpxConfig

# ----------------------- #


def _merge_url(base_url: str | None, url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url

    if base_url is None:
        raise exc.configuration("Relative HTTP URL requires a configured base_url")

    return f"{base_url.rstrip('/')}/{url.lstrip('/')}"


# ....................... #


@final
@attrs.define(slots=True)
class HttpxClient(HttpxClientPort):
    """Thin wrapper around :class:`httpx.AsyncClient`."""

    __client: httpx.AsyncClient | None = attrs.field(default=None, init=False)
    __base_url: str | None = attrs.field(default=None, init=False)
    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #

    async def initialize(
        self,
        base_url: str | None = None,
        *,
        config: HttpxConfig | None = None,
        default_headers: Mapping[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        async with self.__init_lock:
            if self.__client is not None:
                return

            cfg = config or HttpxConfig()
            self.__base_url = base_url
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

    # ....................... #

    def _require_client(self) -> httpx.AsyncClient:
        if self.__client is None:
            raise exc.internal("HttpxClient is not initialized")

        return self.__client

    # ....................... #

    @exc_interceptor.coroutine("httpx.aclose")  # type: ignore[untyped-decorator]
    async def aclose(self) -> None:
        if self.__client is not None:
            await self.__client.aclose()
            self.__client = None
            self.__base_url = None

    # ....................... #

    async def startup(self) -> None:
        return None

    async def close(self) -> None:
        await self.aclose()

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
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        client = self._require_client()

        if url.startswith("http://") or url.startswith("https://"):
            request_url = url
        elif self.__base_url is not None:
            request_url = _merge_url(self.__base_url, url)
        else:
            raise exc.configuration("Relative HTTP URL requires a configured base_url")

        request_timeout = timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT

        response = await client.request(
            method,
            request_url,
            params=params,
            json=json,
            headers=headers,
            timeout=request_timeout,
        )
        response.raise_for_status()

        return response

from forze.base.errors import CoreError
from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Awaitable, Callable, Mapping

import attrs
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class CustomHeadersMiddleware:
    """Middleware that injects custom headers into the response.

    If headers already exist, middleware will raise an error.
    """

    app: ASGIApp
    """The next ASGI application."""

    static_headers: Mapping[str, str] | None = attrs.field(kw_only=True, default=None)
    """Static headers to inject into the response."""

    dynamic_headers: Mapping[str, Callable[[], str | Awaitable[str]]] | None = (
        attrs.field(
            kw_only=True,
            default=None,
        )
    )
    """Dynamic headers to inject into the response."""

    # ....................... #

    async def _compute_headers(self) -> list[tuple[bytes, bytes]]:
        headers: dict[str, str] = {}

        if self.static_headers is not None:
            headers.update(self.static_headers)

        if self.dynamic_headers is not None:
            for key, fn in self.dynamic_headers.items():
                value = fn()

                if isinstance(value, Awaitable):
                    value = await value

                headers[key] = str(value)

        return [
            (
                key.lower().strip().encode("latin-1"),
                value.strip().encode("latin-1"),
            )
            for key, value in headers.items()
        ]

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        injected_headers = await self._compute_headers()
        injected_header_keys = set([x[0] for x in injected_headers])

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers: list[tuple[bytes, bytes]] = list(message.get("headers", []))
                header_keys = set([h[0] for h in headers])

                if header_keys & injected_header_keys:
                    raise CoreError("Duplicate headers found")

                headers.extend(injected_headers)
                message["headers"] = headers

            await send(message)

        await self.app(scope, receive, send_wrapper)

"""Tests for :class:`~forze_fastapi.middlewares.custom_headers.CustomHeadersMiddleware`."""

from __future__ import annotations

from typing import Any

import pytest
from starlette.responses import Response

from forze.base.exceptions import CoreException
from forze_fastapi.middlewares.custom_headers import CustomHeadersMiddleware


async def _noop_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
    if scope["type"] == "http":
        await Response(status_code=200)(scope, receive, send)


@pytest.mark.asyncio
async def test_custom_headers_injects_static_and_dynamic() -> None:
    middleware = CustomHeadersMiddleware(
        _noop_app,
        static_headers={"X-Static": "a"},
        dynamic_headers={"X-Dynamic": lambda: "b"},
    )
    scope = {"type": "http", "method": "GET", "path": "/"}
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await middleware(scope, receive, send)

    start = next(m for m in messages if m["type"] == "http.response.start")
    header_map = {k.decode(): v.decode() for k, v in start["headers"]}
    assert header_map["x-static"] == "a"
    assert header_map["x-dynamic"] == "b"


@pytest.mark.asyncio
async def test_custom_headers_async_dynamic() -> None:
    async def _dyn() -> str:
        return "async"

    middleware = CustomHeadersMiddleware(
        _noop_app,
        dynamic_headers={"X-Async": _dyn},
    )
    headers = await middleware._compute_headers()
    assert (b"x-async", b"async") in headers


@pytest.mark.asyncio
async def test_custom_headers_duplicate_raises() -> None:
    async def app_with_header(scope: dict[str, Any], receive: Any, send: Any) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"x-static", b"existing")],
            },
        )
        await send({"type": "http.response.body", "body": b""})

    middleware = CustomHeadersMiddleware(
        app_with_header,
        static_headers={"X-Static": "injected"},
    )
    scope = {"type": "http", "method": "GET", "path": "/"}

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    with pytest.raises(CoreException, match="Duplicate headers"):
        await middleware(scope, receive, lambda m: None)


@pytest.mark.asyncio
async def test_custom_headers_non_http_passthrough() -> None:
    called = False

    async def inner(scope: dict[str, Any], receive: Any, send: Any) -> None:
        nonlocal called
        called = True

    middleware = CustomHeadersMiddleware(inner, static_headers={"X": "y"})
    await middleware({"type": "websocket"}, lambda: None, lambda m: None)
    assert called is True

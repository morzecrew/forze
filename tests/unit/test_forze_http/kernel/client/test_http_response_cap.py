"""Tests for the ``max_response_bytes`` response-size guard."""

import httpx
import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_http.kernel.client import HttpClient
from forze_http.kernel.client.value_objects import HttpConfig

# ----------------------- #


async def _client(handler, *, max_response_bytes: int | None) -> HttpClient:
    client = HttpClient()
    await client.initialize(
        base_url="https://api.example.com",
        config=HttpConfig(max_response_bytes=max_response_bytes),
        transport=httpx.MockTransport(handler),
    )
    return client


@pytest.mark.asyncio
async def test_body_within_cap_is_returned() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"ok")

    client = await _client(handler, max_response_bytes=1024)

    try:
        response = await client.request("GET", "/x")
        assert response.content == b"ok"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_oversized_content_length_is_refused() -> None:
    big = b"x" * 5000

    def handler(request: httpx.Request) -> httpx.Response:
        # httpx sets Content-Length from the body, so this exercises the
        # pre-read header check.
        return httpx.Response(200, content=big)

    client = await _client(handler, max_response_bytes=1000)

    try:
        with pytest.raises(CoreException) as err:
            await client.request("GET", "/x")
        assert err.value.kind is ExceptionKind.INFRASTRUCTURE
        assert "max_response_bytes" in str(err.value)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_oversized_streamed_body_without_length_is_aborted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # A chunked/streamed response carries no Content-Length, so the cap is
        # enforced while accumulating the body.
        def chunks():
            for _ in range(10):
                yield b"x" * 200

        return httpx.Response(200, content=chunks())

    client = await _client(handler, max_response_bytes=1000)

    try:
        with pytest.raises(CoreException) as err:
            await client.request("GET", "/x")
        assert err.value.kind is ExceptionKind.INFRASTRUCTURE
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_no_cap_allows_large_body() -> None:
    big = b"x" * 100_000

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=big)

    client = await _client(handler, max_response_bytes=None)

    try:
        response = await client.request("GET", "/x")
        assert len(response.content) == 100_000
    finally:
        await client.close()

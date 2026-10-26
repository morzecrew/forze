"""`HttpClient.stream`: the body is handed over unread, a rejection is not.

What these pin is the split between the two halves of a response. A rejected response is
read, capped and raised before the caller's block runs, so it fails exactly as `request`
fails. An accepted one reaches the caller as it arrives, with no cap, and leaving the
block — finished or not — closes it.
"""

import asyncio
from collections.abc import AsyncIterator, Callable

import httpx
import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_http.kernel.client import HttpClient
from forze_http.kernel.client.value_objects import HttpConfig

pytestmark = pytest.mark.unit

# ----------------------- #


class _Body(httpx.AsyncByteStream):
    """A response body sent chunk by chunk, that can stall, fail, and records its close."""

    def __init__(
        self,
        *chunks: bytes,
        stall: asyncio.Event | None = None,
        fail: Exception | None = None,
    ) -> None:
        self.chunks = chunks
        self.stall = stall
        self.fail = fail
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for index, chunk in enumerate(self.chunks):
            yield chunk

            # Past the first chunk: hold until released, so a client that buffers the
            # whole body before returning never gets as far as returning.
            if index == 0 and self.stall is not None:
                await self.stall.wait()

        if self.fail is not None:
            raise self.fail

    async def aclose(self) -> None:
        self.closed = True


async def _client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    max_response_bytes: int | None = None,
) -> HttpClient:
    client = HttpClient()
    await client.initialize(
        base_url="https://api.example.com",
        config=HttpConfig(max_response_bytes=max_response_bytes),
        transport=httpx.MockTransport(handler),
    )

    return client


# ....................... #


class TestHttpClientStream:
    async def test_the_body_arrives_as_it_is_sent(self) -> None:
        stall = asyncio.Event()
        client = await _client(
            lambda _r: httpx.Response(200, stream=_Body(b"one", b"two", stall=stall))
        )

        try:
            async with asyncio.timeout(2), client.stream("GET", "/x") as response:
                chunks = response.aiter_bytes()

                assert await anext(chunks) == b"one"

                stall.set()

                assert [chunk async for chunk in chunks] == [b"two"]

        finally:
            await client.close()

    @pytest.mark.parametrize("status", [302, 400, 401, 404, 429, 500, 503])
    async def test_a_rejection_raises_before_the_block_as_request_raises_it(
        self, status: int
    ) -> None:
        client = await _client(lambda _r: httpx.Response(status, content=b"nope"))
        entered = False

        try:
            with pytest.raises(CoreException) as by_request:
                await client.request("GET", "/x")

            with pytest.raises(CoreException) as by_stream:
                async with client.stream("GET", "/x"):
                    entered = True

        finally:
            await client.close()

        assert entered is False
        assert by_stream.value.kind is by_request.value.kind
        assert by_stream.value.code == by_request.value.code
        assert by_stream.value.summary == by_request.value.summary

    async def test_a_rejected_body_is_held_to_the_cap(self) -> None:
        client = await _client(
            lambda _r: httpx.Response(500, stream=_Body(b"x" * 600, b"x" * 600)),
            max_response_bytes=1000,
        )

        try:
            with pytest.raises(CoreException, match="max_response_bytes") as raised:
                async with client.stream("GET", "/x"):
                    pass

        finally:
            await client.close()

        assert raised.value.kind is ExceptionKind.INFRASTRUCTURE

    async def test_an_accepted_body_is_not_held_to_the_cap(self) -> None:
        # The cap protects memory, and a streamed body is never buffered.
        client = await _client(
            lambda _r: httpx.Response(200, content=b"x" * 5000),
            max_response_bytes=1000,
        )

        try:
            async with client.stream("GET", "/x") as response:
                received = b"".join([chunk async for chunk in response.aiter_bytes()])

        finally:
            await client.close()

        assert len(received) == 5000

    async def test_without_raise_for_status_the_rejection_is_yielded_read(self) -> None:
        client = await _client(lambda _r: httpx.Response(400, json={"error": "bad"}))

        try:
            async with client.stream("GET", "/x", raise_for_status=False) as response:
                status, content = response.status_code, response.content

        finally:
            await client.close()

        assert status == 400
        assert content == b'{"error":"bad"}'

    async def test_leaving_early_closes_the_response(self) -> None:
        body = _Body(b"one", b"two", b"three")
        client = await _client(lambda _r: httpx.Response(200, stream=body))

        try:
            async with client.stream("GET", "/x") as response:
                async for _chunk in response.aiter_bytes():
                    break

            assert body.closed is True

        finally:
            await client.close()

    async def test_a_read_timeout_mid_body_is_mapped(self) -> None:
        client = await _client(
            lambda _r: httpx.Response(
                200, stream=_Body(b"one", fail=httpx.ReadTimeout("idle upstream"))
            )
        )
        received: list[bytes] = []

        try:
            with pytest.raises(CoreException) as raised:
                async with client.stream("GET", "/x") as response:
                    async for chunk in response.aiter_bytes():
                        received.append(chunk)

        finally:
            await client.close()

        assert received == [b"one"]
        assert raised.value.kind is ExceptionKind.INFRASTRUCTURE
        assert raised.value.summary == "HTTP request timed out."

    async def test_the_timeout_reaches_the_transport(self) -> None:
        seen: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen.update(request.extensions["timeout"])
            return httpx.Response(200)

        client = await _client(handler)

        try:
            async with client.stream("GET", "/x", timeout=2.5):
                pass

        finally:
            await client.close()

        # Every phase, read included: between chunks the stream waits at most this long.
        assert seen == {"connect": 2.5, "read": 2.5, "write": 2.5, "pool": 2.5}

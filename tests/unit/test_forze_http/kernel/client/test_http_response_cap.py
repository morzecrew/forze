"""Tests for the ``max_response_bytes`` response-size guard."""

import contextlib
import gzip
import tracemalloc
import zlib
from collections.abc import AsyncIterator, Callable

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


# ....................... #


class _Chunks(httpx.AsyncByteStream):
    """A body served as these chunks, unread until the client reads it."""

    def __init__(self, *chunks: bytes) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


def _encoders() -> list[tuple[str, Callable[[bytes], bytes]]]:
    """Every content coding httpx can decode in this environment, with an encoder for it."""

    encoders: list[tuple[str, Callable[[bytes], bytes]]] = [
        ("gzip", gzip.compress),
        ("deflate", zlib.compress),
    ]

    with contextlib.suppress(ImportError):
        import zstandard

        encoders.append(("zstd", zstandard.ZstdCompressor().compress))

    with contextlib.suppress(ImportError):
        import brotli  # pyright: ignore[reportMissingImports]

        encoders.append(("br", brotli.compress))  # pyright: ignore[reportUnknownMemberType]

    return encoders


class TestEncodedBodies:
    """A compressed body is read once and decoded once. The buffered response used to keep
    the upstream `Content-Encoding`, so httpx decoded the decoded body a second time and every
    compressed answer failed as a transport error."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("coding", "encode"), _encoders())
    async def test_an_encoded_body_is_returned_decoded(
        self, coding: str, encode: Callable[[bytes], bytes]
    ) -> None:
        body = b'{"ok": true}'

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"Content-Encoding": coding}, content=encode(body))

        client = await _client(handler, max_response_bytes=None)

        try:
            response = await client.request("GET", "/x")

        finally:
            await client.close()

        assert response.json() == {"ok": True}
        assert "content-encoding" not in response.headers
        assert response.headers["content-length"] == str(len(body))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("coding", "encode"),
        [
            ("gzip", gzip.compress),
            ("deflate", zlib.compress),
            # A raw deflate stream with no zlib header, which httpx accepts too.
            ("deflate", lambda data: zlib.compress(data, wbits=-zlib.MAX_WBITS)),
        ],
    )
    async def test_under_a_cap_gzip_and_deflate_are_decoded(
        self, coding: str, encode: Callable[[bytes], bytes]
    ) -> None:
        seen: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers.get("accept-encoding"))
            return httpx.Response(
                200, headers={"Content-Encoding": coding}, content=encode(b'{"ok": true}')
            )

        client = await _client(handler, max_response_bytes=1024)

        try:
            response = await client.request("GET", "/x")

        finally:
            await client.close()

        assert response.json() == {"ok": True}
        # A capped client asks only for the codings it can bound as they decode.
        assert seen == ["gzip, deflate"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("coding", ["br", "zstd", "gzip, gzip"])
    async def test_under_a_cap_a_coding_it_cannot_bound_is_refused(self, coding: str) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"Content-Encoding": coding}, stream=_Chunks(b"?"))

        client = await _client(handler, max_response_bytes=1024)

        try:
            with pytest.raises(CoreException) as err:
                await client.request("GET", "/x")

        finally:
            await client.close()

        assert err.value.kind is ExceptionKind.INFRASTRUCTURE
        assert "content coding" in str(err.value)

    @pytest.mark.asyncio
    async def test_one_compressed_chunk_cannot_expand_past_the_cap(self) -> None:
        # 20 MB of zeros arrives as one 20 KB chunk. Decoding it whole before checking the
        # cap would hold all 20 MB; decoding it a step at a time holds about the cap.
        bomb = gzip.compress(bytes(20_000_000))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=_Chunks(bomb))

        client = await _client(handler, max_response_bytes=1000)
        tracemalloc.start()

        try:
            with pytest.raises(CoreException) as err:
                await client.request("GET", "/x")

            peak = tracemalloc.get_traced_memory()[1]

        finally:
            tracemalloc.stop()
            await client.close()

        assert "max_response_bytes" in str(err.value)
        assert peak < 2_000_000

    @pytest.mark.asyncio
    async def test_an_encoded_rejection_keeps_its_status(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                404, headers={"Content-Encoding": "gzip"}, content=gzip.compress(b"gone")
            )

        client = await _client(handler, max_response_bytes=None)

        try:
            with pytest.raises(CoreException) as err:
                await client.request("GET", "/x")

            async with client.stream("GET", "/x", raise_for_status=False) as rejected:
                content = rejected.content

        finally:
            await client.close()

        assert err.value.kind is ExceptionKind.NOT_FOUND
        assert content == b"gone"

    @pytest.mark.asyncio
    async def test_the_cap_counts_the_decoded_body(self) -> None:
        # Small on the wire, large once decoded: the memory the cap protects is the decoded one.
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, headers={"Content-Encoding": "gzip"}, content=gzip.compress(b"x" * 10_000)
            )

        client = await _client(handler, max_response_bytes=1000)

        try:
            with pytest.raises(CoreException) as err:
                await client.request("GET", "/x")

        finally:
            await client.close()

        assert "max_response_bytes" in str(err.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("coding", "encode"),
        [
            ("gzip", gzip.compress),
            ("deflate", zlib.compress),
            ("deflate", lambda data: zlib.compress(data, wbits=-zlib.MAX_WBITS)),
        ],
    )
    async def test_under_a_cap_a_truncated_body_is_refused(
        self, coding: str, encode: Callable[[bytes], bytes]
    ) -> None:
        # Cut short inside the compressed stream: what decoded so far is not the body.
        cut = encode(b'{"ok": true}' * 20)[:-6]

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"Content-Encoding": coding}, stream=_Chunks(cut))

        client = await _client(handler, max_response_bytes=4096)

        try:
            with pytest.raises(CoreException) as err:
                await client.request("GET", "/x")

        finally:
            await client.close()

        assert err.value.kind is ExceptionKind.INFRASTRUCTURE
        assert "ended before its content coding did" in str(err.value)

    @pytest.mark.asyncio
    async def test_under_a_cap_an_empty_encoded_body_is_returned(self) -> None:
        # A HEAD answer or a 304 declares the coding and carries no body to finish.
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=_Chunks())

        client = await _client(handler, max_response_bytes=4096)

        try:
            response = await client.request("HEAD", "/x")

        finally:
            await client.close()

        assert response.content == b""

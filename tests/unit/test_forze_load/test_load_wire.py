"""The stream wires a realtime run reads: event-stream lines and frames, and WebSocket frames."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from forze_load import Frame
from forze_load import _wire as wire
from forze_load._wire import (
    SseParser,
    WebSocket,
    WsHandshakeError,
    WsRefused,
    accept_for,
    sse_lines,
    ws_connect,
)

pytestmark = pytest.mark.unit

# ----------------------- #


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


async def _lines(*parts: bytes) -> list[str]:
    return [line async for line in sse_lines(_chunks(*parts))]


def _frames(text: str, parser: SseParser | None = None) -> list[Frame]:
    parser = parser or SseParser()
    frames = [parser.feed(line) for line in text.split("\n")]

    return [frame for frame in frames if frame is not None]


class TestEventStreamLines:
    async def test_lf_crlf_and_a_lone_cr_each_end_a_line(self) -> None:
        assert await _lines(b"a\nb\r\nc\rd\n") == ["a", "b", "c", "d"]

    async def test_a_crlf_split_across_chunks_ends_one_line(self) -> None:
        assert await _lines(b"data: a\r", b"\ndata: b\r\n\r", b"\n") == ["data: a", "data: b", ""]

    async def test_a_line_split_across_chunks_is_joined(self) -> None:
        assert await _lines(b"da", b"ta: x\n") == ["data: x"]

    async def test_a_line_past_the_limit_breaks_the_stream(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(wire, "_MAX_FRAME", 8)

        with pytest.raises(httpx.RemoteProtocolError):
            await _lines(b"data: 0123456789")

    async def test_one_leading_byte_order_mark_is_dropped_even_split(self) -> None:
        assert await _lines(b"\xef\xbb\xbfdata: x\n") == ["data: x"]
        assert await _lines(b"\xef", b"\xbb\xbfdata: y\n") == ["data: y"]
        assert await _lines(b"data: \xef\xbb\xbf\n") == ["data: \ufeff"]  # only a leading one

    async def test_a_stream_silent_for_idle_s_times_out(self) -> None:
        async def silent() -> AsyncIterator[bytes]:
            yield b"data: x\n"
            await asyncio.sleep(10)
            yield b""

        with pytest.raises(httpx.ReadTimeout):
            async for _line in sse_lines(silent(), idle_s=0.05):
                pass

    async def test_no_other_character_ends_a_line(self) -> None:
        # `str.splitlines` would split on U+2028; the event-stream format does not.
        assert await _lines("data: a b\n".encode()) == ["data: a b"]


class TestEventStreamFrames:
    def test_a_durable_frame_carries_its_id_event_and_data(self) -> None:
        parser = SseParser()
        (frame,) = _frames('id: e1\nevent: shipped\ndata: {"id": "e1", "data": {"k": 1}}\n', parser)

        assert (frame.event, frame.id, frame.data) == ("shipped", "e1", {"k": 1})
        assert parser.last_id == "e1"

    def test_an_ephemeral_frame_has_no_id_and_keeps_the_last_one(self) -> None:
        parser = SseParser()
        frames = _frames('id: e1\ndata: {"data": 1}\n\ndata: {"data": 2}\n', parser)

        assert [frame.id for frame in frames] == ["e1", None]
        assert [frame.event for frame in frames] == ["message", "message"]
        assert parser.last_id == "e1"

    def test_data_lines_join_with_newlines(self) -> None:
        (frame,) = _frames("data: a\ndata:b\ndata:  c\n")

        assert frame.text == "a\nb\n c"

    def test_keepalives_unknown_fields_and_a_blank_line_without_data_make_no_frame(self) -> None:
        assert _frames(": keepalive\n\nfoo: bar\n\nevent: x\n") == []

    def test_retry_is_read_in_seconds_and_only_from_digits(self) -> None:
        parser = SseParser()
        # "²" is a digit to `str.isdigit` and not to `int`.
        _frames("retry: 1500\n\nretry: soon\n\nretry: \u00b2\n", parser)

        assert parser.retry_s == 1.5

    def test_an_event_past_the_limit_breaks_the_stream(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(wire, "_MAX_FRAME", 8)
        parser = SseParser()
        parser.feed("data: 1234")

        with pytest.raises(httpx.RemoteProtocolError):
            parser.feed("data: 5678")

    def test_an_id_with_a_nul_is_ignored(self) -> None:
        parser = SseParser()
        (frame,) = _frames("id: a\0b\ndata: {}\n", parser)

        assert frame.id is None
        assert parser.last_id is None


class TestFrameData:
    def test_the_envelope_s_data_is_decoded_on_first_read(self) -> None:
        frame = Frame(event="e", id=None, text='{"id": null, "data": [1, 2]}')

        assert frame.data == [1, 2]

    def test_a_payload_that_is_not_json_reads_as_its_text(self) -> None:
        assert Frame(event="e", id=None, text="plain").data == "plain"

    def test_json_that_is_not_an_envelope_reads_as_itself(self) -> None:
        assert Frame(event="e", id=None, text="[1]").data == [1]

    def test_a_frame_whose_data_is_never_read_is_never_decoded(self) -> None:
        frame = Frame(event="e", id=None, text="{not json")

        assert frame.event == "e"  # nothing raised: decoding waits for `data`


# ....................... #


class _Stream:
    """A fake upgraded connection: reads what the test queued, records what the client sent."""

    def __init__(self, *frames: bytes) -> None:
        self.incoming = bytearray(b"".join(frames))
        self.sent: list[bytes] = []
        self.closed = False
        self.read_error: Exception | None = None

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self.read_error is not None:
            raise self.read_error

        chunk, self.incoming = bytes(self.incoming[:3]), self.incoming[3:]  # small reads

        return chunk

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if self.closed:
            raise OSError("closed")

        self.sent.append(buffer)

    async def aclose(self) -> None:
        self.closed = True


def _server_frame(opcode: int, payload: bytes, *, fin: bool = True, mask: bytes = b"") -> bytes:
    n = len(payload)
    length = (
        bytes([n])
        if n < 126
        else bytes([126]) + n.to_bytes(2)
        if n < 65536
        else bytes([127]) + n.to_bytes(8)
    )

    if mask:
        length = bytes([length[0] | 0x80]) + length[1:]
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))

    return bytes([(0x80 if fin else 0) | opcode]) + length + mask + payload


def _client_payload(frame: bytes) -> tuple[int, bytes]:
    assert frame[1] & 0x80, "a client frame must be masked"
    mask, payload = frame[2:6], frame[6:]

    return frame[0] & 0x0F, bytes(b ^ mask[i % 4] for i, b in enumerate(payload))


class TestWebSocket:
    async def test_a_text_message_is_read_whole(self) -> None:
        ws = WebSocket(_Stream(_server_frame(0x1, b'{"event": "x"}')))

        assert await ws.receive() == ("text", '{"event": "x"}')

    async def test_a_fragmented_message_is_joined_and_a_binary_one_skipped(self) -> None:
        stream = _Stream(
            _server_frame(0x2, b"\x00\x01"),
            _server_frame(0x1, b"he", fin=False),
            _server_frame(0x0, b"llo"),
        )

        assert await WebSocket(stream).receive() == ("text", "hello")

    async def test_a_pong_between_two_fragments_keeps_the_message_whole(self) -> None:
        stream = _Stream(
            _server_frame(0x1, b'{"a": ', fin=False),
            _server_frame(0xA, b""),
            _server_frame(0x0, b'"x"}'),
        )
        ws = WebSocket(stream)

        assert await ws.receive() == ("pong", None)
        assert await ws.receive() == ("text", '{"a": "x"}')

    async def test_a_ping_is_answered_with_a_masked_pong_carrying_its_payload(self) -> None:
        stream = _Stream(_server_frame(0x9, b"hi"), _server_frame(0x1, b"after"))

        assert await WebSocket(stream).receive() == ("text", "after")
        assert _client_payload(stream.sent[0]) == (0xA, b"hi")

    async def test_the_client_s_ping_comes_back_as_a_pong(self) -> None:
        stream = _Stream(_server_frame(0xA, b""))
        ws = WebSocket(stream)
        await ws.ping()

        assert _client_payload(stream.sent[0]) == (0x9, b"")
        assert await ws.receive() == ("pong", None)

    @pytest.mark.parametrize(
        ("payload", "code"), [((1012).to_bytes(2) + b"draining", 1012), (b"", 1005)]
    )
    async def test_a_close_frame_gives_its_code(self, payload: bytes, code: int) -> None:
        assert await WebSocket(_Stream(_server_frame(0x8, payload))).receive() == ("close", code)

    async def test_a_connection_that_ends_or_breaks_reads_as_an_abnormal_close(self) -> None:
        assert await WebSocket(_Stream(b"\x81")).receive() == ("close", 1006)

        broken = _Stream()
        broken.read_error = RuntimeError("reset")
        assert await WebSocket(broken).receive() == ("close", 1006)

    async def test_extended_lengths_are_read(self) -> None:
        medium, large = b"m" * 300, b"L" * 70_000
        ws = WebSocket(_Stream(_server_frame(0x1, medium), _server_frame(0x1, large)))

        assert await ws.receive() == ("text", medium.decode())
        assert await ws.receive() == ("text", large.decode())

    @pytest.mark.parametrize(
        ("frame", "code"),
        [
            (_server_frame(0x1, b"x", mask=b"\x01\x02\x03\x04"), 1002),  # a server never masks
            (bytes([0x81 | 0x40, 1]) + b"x", 1002),  # a reserved bit, with no extension agreed
            (_server_frame(0x3, b""), 1002),  # a reserved opcode
            (_server_frame(0x9, b"p" * 126), 1002),  # a control frame over 125 bytes
            (_server_frame(0x9, b"p", fin=False), 1002),  # a fragmented control frame
            (_server_frame(0x1, b"\xff\xfe"), 1007),  # text that is not UTF-8
            (bytes([0x81, 127]) + (1 << 40).to_bytes(8), 1009),  # a frame past the limit
        ],
        ids=["masked", "rsv", "opcode", "long-ping", "split-ping", "utf8", "huge"],
    )
    async def test_a_violation_closes_with_its_code_and_says_so(
        self, frame: bytes, code: int
    ) -> None:
        stream = _Stream(frame)

        assert await WebSocket(stream).receive() == ("close", code)
        assert _client_payload(stream.sent[-1]) == (0x8, code.to_bytes(2))

    async def test_a_message_past_the_limit_closes_with_1009(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(wire, "_MAX_FRAME", 8)
        stream = _Stream(_server_frame(0x1, b"12345", fin=False), _server_frame(0x0, b"6789"))

        assert await WebSocket(stream).receive() == ("close", 1009)

    async def test_the_server_s_close_is_echoed(self) -> None:
        stream = _Stream(_server_frame(0x8, (1012).to_bytes(2) + b"draining"))

        assert await WebSocket(stream).receive() == ("close", 1012)
        assert _client_payload(stream.sent[-1]) == (0x8, (1012).to_bytes(2))

    async def test_a_socket_silent_for_idle_s_reads_as_closed(self) -> None:
        class _Silent(_Stream):
            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                await asyncio.sleep(10)  # then a frame: only the idle timer reads it as closed
                return _server_frame(0x1, b"late")

        assert await WebSocket(_Silent(), idle_s=0.05).receive() == ("close", 1006)

    async def test_a_pong_to_a_broken_connection_is_not_an_error(self) -> None:
        stream = _Stream(_server_frame(0x9, b""))
        stream.closed = True

        assert await WebSocket(stream).receive() == ("close", 1006)


class TestWsConnect:
    async def test_the_upgrade_carries_the_handshake_headers_and_yields_the_socket(self) -> None:
        seen: list[httpx.Request] = []
        stream = _Stream(_server_frame(0x1, b"hi"))

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            headers = {
                "upgrade": "websocket",
                "connection": "keep-alive, Upgrade",
                "sec-websocket-accept": accept_for(request.headers["sec-websocket-key"]),
            }

            return httpx.Response(101, headers=headers, extensions={"network_stream": stream})

        async with (
            httpx.AsyncClient(
                transport=httpx.MockTransport(handler), base_url="http://t"
            ) as client,
            ws_connect(client, "/rt", params={"a": "1"}, headers={"X-User": "ada"}) as ws,
        ):
            assert await ws.receive() == ("text", "hi")

        request = seen[0]
        assert request.url.params["a"] == "1"
        assert request.headers["Upgrade"] == "websocket"
        assert request.headers["X-User"] == "ada"
        assert len(request.headers["Sec-WebSocket-Key"]) == 24
        assert stream.closed

    async def test_another_status_than_101_is_refused(self) -> None:
        async def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(403)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://t"
        ) as client:
            with pytest.raises(WsRefused) as refused:
                async with ws_connect(client, "/rt", params={}, headers={}):
                    await asyncio.sleep(0)

        assert refused.value.status == 403

    @pytest.mark.parametrize(
        "headers",
        [
            {},
            {"upgrade": "websocket", "connection": "Upgrade", "sec-websocket-accept": "wrong"},
            {"upgrade": "h2c", "connection": "Upgrade"},
        ],
        ids=["none", "wrong-accept", "wrong-upgrade"],
    )
    async def test_a_101_that_does_not_complete_the_handshake_is_refused(
        self, headers: dict[str, str]
    ) -> None:
        stream = _Stream()

        async def handler(request: httpx.Request) -> httpx.Response:
            if "sec-websocket-accept" not in headers and headers:
                headers["sec-websocket-accept"] = accept_for(request.headers["sec-websocket-key"])

            return httpx.Response(101, headers=headers, extensions={"network_stream": stream})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://t"
        ) as client:
            with pytest.raises(WsHandshakeError):
                async with ws_connect(client, "/rt", params={}, headers={}):
                    await asyncio.sleep(0)

    def test_the_accept_is_the_rfc_s_example(self) -> None:
        # RFC 6455 §1.3's worked example.
        assert accept_for("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="

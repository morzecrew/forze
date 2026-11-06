"""The two stream wires a realtime run reads: Server-Sent Events, and a minimal WebSocket client.

httpx speaks neither as a stream of frames. SSE is plain HTTP, so its lines are split here as
the event-stream format says (CR, LF or CRLF, nothing else). A WebSocket is opened through
httpx's HTTP/1.1 upgrade, so the request keeps the run's target guard, cookie policy and
credentials, and its frames (RFC 6455) are read off the upgraded connection here. The client
only reads: it answers pings, sends one ping to learn the socket is past its handshake, and
never sends a message, so it never acks or moves a cursor.
"""

from __future__ import annotations

import base64
import contextlib
import json
import re
from collections.abc import AsyncIterator, Mapping
from typing import Any, Literal, Protocol

import attrs
import httpx

from forze.base.primitives import secure_random_bytes

# ----------------------- #

_NEWLINE = re.compile(rb"\r\n|\r|\n")
_UNREAD: Any = object()

_TEXT, _PING, _PONG = 0x1, 0x9, 0xA
_MAX_FRAME = 16 * 1024 * 1024
"""The largest frame read; past it the stream is treated as broken, not buffered."""

NO_STATUS = 1005
"""The close code RFC 6455 reserves for a close frame that carries none."""


# ....................... #


@attrs.define(eq=False)
class Frame:
    """One realtime frame as a subscriber received it, handed to a plan's ``match``.

    ``data`` is the frame's payload, the ``data`` of forze's ``{id, data}`` envelope. It is
    decoded from JSON the first time it is read, so a ``match`` that reads only ``event``
    costs no decoding; a payload that is not JSON reads as its text.
    """

    event: str
    """The event name: the SSE ``event:`` field, or the WebSocket frame's ``event``."""

    id: str | None
    """The durable event id, or ``None`` for an ephemeral signal."""

    text: str = attrs.field(repr=False)
    """The frame's JSON text as received."""

    _data: Any = attrs.field(default=_UNREAD, repr=False)

    @property
    def data(self) -> Any:
        """The payload: the envelope's ``data``, decoded on first read."""

        if self._data is _UNREAD:
            try:
                envelope = json.loads(self.text)

            except ValueError:
                envelope = self.text

            self._data = envelope.get("data") if isinstance(envelope, dict) else envelope

        return self._data


# ....................... #


async def sse_lines(chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
    """The lines of an event stream: split on CR, LF or CRLF, a CRLF split across chunks
    included. Unlike ``str.splitlines``, no other character ends a line."""

    buffer = b""
    after_cr = False

    async for chunk in chunks:
        if after_cr and chunk.startswith(b"\n"):
            chunk = chunk[1:]  # the LF of a CRLF whose CR ended the previous chunk

        after_cr = chunk.endswith(b"\r")
        *lines, buffer = _NEWLINE.split(buffer + chunk)

        for line in lines:
            yield line.decode("utf-8", "replace")


@attrs.define
class SseParser:
    """The event-stream format, one line at a time: ``id``, ``event``, ``data``, ``retry``."""

    last_id: str | None = None
    """The last ``id:`` the stream set: what a resume sends as ``Last-Event-ID``."""

    retry_s: float | None = None
    """The last ``retry:`` the stream set, in seconds."""

    _event: str | None = None
    _id: str | None = None
    _data: list[str] = attrs.field(factory=lambda: list[str]())

    def feed(self, line: str) -> Frame | None:
        """Take one line; return the frame a blank line completes, if it carried data."""

        if not line:
            frame = (
                Frame(event=self._event or "message", id=self._id, text="\n".join(self._data))
                if self._data
                else None
            )
            self._event, self._id, self._data = None, None, []

            return frame

        if line.startswith(":"):  # a comment: forze's keepalive
            return None

        name, _, value = line.partition(":")
        value = value.removeprefix(" ")

        match name:
            case "data":
                self._data.append(value)

            case "event":
                self._event = value

            case "id" if "\0" not in value:
                self._id = value or None
                self.last_id = self._id

            case "retry" if value.isdigit():
                self.retry_s = int(value) / 1000

            case _:  # an unknown field is ignored, as the format says
                pass

        return None


# ....................... #


class _Stream(Protocol):
    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes: ...

    async def write(self, buffer: bytes, timeout: float | None = None) -> None: ...

    async def aclose(self) -> None: ...


class WsRefused(Exception):
    """The upgrade was answered with another status than 101."""

    def __init__(self, status: int) -> None:
        super().__init__(f"WebSocket upgrade answered {status}")
        self.status = status


type WsEvent = (
    tuple[Literal["text"], str] | tuple[Literal["pong"], None] | tuple[Literal["close"], int]
)


class WebSocket:
    """The read side of an upgraded connection, answering the server's pings."""

    def __init__(self, stream: _Stream) -> None:
        self._stream = stream
        self._buffer = b""

    async def ping(self) -> None:
        """Send a ping: its pong, or a close, tells whether the server kept the socket."""

        await self._send(_PING, b"")

    async def receive(self) -> WsEvent:
        """The next text message, the pong of :meth:`ping`, or the server's close code.

        A connection that ends without a close frame reads as an abnormal close, 1006.
        """

        message: list[bytes] = []
        kind = _TEXT

        while True:
            try:
                fin, opcode, payload = await self._frame()

            except EOFError:
                return "close", 1006

            match opcode:
                case 0x0 | 0x1 | 0x2:  # a message, or a fragment of one
                    kind = opcode or kind
                    message.append(payload)

                    if not fin:
                        continue

                    data, message = b"".join(message), []

                    if kind == _TEXT:  # forze's protocol is JSON text; a binary one is skipped
                        return "text", data.decode("utf-8", "replace")

                case 0x8:
                    return "close", int.from_bytes(payload[:2]) if len(payload) >= 2 else NO_STATUS

                case 0x9:
                    await self._send(_PONG, payload)

                case 0xA:
                    return "pong", None

                case _:  # a reserved opcode: the server broke the protocol
                    return "close", 1002

    async def _frame(self) -> tuple[bool, int, bytes]:
        head = await self._exactly(2)
        fin, opcode = bool(head[0] & 0x80), head[0] & 0x0F
        masked, length = bool(head[1] & 0x80), head[1] & 0x7F

        if length == 126:
            length = int.from_bytes(await self._exactly(2))

        elif length == 127:
            length = int.from_bytes(await self._exactly(8))

        if length > _MAX_FRAME:
            raise EOFError

        mask = await self._exactly(4) if masked else b""
        payload = await self._exactly(length)

        return fin, opcode, _unmask(payload, mask) if masked else payload

    async def _exactly(self, n: int) -> bytes:
        while len(self._buffer) < n:
            try:
                chunk = await self._stream.read(65536)

            # httpcore's network errors are not httpx's: a broken connection reads as closed.
            except Exception as error:
                raise EOFError from error

            if not chunk:
                raise EOFError

            self._buffer += chunk

        head, self._buffer = self._buffer[:n], self._buffer[n:]

        return head

    async def _send(self, opcode: int, payload: bytes) -> None:
        # A client's frames are masked (RFC 6455 §5.3); control frames carry at most 125 bytes.
        mask = secure_random_bytes(4)
        frame = bytes([0x80 | opcode, 0x80 | len(payload)]) + mask + _unmask(payload, mask)

        # A broken connection: the next read reports it as a close.
        with contextlib.suppress(Exception):
            await self._stream.write(frame)


def _unmask(payload: bytes, mask: bytes) -> bytes:
    return bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))


@contextlib.asynccontextmanager
async def ws_connect(
    client: httpx.AsyncClient,
    path: str,
    *,
    params: Mapping[str, str],
    headers: Mapping[str, str],
) -> AsyncIterator[WebSocket]:
    """Open a WebSocket at *path* through *client*'s HTTP/1.1 upgrade.

    :raises WsRefused: the server answered the upgrade with another status than 101.
    """

    upgrade = {
        **headers,
        "Connection": "Upgrade",
        "Upgrade": "websocket",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": base64.b64encode(secure_random_bytes(16)).decode(),
    }

    async with client.stream("GET", path, params=params, headers=upgrade) as response:
        if response.status_code != 101:
            raise WsRefused(response.status_code)

        stream: _Stream = response.extensions["network_stream"]

        try:
            yield WebSocket(stream)

        finally:
            await stream.aclose()

"""The two stream wires a realtime run reads: Server-Sent Events, and a minimal WebSocket client.

httpx speaks neither as a stream of frames. SSE is plain HTTP, so its lines are split here as
the event-stream format says (CR, LF or CRLF, nothing else). A WebSocket is opened through
httpx's HTTP/1.1 upgrade, so the request keeps the run's target guard, cookie policy and
credentials, and its frames (RFC 6455) are read off the upgraded connection here. The client
only reads: it answers pings, sends one ping to learn the socket is past its handshake, and
never sends a message, so it never acks or moves a cursor.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
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

_TEXT, _CLOSE, _PING, _PONG = 0x1, 0x8, 0x9, 0xA
_MAX_FRAME = 16 * 1024 * 1024
"""The largest line, event or message read; past it the stream is treated as broken."""

_BOM = b"\xef\xbb\xbf"
_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
"""RFC 6455 §1.3: the server proves it read the key by hashing it with this."""

NO_STATUS = 1005
"""The close code RFC 6455 reserves for a close frame that carries none."""


# ....................... #


@attrs.define(eq=False)
class Frame:
    """One realtime frame as a subscriber received it, handed to a plan's ``match``.

    ``data`` is the frame's payload, the ``data`` of forze's ``{id, data}`` envelope. On SSE it
    is decoded from JSON the first time it is read, so a ``match`` that reads only ``event``
    costs no decoding; a WebSocket frame is decoded on arrival, since its event name is inside.
    A payload that is not JSON reads as its text.
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


async def sse_lines(
    chunks: AsyncIterator[bytes], *, idle_s: float | None = None
) -> AsyncIterator[str]:
    """The lines of an event stream: split on CR, LF or CRLF, a CRLF split across chunks
    included, after one leading byte-order mark. Unlike ``str.splitlines``, no other character
    ends a line.

    :raises httpx.ReadTimeout: no byte came for *idle_s* seconds.
    :raises httpx.RemoteProtocolError: a line ran past :data:`_MAX_FRAME`.
    """

    buffer = b""
    after_cr = False
    started = False
    iterator = aiter(chunks)

    while True:
        try:
            async with asyncio.timeout(idle_s):
                chunk = await anext(iterator)

        except StopAsyncIteration:
            return

        except TimeoutError as error:
            raise httpx.ReadTimeout(f"No byte for {idle_s} s") from error

        if not started:  # the format ignores one leading BOM, split across chunks or not
            buffer += chunk

            if len(buffer) < len(_BOM) and _BOM.startswith(buffer):
                continue

            chunk, buffer, started = buffer.removeprefix(_BOM), b"", True

        if after_cr and chunk.startswith(b"\n"):
            chunk = chunk[1:]  # the LF of a CRLF whose CR ended the previous chunk

        after_cr = chunk.endswith(b"\r")
        *lines, buffer = _NEWLINE.split(buffer + chunk)

        # A line completed inside this chunk counts as much as the remainder still open.
        if len(buffer) > _MAX_FRAME or any(len(line) > _MAX_FRAME for line in lines):
            raise httpx.RemoteProtocolError("An event-stream line ran past the 16 MiB limit")

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
    _size: int = 0

    def feed(self, line: str) -> Frame | None:
        """Take one line; return the frame a blank line completes, if it carried data.

        :raises httpx.RemoteProtocolError: an event's data ran past :data:`_MAX_FRAME`.
        """

        if not line:
            frame = (
                Frame(event=self._event or "message", id=self._id, text="\n".join(self._data))
                if self._data
                else None
            )
            self._event, self._id, self._data, self._size = None, None, [], 0

            return frame

        if line.startswith(":"):  # a comment: forze's keepalive
            return None

        name, _, value = line.partition(":")
        value = value.removeprefix(" ")

        match name:
            case "data":
                self._size += len(value.encode()) + 1  # bytes, as the limit is on the wire

                if self._size > _MAX_FRAME:
                    raise httpx.RemoteProtocolError("An event's data ran past the 16 MiB limit")

                self._data.append(value)

            case "event":
                self._event = value

            case "id" if "\0" not in value:
                self._id = value or None
                self.last_id = self._id

            case "retry" if value.isascii() and value.isdigit():
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


class WsHandshakeError(Exception):
    """A 101 whose headers do not complete the handshake (RFC 6455 §4.1)."""


class _Violation(Exception):
    """The server broke the protocol; the client closes with :attr:`code`."""

    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


type WsEvent = (
    tuple[Literal["text"], str] | tuple[Literal["pong"], None] | tuple[Literal["close"], int]
)


class WebSocket:
    """The read side of an upgraded connection, answering the server's pings.

    A protocol violation closes the socket with its code, the close echoed to the server:
    1002 for a masked frame, reserved bits, a reserved opcode or a control frame that is
    fragmented or over 125 bytes; 1007 for text that is not UTF-8; 1009 for a frame or a
    message past :data:`_MAX_FRAME`. No byte for *idle_s* seconds reads as 1006.
    """

    def __init__(self, stream: _Stream, *, idle_s: float | None = None) -> None:
        self._stream = stream
        self._idle_s = idle_s
        self._buffer = b""
        # A message's fragments, kept across calls: a pong can arrive between two of them.
        self._message: list[bytes] = []
        self._size = 0
        self._kind = _TEXT
        self._fragmented = False

    async def ping(self) -> None:
        """Send a ping: its pong, or a close, tells whether the server kept the socket."""

        await self._send(_PING, b"")

    async def receive(self) -> WsEvent:
        """The next text message, the pong of :meth:`ping`, or the server's close code.

        A connection that ends without a close frame reads as an abnormal close, 1006.
        """

        try:
            return await self._receive()

        except EOFError:
            return "close", 1006

        except _Violation as violation:
            await self._send(_CLOSE, violation.code.to_bytes(2))

            return "close", violation.code

    async def _receive(self) -> WsEvent:
        while True:
            fin, opcode, payload = await self._frame()

            if opcode >= _CLOSE and (not fin or len(payload) > 125):
                raise _Violation(1002)  # a control frame is whole and short (§5.5)

            match opcode:
                case 0x0 | 0x1 | 0x2:  # a message, or a fragment of one
                    # A continuation needs a message to continue; a new one, none open (§5.4).
                    if (opcode == 0x0) != self._fragmented:
                        raise _Violation(1002)

                    self._kind = opcode or self._kind
                    self._fragmented = not fin
                    self._message.append(payload)
                    self._size += len(payload)

                    if self._size > _MAX_FRAME:
                        raise _Violation(1009)

                    if not fin:
                        continue

                    data, self._message, self._size = b"".join(self._message), [], 0

                    if self._kind != _TEXT:  # forze's protocol is JSON text: binary is skipped
                        continue

                    try:
                        return "text", data.decode("utf-8")

                    except UnicodeDecodeError:
                        raise _Violation(1007) from None

                case 0x8:
                    if len(payload) == 1:  # a status is two bytes or absent (§5.5.1)
                        raise _Violation(1002)

                    await self._send(_CLOSE, payload[:2])  # echoed, as §5.5.1 asks

                    return "close", int.from_bytes(payload[:2]) if len(payload) >= 2 else NO_STATUS

                case 0x9:
                    await self._send(_PONG, payload)

                case 0xA:
                    return "pong", None

                case _:
                    raise _Violation(1002)

    async def _frame(self) -> tuple[bool, int, bytes]:
        head = await self._exactly(2)
        fin, opcode = bool(head[0] & 0x80), head[0] & 0x0F
        length = head[1] & 0x7F

        # No extension was negotiated, so no reserved bit; a server never masks (§5.1).
        if head[0] & 0x70 or head[1] & 0x80:
            raise _Violation(1002)

        if length == 126:
            length = int.from_bytes(await self._exactly(2))

        elif length == 127:
            length = int.from_bytes(await self._exactly(8))

        if length > _MAX_FRAME:
            raise _Violation(1009)

        return fin, opcode, await self._exactly(length)

    async def _exactly(self, n: int) -> bytes:
        while len(self._buffer) < n:
            try:
                async with asyncio.timeout(self._idle_s):
                    chunk = await self._stream.read(65536)

            # httpcore's network errors are not httpx's: a broken connection, or one silent for
            # `idle_s`, reads as closed.
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


def accept_for(nonce: str) -> str:
    """The ``Sec-WebSocket-Accept`` a server answers the *nonce* sent as ``Sec-WebSocket-Key``
    with (RFC 6455 §4.2.2)."""

    # SHA-1 is what the RFC mandates here: a proof the server read the nonce, not a secret.
    digest = hashlib.sha1((nonce + _WS_GUID).encode(), usedforsecurity=False).digest()

    return base64.b64encode(digest).decode()


def _accepted(headers: httpx.Headers, nonce: str) -> bool:
    connection = {token.strip().lower() for token in headers.get("connection", "").split(",")}

    return (
        headers.get("upgrade", "").lower() == "websocket"
        and "upgrade" in connection
        and headers.get("sec-websocket-accept") == accept_for(nonce)
    )


@contextlib.asynccontextmanager
async def ws_connect(
    client: httpx.AsyncClient,
    path: str,
    *,
    params: Mapping[str, str],
    headers: Mapping[str, str],
    idle_s: float | None = None,
) -> AsyncIterator[WebSocket]:
    """Open a WebSocket at *path* through *client*'s HTTP/1.1 upgrade.

    :raises WsRefused: the server answered the upgrade with another status than 101.
    :raises WsHandshakeError: a 101 without ``Upgrade: websocket``, ``Connection: upgrade``
        or the ``Sec-WebSocket-Accept`` the nonce calls for.
    """

    nonce = base64.b64encode(secure_random_bytes(16)).decode()
    upgrade = {
        **headers,
        "Connection": "Upgrade",
        "Upgrade": "websocket",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": nonce,
    }

    async with client.stream("GET", path, params=params, headers=upgrade) as response:
        if response.status_code != 101:
            raise WsRefused(response.status_code)

        if not _accepted(response.headers, nonce):
            raise WsHandshakeError("The 101 does not complete the WebSocket handshake")

        stream: _Stream = response.extensions["network_stream"]

        try:
            yield WebSocket(stream, idle_s=idle_s)

        finally:
            await stream.aclose()

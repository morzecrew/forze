"""Chunked-AEAD wire format for bounded-memory encryption of large values.

The whole-payload :mod:`~forze.base.crypto.envelope` seals a value as one AEAD
blob, so encrypting or decrypting it needs the entire plaintext in memory. This
module frames a value into a header plus a sequence of independently-sealed
*chunks*, so a producer/consumer touches only one chunk at a time — the basis for
streaming a large object through fixed memory.

Layout (all integers big-endian)::

    header:  magic "FZEc" | scheme(u8) | alg | key_id | key_version | wrapped_dek | chunk_size(u32)
    chunk*:  is_final(u8) | nonce | ciphertext            (repeated to end of stream)

(Variable fields are length-prefixed: ``alg``/``key_version``/``nonce`` by a u8,
``key_id``/``wrapped_dek`` by a u16, ``ciphertext`` by a u32.)

Each chunk is sealed under the object's single data key with a fresh nonce and an
AAD of ``base_aad | index | is_final``. The index is *positional* (the reader's
running counter, never stored), so reordering a chunk fails authentication; the
``is_final`` flag is stored and authenticated, so dropping the last chunk
(truncation) leaves the stream with no final chunk and is rejected, and a
non-final chunk cannot be passed off as the terminator. The data key itself is
KMS-wrapped in the header exactly as in the whole-payload envelope — this module
never sees a key-encryption key.

A new magic (``FZEc`` vs the whole-payload ``FZEv``) lets a reader tell the two
formats apart, so a store can hold a mix during a migration.
"""

import struct
from typing import Iterator, final

import attrs

from ..exceptions import exc
from .aead import Aead

# ----------------------- #

_CHUNK_MAGIC = b"FZEc"
"""Magic marker for a chunked-AEAD stream (``FZE`` + chunked-format family)."""

_CHUNK_SCHEME_VERSION = 1
"""Current chunked-scheme version (bumped on an incompatible layout change)."""

DEFAULT_CHUNK_SIZE = 1 << 20
"""Default plaintext chunk size (1 MiB) — the memory granularity of a stream."""

MAX_CHUNK_SIZE = (1 << 32) - 1
"""Largest declarable chunk size (the u32 ceiling)."""

_HEADER = struct.Struct(">4sB")
"""Magic (4 bytes) + scheme version (1 byte)."""

_U8 = struct.Struct(">B")
_U16 = struct.Struct(">H")
_U32 = struct.Struct(">I")


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ChunkedHeader:
    """Self-describing header for a chunked-AEAD stream (everything but the chunks)."""

    alg: str
    """AEAD algorithm that sealed every chunk (e.g. ``"AES-256-GCM"``)."""

    key_id: str
    """Identifier of the key-encryption key (CMK) that wrapped the data key."""

    key_version: str | None
    """Version of the key-encryption key, when the backend exposes one."""

    wrapped_dek: bytes
    """Data-encryption key wrapped under the key-encryption key (safe to store)."""

    chunk_size: int
    """Plaintext bytes per chunk used by the writer (the last chunk may be smaller)."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ChunkFrame:
    """One parsed (still-sealed) chunk: its terminator flag, nonce, and ciphertext."""

    is_final: bool
    """Whether this is the terminating chunk (authenticated via the AAD)."""

    nonce: bytes = attrs.field(repr=False)
    ciphertext: bytes = attrs.field(repr=False)


# ....................... #


def is_chunked_envelope(blob: bytes) -> bool:
    """Return whether *blob* begins with the chunked-AEAD magic marker."""

    return len(blob) >= len(_CHUNK_MAGIC) and blob[: len(_CHUNK_MAGIC)] == _CHUNK_MAGIC


# ....................... #


def _chunk_aad(base_aad: bytes, index: int, is_final: bool) -> bytes:
    """Per-chunk associated data: the base context plus fixed-width index + final flag.

    The suffix is fixed width (8-byte index, 1-byte flag) at the end, so it stays
    unambiguous whatever *base_aad* contains.
    """

    return b"".join(
        (base_aad, b"|i=", index.to_bytes(8, "big"), b"|f=", b"\x01" if is_final else b"\x00")
    )


# ....................... #


def _put_var(parts: list[bytes], length_struct: struct.Struct, payload: bytes) -> None:
    max_len = (1 << (length_struct.size * 8)) - 1

    if len(payload) > max_len:
        raise exc.validation(
            f"Chunked field exceeds maximum length {max_len}",
            code="core.crypto.chunked_field_too_large",
            details={"length": len(payload), "max": max_len},
        )

    parts.append(length_struct.pack(len(payload)))
    parts.append(payload)


# ....................... #


def pack_chunked_header(header: ChunkedHeader) -> bytes:
    """Serialize a :class:`ChunkedHeader` to its wire form (the stream prologue)."""

    if header.chunk_size < 1 or header.chunk_size > MAX_CHUNK_SIZE:
        raise exc.validation(
            f"Chunk size must be in [1, {MAX_CHUNK_SIZE}], got {header.chunk_size}",
            code="core.crypto.chunked_bad_chunk_size",
        )

    parts: list[bytes] = [_HEADER.pack(_CHUNK_MAGIC, _CHUNK_SCHEME_VERSION)]

    _put_var(parts, _U8, header.alg.encode("utf-8"))
    _put_var(parts, _U16, header.key_id.encode("utf-8"))
    _put_var(
        parts,
        _U8,
        b"" if header.key_version is None else header.key_version.encode("utf-8"),
    )
    _put_var(parts, _U16, header.wrapped_dek)
    parts.append(_U32.pack(header.chunk_size))

    return b"".join(parts)


# ....................... #


def seal_chunk(
    aead: Aead,
    *,
    key: bytes,
    base_aad: bytes,
    index: int,
    is_final: bool,
    plaintext: bytes,
) -> bytes:
    """Seal one plaintext chunk into a framed ``is_final | nonce | ciphertext`` record.

    The chunk is bound to its *index* and *is_final* flag through the AAD, so a
    reader that opens frames in order with the same base AAD detects reordering
    (index mismatch) and truncation (a missing final chunk).
    """

    aad = _chunk_aad(base_aad, index, is_final)
    nonce, ciphertext = aead.seal(key=key, plaintext=plaintext, aad=aad)

    parts: list[bytes] = [b"\x01" if is_final else b"\x00"]
    _put_var(parts, _U8, nonce)
    _put_var(parts, _U32, ciphertext)

    return b"".join(parts)


# ....................... #


def open_chunk(
    aead: Aead,
    *,
    key: bytes,
    base_aad: bytes,
    index: int,
    frame: ChunkFrame,
) -> bytes:
    """Verify and decrypt one parsed :class:`ChunkFrame` at position *index*.

    :raises CoreException: ``validation`` when authentication fails — a tampered,
        reordered (wrong *index*), or mis-flagged chunk.
    """

    aad = _chunk_aad(base_aad, index, frame.is_final)
    return aead.open(
        key=key, nonce=frame.nonce, ciphertext=frame.ciphertext, aad=aad
    )


# ....................... #


def _try_take(
    buf: bytes | bytearray, offset: int, length: int
) -> tuple[bytes, int] | None:
    end = offset + length
    if end > len(buf):
        return None
    return bytes(buf[offset:end]), end


def _try_take_var(
    buf: bytes | bytearray, offset: int, length_struct: struct.Struct
) -> tuple[bytes, int] | None:
    if offset + length_struct.size > len(buf):
        return None

    (length,) = length_struct.unpack_from(buf, offset)
    return _try_take(buf, offset + length_struct.size, length)


# ....................... #


def _try_parse_header(
    blob: bytes | bytearray, offset: int = 0
) -> tuple[ChunkedHeader, int] | None:
    """Parse a header at *offset*; ``None`` when incomplete, raising when malformed."""

    if len(blob) - offset < _HEADER.size:
        return None

    magic, scheme = _HEADER.unpack_from(blob, offset)

    if magic != _CHUNK_MAGIC:
        raise exc.validation(
            "Not a chunked-AEAD stream (missing magic marker)",
            code="core.crypto.chunked_bad_magic",
        )

    if scheme != _CHUNK_SCHEME_VERSION:
        raise exc.validation(
            f"Unsupported chunked scheme version {scheme}",
            code="core.crypto.chunked_unsupported_scheme",
            details={"scheme": scheme, "supported": _CHUNK_SCHEME_VERSION},
        )

    pos = offset + _HEADER.size
    fields: list[bytes] = []

    for length_struct in (_U8, _U16, _U8, _U16):
        taken = _try_take_var(blob, pos, length_struct)
        if taken is None:
            return None
        value, pos = taken
        fields.append(value)

    chunk_size_raw = _try_take(blob, pos, _U32.size)
    if chunk_size_raw is None:
        return None
    cs_bytes, pos = chunk_size_raw

    alg_raw, key_id_raw, key_version_raw, wrapped_dek = fields

    try:
        alg = alg_raw.decode("utf-8")
        key_id = key_id_raw.decode("utf-8")
        key_version = key_version_raw.decode("utf-8") or None
    except UnicodeDecodeError as error:
        raise exc.validation(
            "Malformed chunked header: text field is not valid UTF-8",
            code="core.crypto.chunked_bad_encoding",
        ) from error

    return (
        ChunkedHeader(
            alg=alg,
            key_id=key_id,
            key_version=key_version,
            wrapped_dek=wrapped_dek,
            chunk_size=int.from_bytes(cs_bytes, "big"),
        ),
        pos,
    )


# ....................... #


def _try_parse_frame(
    blob: bytes | bytearray, offset: int = 0
) -> tuple[ChunkFrame, int] | None:
    """Parse a frame at *offset*; ``None`` when the buffer lacks a complete frame."""

    if offset >= len(blob):
        return None

    is_final = blob[offset] != 0
    pos = offset + 1

    nonce_taken = _try_take_var(blob, pos, _U8)
    if nonce_taken is None:
        return None
    nonce, pos = nonce_taken

    ct_taken = _try_take_var(blob, pos, _U32)
    if ct_taken is None:
        return None
    ciphertext, pos = ct_taken

    return ChunkFrame(is_final=is_final, nonce=nonce, ciphertext=ciphertext), pos


# ....................... #


def unpack_chunked_header(blob: bytes) -> tuple[ChunkedHeader, int]:
    """Parse a chunked header from the start of *blob*, returning ``(header, header_len)``.

    Random-access counterpart to :meth:`ChunkedStreamReader.take_header`: the caller has
    fetched at least the header region, so an incomplete buffer is a truncation error.

    :raises CoreException: ``validation`` when *blob* is not a chunked stream, uses an
        unsupported scheme, or is truncated/malformed.
    """

    parsed = _try_parse_header(blob, 0)

    if parsed is None:
        raise exc.validation(
            "Truncated chunked header",
            code="core.crypto.chunked_truncated",
        )

    return parsed


# ....................... #


def parse_frame(blob: bytes, offset: int = 0) -> tuple[ChunkFrame, int]:
    """Parse one frame from *blob* at *offset*, returning ``(frame, end_offset)``.

    :raises CoreException: ``validation`` when the buffer does not hold a complete frame.
    """

    parsed = _try_parse_frame(blob, offset)

    if parsed is None:
        raise exc.validation(
            "Truncated chunked frame",
            code="core.crypto.chunked_truncated",
        )

    return parsed


# ....................... #


def chunk_frame_stride(blob: bytes, offset: int = 0) -> int | None:
    """Return a frame's total byte length from its length prefixes, or ``None`` if short.

    Reads only the framing (``is_final`` + nonce length + ciphertext length), not the
    ciphertext itself, so a small prefix read yields the per-chunk stride used to seek to
    later chunks — every non-final frame is exactly this size.
    """

    if offset + 2 > len(blob):
        return None

    nonce_len = blob[offset + 1]
    ct_len_pos = offset + 2 + nonce_len

    if ct_len_pos + _U32.size > len(blob):
        return None

    (ct_len,) = _U32.unpack_from(blob, ct_len_pos)

    return 2 + nonce_len + _U32.size + ct_len


# ....................... #


@final
@attrs.define(slots=True)
class ChunkedStreamReader:
    """Incremental parser: feed it arbitrary byte runs, take the header then frames.

    Backends yield bytes in transport-sized pieces that do not align to frame
    boundaries, so the reader buffers a partial frame until the rest arrives.
    Structural parsing only — decryption is :func:`open_chunk`.
    """

    _buf: bytearray = attrs.field(factory=bytearray, init=False, repr=False)
    _header_taken: bool = attrs.field(default=False, init=False)

    # ....................... #

    def feed(self, data: bytes) -> None:
        self._buf.extend(data)

    # ....................... #

    def take_header(self) -> ChunkedHeader | None:
        """Parse and consume the header once enough bytes are buffered, else ``None``."""

        if self._header_taken:
            raise exc.internal("Chunked header already taken from this reader")

        parsed = _try_parse_header(self._buf, 0)

        if parsed is None:
            return None

        header, end = parsed
        del self._buf[:end]
        self._header_taken = True

        return header

    # ....................... #

    def take_frames(self) -> Iterator[ChunkFrame]:
        """Yield and consume every complete frame currently buffered (partial stays)."""

        while True:
            parsed = _try_parse_frame(self._buf, 0)

            if parsed is None:
                return

            frame, end = parsed
            del self._buf[:end]
            yield frame

    # ....................... #

    def has_buffered_bytes(self) -> bool:
        """Whether unparsed bytes remain (e.g. trailing data after the final chunk)."""

        return len(self._buf) > 0

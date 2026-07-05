"""Tests for the chunked-AEAD wire format (framing, ordering, truncation resistance)."""

from __future__ import annotations

import pytest

from forze.base.crypto import (
    AesGcmAead,
    ChunkedHeader,
    ChunkedStreamReader,
    chunk_frame_stride,
    is_chunked_envelope,
    is_envelope,
    open_chunk,
    pack_chunked_header,
    pack_envelope,
    parse_frame,
    seal_chunk,
    unpack_chunked_header,
    unpack_envelope,
)
from forze.base.crypto.envelope import EncryptedEnvelope
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

_AEAD = AesGcmAead()
_DEK = b"k" * 32
_AAD = b"forze.storage|bucket|obj|tenant=None"


def _header(chunk_size: int = 64) -> ChunkedHeader:
    return ChunkedHeader(
        alg=_AEAD.algorithm,
        key_id="cmk",
        key_version="v1",
        wrapped_dek=b"wrapped-dek-bytes",
        chunk_size=chunk_size,
    )


def _seal_stream(chunks: list[tuple[bytes, bool]], *, aad: bytes = _AAD) -> bytes:
    """Serialize a header + a list of ``(plaintext, is_final)`` sealed chunks."""

    out = bytearray(pack_chunked_header(_header()))
    for index, (plaintext, is_final) in enumerate(chunks):
        out += seal_chunk(
            _AEAD,
            key=_DEK,
            base_aad=aad,
            index=index,
            is_final=is_final,
            plaintext=plaintext,
        )
    return bytes(out)


def _decrypt_stream(
    stream: bytes, *, aad: bytes = _AAD, feed_size: int | None = None
) -> bytes:
    """Drive the reader (optionally in tiny feed increments) and open chunks in order."""

    reader = ChunkedStreamReader()
    feeds = (
        [stream]
        if feed_size is None
        else [stream[i : i + feed_size] for i in range(0, len(stream), feed_size)]
    )

    header: ChunkedHeader | None = None
    out = bytearray()
    index = 0
    seen_final = False

    for piece in feeds:
        reader.feed(piece)

        if header is None:
            header = reader.take_header()
            if header is None:
                continue

        for frame in reader.take_frames():
            if seen_final:
                raise AssertionError("frame decoded after the final chunk")
            out += open_chunk(_AEAD, key=_DEK, base_aad=aad, index=index, frame=frame)
            index += 1
            seen_final = seen_final or frame.is_final

    assert header is not None
    assert seen_final, "stream ended without a final chunk (truncation)"
    assert not reader.has_buffered_bytes()
    return bytes(out)


# ....................... #


def test_header_round_trips() -> None:
    header = _header(chunk_size=1024)
    reader = ChunkedStreamReader()
    reader.feed(pack_chunked_header(header))

    parsed = reader.take_header()

    assert parsed == header


def test_single_chunk_round_trip() -> None:
    stream = _seal_stream([(b"hello world", True)])
    assert _decrypt_stream(stream) == b"hello world"


def test_multi_chunk_round_trip() -> None:
    chunks = [(b"aaaa", False), (b"bbbb", False), (b"cc", True)]
    stream = _seal_stream(chunks)
    assert _decrypt_stream(stream) == b"aaaabbbbcc"


def test_empty_final_chunk_round_trip() -> None:
    """A zero-length value is one final, empty chunk."""

    stream = _seal_stream([(b"", True)])
    assert _decrypt_stream(stream) == b""


def test_reassembles_across_arbitrary_feed_boundaries() -> None:
    chunks = [(bytes([i]) * 40, False) for i in range(5)] + [(b"tail", True)]
    stream = _seal_stream(chunks)

    # Feed one byte at a time — frames must reassemble regardless of transport chunking.
    assert _decrypt_stream(stream, feed_size=1) == b"".join(c for c, _ in chunks)


def test_tampered_ciphertext_is_rejected() -> None:
    stream = bytearray(_seal_stream([(b"secret payload", True)]))
    stream[-1] ^= 0x01  # flip a ciphertext/tag bit

    with pytest.raises(CoreException) as ei:
        _decrypt_stream(bytes(stream))
    assert ei.value.kind is ExceptionKind.VALIDATION


def test_reordered_chunks_are_rejected() -> None:
    """Swapping two chunks fails: the index is positional and bound into the AAD."""

    reader = ChunkedStreamReader()
    reader.feed(_seal_stream([(b"first---", False), (b"second--", False), (b"z", True)]))
    reader.take_header()
    frames = list(reader.take_frames())

    # Open frame[1] at position 0 (as if reordered) → AAD index mismatch → auth fail.
    with pytest.raises(CoreException):
        open_chunk(_AEAD, key=_DEK, base_aad=_AAD, index=0, frame=frames[1])


def test_flipping_final_flag_is_rejected() -> None:
    """The is_final flag is authenticated (in the AAD), so flipping the stored byte fails."""

    # A single non-final chunk with its final byte forced to 1.
    header = pack_chunked_header(_header())
    frame = bytearray(
        seal_chunk(_AEAD, key=_DEK, base_aad=_AAD, index=0, is_final=False, plaintext=b"x")
    )
    assert frame[0] == 0
    frame[0] = 1  # claim finality it was not sealed with

    reader = ChunkedStreamReader()
    reader.feed(header + bytes(frame))
    reader.take_header()
    (parsed,) = list(reader.take_frames())

    assert parsed.is_final is True
    with pytest.raises(CoreException):
        open_chunk(_AEAD, key=_DEK, base_aad=_AAD, index=0, frame=parsed)


def test_wrong_base_aad_is_rejected() -> None:
    stream = _seal_stream([(b"bound to context", True)])

    with pytest.raises(CoreException):
        _decrypt_stream(stream, aad=b"forze.storage|other-bucket|obj|tenant=None")


def test_truncation_leaves_no_final_chunk() -> None:
    """Dropping the terminating chunk yields a stream the consumer must reject."""

    reader = ChunkedStreamReader()
    reader.feed(_seal_stream([(b"part-one", False), (b"part-two", True)]))
    reader.take_header()
    frames = list(reader.take_frames())

    # Attacker keeps only the non-final chunk. It opens fine at its own index, but
    # the consumer never sees a final chunk — the truncation signal.
    assert frames[0].is_final is False
    opened = open_chunk(_AEAD, key=_DEK, base_aad=_AAD, index=0, frame=frames[0])
    assert opened == b"part-one"
    assert all(not f.is_final for f in frames[:1])


def test_is_chunked_envelope_discriminates_from_whole_payload() -> None:
    chunked = _seal_stream([(b"x", True)])
    whole = pack_envelope(
        EncryptedEnvelope(
            alg=_AEAD.algorithm,
            key_id="cmk",
            key_version=None,
            nonce=b"n" * 12,
            wrapped_dek=b"w",
            ciphertext=b"c",
        )
    )

    assert is_chunked_envelope(chunked) is True
    assert is_envelope(chunked) is False
    assert is_chunked_envelope(whole) is False
    assert is_envelope(whole) is True


def test_reader_rejects_whole_payload_magic() -> None:
    whole = pack_envelope(
        EncryptedEnvelope(
            alg=_AEAD.algorithm,
            key_id="cmk",
            key_version=None,
            nonce=b"n" * 12,
            wrapped_dek=b"w",
            ciphertext=b"c",
        )
    )
    reader = ChunkedStreamReader()
    reader.feed(whole)

    with pytest.raises(CoreException) as ei:
        reader.take_header()
    assert ei.value.code == "core.crypto.chunked_bad_magic"


def test_unpack_envelope_rejects_chunked_magic() -> None:
    with pytest.raises(CoreException):
        unpack_envelope(_seal_stream([(b"x", True)]))


def test_header_incomplete_returns_none_until_complete() -> None:
    full = pack_chunked_header(_header())
    reader = ChunkedStreamReader()

    reader.feed(full[:3])
    assert reader.take_header() is None  # not enough bytes yet

    reader.feed(full[3:])
    assert reader.take_header() == _header()


def test_bad_chunk_size_rejected() -> None:
    for bad in (0, (1 << 32)):
        with pytest.raises(CoreException):
            pack_chunked_header(
                ChunkedHeader(
                    alg=_AEAD.algorithm,
                    key_id="cmk",
                    key_version=None,
                    wrapped_dek=b"w",
                    chunk_size=bad,
                )
            )


# ....................... #
# random-access primitives (Phase 5)


def test_unpack_chunked_header_round_trips_with_length() -> None:
    header = _header(chunk_size=256)
    packed = pack_chunked_header(header)

    parsed, header_len = unpack_chunked_header(packed + b"following frame bytes")

    assert parsed == header
    assert header_len == len(packed)


def test_unpack_chunked_header_rejects_truncated() -> None:
    packed = pack_chunked_header(_header())
    with pytest.raises(CoreException):
        unpack_chunked_header(packed[:5])


def test_chunk_frame_stride_matches_frame_length() -> None:
    stream = _seal_stream([(b"a" * 64, False), (b"b" * 64, False), (b"c", True)])
    _header_obj, header_len = unpack_chunked_header(stream)

    _frame, frame_end = parse_frame(stream, header_len)

    assert chunk_frame_stride(stream, header_len) == frame_end - header_len


def test_chunk_frame_stride_none_when_prefix_incomplete() -> None:
    stream = _seal_stream([(b"x" * 64, True)])
    _header_obj, header_len = unpack_chunked_header(stream)

    # One byte past the header: not enough for the frame's length prefix.
    assert chunk_frame_stride(stream[: header_len + 1], header_len) is None


def test_parse_frame_raises_on_truncation() -> None:
    stream = _seal_stream([(b"x" * 64, True)])
    _header_obj, header_len = unpack_chunked_header(stream)

    with pytest.raises(CoreException):
        parse_frame(stream[: header_len + 3], header_len)


def test_random_access_opens_a_middle_chunk() -> None:
    """A chunk can be fetched and opened by index using stride arithmetic."""

    chunks = [(bytes([i]) * 64, False) for i in range(4)] + [(b"tail", True)]
    stream = _seal_stream(chunks)
    _header_obj, header_len = unpack_chunked_header(stream)
    stride = chunk_frame_stride(stream, header_len)
    assert stride is not None

    # Seek straight to chunk index 2 without parsing 0 and 1.
    frame, _end = parse_frame(stream, header_len + 2 * stride)
    plaintext = open_chunk(_AEAD, key=_DEK, base_aad=_AAD, index=2, frame=frame)

    assert plaintext == bytes([2]) * 64


def test_oversized_frame_rejected_before_buffering() -> None:
    """A frame declaring more ciphertext than the chunk size fails fast (bounded memory)."""

    import struct

    reader = ChunkedStreamReader()
    reader.feed(pack_chunked_header(_header(chunk_size=64)))
    assert reader.take_header() is not None

    # A frame claiming a ~1 GiB ciphertext while only a few bytes are actually fed.
    oversized = bytes([0, 12]) + b"n" * 12 + struct.pack(">I", 1 << 30) + b"xx"
    reader.feed(oversized)

    with pytest.raises(CoreException) as ei:
        list(reader.take_frames())
    assert ei.value.code == "core.crypto.chunked_frame_too_large"

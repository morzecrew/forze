"""Tests for the keyring's chunked streaming cipher (encrypt_stream / decrypt_stream)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    ChaCha20Poly1305Aead,
    KeyRef,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


def _keyring(kms: MockKeyManagement | None = None, *, directory=None, **kw) -> Keyring:
    return Keyring(
        kms=kms or MockKeyManagement(),
        aead=AesGcmAead(),
        directory=directory or StaticKeyDirectory(KeyRef(key_id="cmk")),
        **kw,
    )


async def _aiter(data: bytes, *, piece: int = 7) -> AsyncIterator[bytes]:
    for i in range(0, len(data), piece):
        yield data[i : i + piece]


async def _collect(source: AsyncIterator[bytes]) -> bytes:
    out = bytearray()
    async for piece in source:
        out += piece
    return bytes(out)


async def _seal(ring: Keyring, data: bytes, *, tenant=None, aad=b"", chunk_size=16) -> bytes:
    return await _collect(
        ring.encrypt_stream(
            _aiter(data), tenant=tenant, aad=aad, chunk_size=chunk_size
        )
    )


# ....................... #


@pytest.mark.parametrize(
    "size",
    [0, 1, 15, 16, 17, 48, 100],  # empty, sub-chunk, exact multiples, spanning
)
async def test_round_trip_various_sizes(size: int) -> None:
    ring = _keyring()
    data = bytes((i * 7) % 256 for i in range(size))

    stream = await _seal(ring, data, chunk_size=16)
    restored = await _collect(ring.decrypt_stream(_aiter(stream)))

    assert restored == data


async def test_cross_keyring_round_trip() -> None:
    """A fresh reader (cold cache, shared KMS) decrypts what a writer sealed."""

    kms = MockKeyManagement()
    writer = _keyring(kms)
    reader = _keyring(kms)

    data = b"x" * 200
    stream = await _seal(writer, data, chunk_size=32)

    assert await _collect(reader.decrypt_stream(_aiter(stream))) == data


async def test_ciphertext_is_not_plaintext_and_is_chunked() -> None:
    from forze.base.crypto import is_chunked_envelope, is_envelope

    ring = _keyring()
    stream = await _seal(ring, b"the quick brown fox" * 5, chunk_size=16)

    assert b"the quick brown fox" not in stream
    assert is_chunked_envelope(stream) is True
    assert is_envelope(stream) is False  # not the whole-payload format


async def test_wrong_aad_is_rejected() -> None:
    ring = _keyring()
    stream = await _seal(ring, b"bound payload" * 4, aad=b"ctx-a", chunk_size=16)

    with pytest.raises(CoreException) as ei:
        await _collect(ring.decrypt_stream(_aiter(stream), aad=b"ctx-b"))
    assert ei.value.kind is ExceptionKind.VALIDATION


async def test_tampered_ciphertext_is_rejected() -> None:
    ring = _keyring()
    stream = bytearray(await _seal(ring, b"secret payload" * 4, chunk_size=16))
    stream[-1] ^= 0x01  # corrupt the final chunk's tag

    with pytest.raises(CoreException):
        await _collect(ring.decrypt_stream(_aiter(bytes(stream))))


async def test_truncated_stream_is_rejected() -> None:
    """Dropping the tail leaves the reader with no final chunk → rejected."""

    ring = _keyring()
    stream = await _seal(ring, b"a" * 200, chunk_size=32)
    truncated = stream[:-20]  # cut into / drop the final frame

    with pytest.raises(CoreException) as ei:
        await _collect(ring.decrypt_stream(_aiter(truncated)))
    assert ei.value.code == "core.crypto.chunked_truncated"


async def test_algorithm_mismatch_is_rejected() -> None:
    kms = MockKeyManagement()
    writer = _keyring(kms)  # AES-256-GCM
    reader = Keyring(
        kms=kms,
        aead=ChaCha20Poly1305Aead(),  # deployment swapped the cipher
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    stream = await _seal(writer, b"payload" * 8, chunk_size=16)

    with pytest.raises(CoreException) as ei:
        await _collect(reader.decrypt_stream(_aiter(stream)))
    assert ei.value.code == "core.crypto.algorithm_mismatch"


async def test_foreign_tenant_key_id_is_rejected_before_unwrap() -> None:
    """A stream sealed for tenant A cannot be decrypted claiming tenant B."""

    kms = MockKeyManagement()
    directory = TenantTemplateKeyDirectory(
        template="tenant/{tenant_id}/cmk", default_key_id="default"
    )
    writer = _keyring(kms, directory=directory)
    reader = _keyring(kms, directory=directory)  # cold cache

    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    stream = await _seal(writer, b"a-secret" * 8, tenant=tenant_a, chunk_size=16)

    with pytest.raises(CoreException) as ei:
        await _collect(reader.decrypt_stream(_aiter(stream), tenant=tenant_b))
    assert ei.value.code == "core.crypto.key_id_unauthorized"

    # The rightful tenant still decrypts.
    restored = await _collect(reader.decrypt_stream(_aiter(stream), tenant=tenant_a))
    assert restored == b"a-secret" * 8


async def test_bad_chunk_size_rejected() -> None:
    ring = _keyring()

    with pytest.raises(CoreException) as ei:
        await _seal(ring, b"data", chunk_size=0)
    assert ei.value.code == "core.crypto.chunked_bad_chunk_size"


async def test_trailing_bytes_after_final_are_rejected() -> None:
    ring = _keyring()
    stream = await _seal(ring, b"payload" * 4, chunk_size=16)
    tampered = stream + b"\x00\x00garbage"

    with pytest.raises(CoreException) as ei:
        await _collect(ring.decrypt_stream(_aiter(tampered)))
    # Trailing garbage is rejected — either as trailing data, a truncated tail, or (when
    # the garbage parses as a frame header) an oversized-frame declaration.
    assert ei.value.code in (
        "core.crypto.chunked_trailing_data",
        "core.crypto.chunked_truncated",
        "core.crypto.chunked_frame_too_large",
    )


# ....................... #
# random-access opener (Phase 5)


async def test_open_chunked_stream_opens_frames_by_index() -> None:
    from forze.base.crypto import chunk_frame_stride, parse_frame, unpack_chunked_header

    ring = _keyring()
    data = b"".join(bytes([i]) * 32 for i in range(5))
    stream = await _seal(ring, data, chunk_size=32)

    opener = await ring.open_chunked_stream(stream, aad=b"")
    _header, header_len = unpack_chunked_header(stream)
    stride = chunk_frame_stride(stream, header_len)
    assert stride is not None
    assert opener.chunk_size == 32
    assert opener.header_len == header_len

    # Open chunk index 3 directly.
    frame, _end = parse_frame(stream, header_len + 3 * stride)
    assert opener.open_frame(3, frame) == bytes([3]) * 32


async def test_open_chunked_stream_rejects_foreign_tenant() -> None:
    kms = MockKeyManagement()
    directory = TenantTemplateKeyDirectory(
        template="tenant/{tenant_id}/cmk", default_key_id="default"
    )
    writer = _keyring(kms, directory=directory)
    reader = _keyring(kms, directory=directory)

    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    stream = await _seal(writer, b"secret" * 8, tenant=tenant_a, chunk_size=16)

    with pytest.raises(CoreException) as ei:
        await reader.open_chunked_stream(stream, aad=b"", tenant=tenant_b)
    assert ei.value.code == "core.crypto.key_id_unauthorized"

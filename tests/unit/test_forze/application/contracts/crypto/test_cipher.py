"""Round-trip and authentication tests for :class:`EnvelopeCipher`.

Exercises the provider-agnostic core through the in-memory mock backends, so the
full encrypt → pack → unpack → decrypt path runs with no infrastructure.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    EnvelopeCipher,
    KeyRef,
    is_envelope,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


def _cipher() -> EnvelopeCipher:
    return EnvelopeCipher(kms=MockKeyManagement(), aead=AesGcmAead())


_KEY = KeyRef(key_id="tenant/42/cmk")


# ....................... #


async def test_encrypt_decrypt_round_trip() -> None:
    cipher = _cipher()
    plaintext = b"alice@example.com"

    blob = await cipher.encrypt(plaintext, key_ref=_KEY)
    restored = await cipher.decrypt(blob)

    assert restored == plaintext


# ....................... #


async def test_ciphertext_is_a_self_describing_envelope() -> None:
    blob = await _cipher().encrypt(b"secret", key_ref=_KEY)

    assert is_envelope(blob)
    assert b"secret" not in blob  # plaintext is not present verbatim


# ....................... #


async def test_encrypt_is_non_deterministic() -> None:
    cipher = _cipher()

    first = await cipher.encrypt(b"same", key_ref=_KEY)
    second = await cipher.encrypt(b"same", key_ref=_KEY)

    assert first != second  # fresh data key + nonce per call


# ....................... #


async def test_associated_data_round_trips() -> None:
    cipher = _cipher()
    aad = b"tenant=42|field=email"

    blob = await cipher.encrypt(b"pii", key_ref=_KEY, aad=aad)

    assert await cipher.decrypt(blob, aad=aad) == b"pii"


# ....................... #


async def test_decrypt_with_wrong_aad_fails_authentication() -> None:
    cipher = _cipher()
    blob = await cipher.encrypt(b"pii", key_ref=_KEY, aad=b"tenant=42")

    with pytest.raises(CoreException) as excinfo:
        await cipher.decrypt(blob, aad=b"tenant=99")

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


async def test_decrypt_rejects_tampered_ciphertext() -> None:
    cipher = _cipher()
    blob = bytearray(await cipher.encrypt(b"pii", key_ref=_KEY))
    blob[-1] ^= 0xFF  # flip a ciphertext/tag byte

    with pytest.raises(CoreException) as excinfo:
        await cipher.decrypt(bytes(blob))

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


async def test_decrypt_rejects_non_envelope() -> None:
    with pytest.raises(CoreException) as excinfo:
        await _cipher().decrypt(b"not an envelope")

    assert excinfo.value.kind is ExceptionKind.VALIDATION

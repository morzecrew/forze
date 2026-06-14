"""Tests for the production AEAD ciphers in :mod:`forze.base.crypto.ciphers`."""

from __future__ import annotations

import os

import pytest

from forze.base.crypto import Aead, AesGcmAead, ChaCha20Poly1305Aead
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

_CIPHERS: list[Aead] = [AesGcmAead(), ChaCha20Poly1305Aead()]
_KEY = os.urandom(32)


# ....................... #


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_seal_open_round_trip(aead: Aead) -> None:
    nonce, ciphertext = aead.seal(key=_KEY, plaintext=b"secret", aad=b"ctx")

    assert aead.open(key=_KEY, nonce=nonce, ciphertext=ciphertext, aad=b"ctx") == b"secret"


# ....................... #


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_seal_is_non_deterministic(aead: Aead) -> None:
    first = aead.seal(key=_KEY, plaintext=b"same")
    second = aead.seal(key=_KEY, plaintext=b"same")

    assert first != second  # fresh random nonce per call


# ....................... #


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_open_rejects_tampered_ciphertext(aead: Aead) -> None:
    nonce, ciphertext = aead.seal(key=_KEY, plaintext=b"secret")
    tampered = bytes(ciphertext[:-1]) + bytes([ciphertext[-1] ^ 0xFF])

    with pytest.raises(CoreException) as excinfo:
        aead.open(key=_KEY, nonce=nonce, ciphertext=tampered)

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_open_rejects_wrong_aad(aead: Aead) -> None:
    nonce, ciphertext = aead.seal(key=_KEY, plaintext=b"secret", aad=b"ctx")

    with pytest.raises(CoreException) as excinfo:
        aead.open(key=_KEY, nonce=nonce, ciphertext=ciphertext, aad=b"other")

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_open_rejects_wrong_key(aead: Aead) -> None:
    nonce, ciphertext = aead.seal(key=_KEY, plaintext=b"secret")

    with pytest.raises(CoreException) as excinfo:
        aead.open(key=os.urandom(32), nonce=nonce, ciphertext=ciphertext)

    assert excinfo.value.kind is ExceptionKind.VALIDATION

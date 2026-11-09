"""Tests for the production AEAD ciphers in :mod:`forze.base.crypto.ciphers`."""

from __future__ import annotations

import os

import pytest

from forze.base.crypto import Aead, AesGcmAead, ChaCha20Poly1305Aead, bind_aead
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


# ....................... #


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_open_wrong_size_key_is_structured_error(aead: Aead) -> None:
    """A corrupted envelope yielding a wrong-size key surfaces as CoreException, not a
    raw ValueError that would bypass the codec's CoreException handler."""

    nonce, ciphertext = aead.seal(key=_KEY, plaintext=b"secret")

    with pytest.raises(CoreException) as excinfo:
        aead.open(key=os.urandom(31), nonce=nonce, ciphertext=ciphertext)

    assert excinfo.value.kind is ExceptionKind.VALIDATION
    # Distinct from a tamper failure, to diagnose key misconfiguration.
    assert excinfo.value.code == "core.crypto.aead_key_invalid"


# ....................... #


def test_chacha_open_wrong_size_nonce_is_structured_error() -> None:
    """ChaCha20 requires a 12-byte nonce; a corrupted shorter one (which raises
    ValueError, not InvalidTag) must still surface as CoreException."""

    aead = ChaCha20Poly1305Aead()
    _, ciphertext = aead.seal(key=_KEY, plaintext=b"secret")

    with pytest.raises(CoreException) as excinfo:
        aead.open(key=_KEY, nonce=b"too-short", ciphertext=ciphertext)

    assert excinfo.value.kind is ExceptionKind.VALIDATION
    assert excinfo.value.code == "core.crypto.aead_key_invalid"


# ....................... #
# bind / bind_aead: one cipher object per data key


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_bound_cipher_interoperates_with_the_unbound_one(aead: Aead) -> None:
    bound = bind_aead(aead, _KEY)

    nonce, ciphertext = bound.seal(plaintext=b"secret", aad=b"ctx")
    assert aead.open(key=_KEY, nonce=nonce, ciphertext=ciphertext, aad=b"ctx") == b"secret"

    nonce, ciphertext = aead.seal(key=_KEY, plaintext=b"secret", aad=b"ctx")
    assert bound.open(nonce=nonce, ciphertext=ciphertext, aad=b"ctx") == b"secret"

    assert bound.seal(plaintext=b"same") != bound.seal(plaintext=b"same")  # fresh nonce


# ....................... #


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_bound_open_maps_errors_like_the_unbound_open(aead: Aead) -> None:
    bound = bind_aead(aead, _KEY)
    nonce, ciphertext = bound.seal(plaintext=b"secret", aad=b"ctx")

    def code(**kwargs: bytes) -> str:
        args = {"nonce": nonce, "ciphertext": ciphertext, "aad": b"ctx", **kwargs}

        with pytest.raises(CoreException) as excinfo:
            bound.open(**args)

        with pytest.raises(CoreException) as unbound:
            aead.open(key=_KEY, **args)

        assert excinfo.value.code == unbound.value.code
        return excinfo.value.code

    assert code(aad=b"other") == "core.crypto.aead_auth_failed"
    assert code(ciphertext=ciphertext[:-1] + bytes([ciphertext[-1] ^ 1])) == (
        "core.crypto.aead_auth_failed"
    )
    assert code(nonce=b"") == "core.crypto.aead_key_invalid"


# ....................... #


@pytest.mark.parametrize("aead", _CIPHERS, ids=lambda a: a.algorithm)
def test_wrong_size_key_binds_to_the_per_call_path(aead: Aead) -> None:
    """A bad key cannot be pre-built, so it keeps the unbound path and its exact error."""

    short = os.urandom(31)
    nonce, ciphertext = aead.seal(key=_KEY, plaintext=b"secret")

    assert aead.bind(short) is None  # type: ignore[attr-defined]

    with pytest.raises(CoreException) as excinfo:
        bind_aead(aead, short).open(nonce=nonce, ciphertext=ciphertext)

    assert excinfo.value.code == "core.crypto.aead_key_invalid"


# ....................... #


def test_bind_aead_falls_back_for_an_aead_without_bind() -> None:
    calls: list[bytes] = []

    class _Plain:
        algorithm = "plain"

        def seal(self, *, key: bytes, plaintext: bytes, aad: bytes = b"") -> tuple[bytes, bytes]:
            calls.append(key)
            return b"n", plaintext + aad

        def open(self, *, key: bytes, nonce: bytes, ciphertext: bytes, aad: bytes = b"") -> bytes:
            calls.append(key)
            return ciphertext.removesuffix(aad)

    bound = bind_aead(_Plain(), b"k")
    nonce, ciphertext = bound.seal(plaintext=b"p", aad=b"a")

    assert bound.open(nonce=nonce, ciphertext=ciphertext, aad=b"a") == b"p"
    assert calls == [b"k", b"k"]  # the key is passed through on every call
    assert "b'k'" not in repr(bound)

"""Tests for :mod:`forze.base.crypto.envelope`."""

from __future__ import annotations

import pytest

from forze.base.crypto import (
    EncryptedEnvelope,
    ensure_algorithm,
    is_envelope,
    pack_envelope,
    unpack_envelope,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


def _envelope(**overrides: object) -> EncryptedEnvelope:
    fields: dict[str, object] = {
        "alg": "test-alg",
        "key_id": "tenant/42/cmk",
        "key_version": "v3",
        "nonce": b"0123456789ab",
        "wrapped_dek": b"\x00\x01\x02\x03" * 8,
        "ciphertext": b"the-ciphertext-bytes",
    }
    fields.update(overrides)
    return EncryptedEnvelope(**fields)  # type: ignore[arg-type]


# ....................... #


def test_round_trip_preserves_all_fields() -> None:
    env = _envelope()

    restored = unpack_envelope(pack_envelope(env))

    assert restored == env


# ....................... #


def test_round_trip_with_null_key_version() -> None:
    env = _envelope(key_version=None)

    restored = unpack_envelope(pack_envelope(env))

    assert restored.key_version is None
    assert restored == env


# ....................... #


def test_round_trip_with_empty_ciphertext() -> None:
    env = _envelope(ciphertext=b"")

    restored = unpack_envelope(pack_envelope(env))

    assert restored.ciphertext == b""


# ....................... #


def test_is_envelope_detects_packed_form() -> None:
    assert is_envelope(pack_envelope(_envelope())) is True


# ....................... #


@pytest.mark.parametrize("blob", [b"", b"plain text value", b"FZ", b"FZX...."])
def test_is_envelope_rejects_non_envelopes(blob: bytes) -> None:
    assert is_envelope(blob) is False


# ....................... #


def test_unpack_rejects_non_envelope() -> None:
    with pytest.raises(CoreException) as excinfo:
        unpack_envelope(b"definitely not an envelope")

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


def test_unpack_rejects_truncated_buffer() -> None:
    packed = pack_envelope(_envelope())

    with pytest.raises(CoreException) as excinfo:
        unpack_envelope(packed[:10])

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


def test_unpack_rejects_unknown_scheme_version() -> None:
    packed = bytearray(pack_envelope(_envelope()))
    packed[4] = 0xFF  # corrupt the scheme-version byte (after the 4-byte magic)

    with pytest.raises(CoreException) as excinfo:
        unpack_envelope(bytes(packed))

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


def test_unpack_rejects_invalid_utf8_text_field() -> None:
    """Tampered bytes in a text field raise validation, not a raw UnicodeDecodeError."""

    packed = bytearray(pack_envelope(_envelope()))
    # The alg field's first content byte sits just after the 5-byte header and its
    # 1-byte length prefix; a lone 0xFF is not valid UTF-8.
    packed[6] = 0xFF

    with pytest.raises(CoreException) as excinfo:
        unpack_envelope(bytes(packed))

    assert excinfo.value.kind is ExceptionKind.VALIDATION
    assert excinfo.value.code == "core.crypto.envelope_bad_encoding"


# ....................... #


def test_ensure_algorithm_passes_on_match() -> None:
    ensure_algorithm(_envelope(alg="AES-256-GCM"), "AES-256-GCM")  # no raise


# ....................... #


def test_ensure_algorithm_rejects_mismatch() -> None:
    with pytest.raises(CoreException) as excinfo:
        ensure_algorithm(_envelope(alg="AES-256-GCM"), "ChaCha20-Poly1305")

    assert excinfo.value.kind is ExceptionKind.VALIDATION
    assert excinfo.value.code == "core.crypto.algorithm_mismatch"

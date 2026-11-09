"""Tests for :mod:`forze.base.crypto.envelope`."""

from __future__ import annotations

import struct
from collections.abc import Callable

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from forze.base.crypto import (
    EncryptedEnvelope,
    ensure_algorithm,
    is_envelope,
    pack_envelope,
    unpack_envelope,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc

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


# ....................... #
# wire-format stability: golden bytes and parity with the reference parser

_GOLDEN = bytes.fromhex(
    "465a4576010b4145532d3235362d47434d0003636d6b0276310c6465666768696a6b6c6d6e6f"
    "0020fe01fe2849b897309ed3272b10aba4da64c99d15957c5e841caa01ebdb473b786a7aba07"
    "398c2eff5312338df4060590602d1812c8cef8f5e5bfd819885b545858"
)
"""An AES-256-GCM envelope packed by the original ``struct``-per-field implementation."""


def test_golden_envelope_parses_and_repacks_byte_identical() -> None:
    env = unpack_envelope(_GOLDEN)

    assert env.alg == "AES-256-GCM"
    assert env.key_id == "cmk"
    assert env.key_version == "v1"
    assert env.nonce == bytes(range(100, 112))
    assert len(env.wrapped_dek) == 32
    assert len(env.ciphertext) == len(b'"ada@example.com"') + 16
    assert pack_envelope(env) == _GOLDEN


# ....................... #


def _ref_take(blob: bytes, offset: int, length: int) -> tuple[bytes, int]:
    end = offset + length

    if end > len(blob):
        raise exc.validation(
            "Truncated envelope: declared field runs past end of buffer",
            code="core.crypto.envelope_truncated",
            details={"offset": offset, "need": length, "size": len(blob)},
        )

    return blob[offset:end], end


def _ref_take_var(blob: bytes, offset: int, length_struct: struct.Struct) -> tuple[bytes, int]:
    if offset + length_struct.size > len(blob):
        raise exc.validation(
            "Truncated envelope: missing length header",
            code="core.crypto.envelope_truncated",
            details={"offset": offset, "size": len(blob)},
        )

    (length,) = length_struct.unpack_from(blob, offset)
    return _ref_take(blob, offset + length_struct.size, length)


def _ref_unpack(blob: bytes) -> EncryptedEnvelope:
    """The original parser, verbatim — the oracle the faster one must match exactly."""

    if not (len(blob) >= 4 and blob[:4] == b"FZEv"):
        raise exc.validation(
            "Not a Forze envelope (missing magic marker)",
            code="core.crypto.envelope_bad_magic",
        )

    if len(blob) < 5:
        raise exc.validation(
            "Truncated envelope: missing header",
            code="core.crypto.envelope_truncated",
            details={"size": len(blob)},
        )

    _, scheme = struct.unpack_from(">4sB", blob, 0)

    if scheme != 1:
        raise exc.validation(
            f"Unsupported envelope scheme version {scheme}",
            code="core.crypto.envelope_unsupported_scheme",
            details={"scheme": scheme, "supported": 1},
        )

    u8, u16 = struct.Struct(">B"), struct.Struct(">H")
    offset = 5
    alg_raw, offset = _ref_take_var(blob, offset, u8)
    key_id_raw, offset = _ref_take_var(blob, offset, u16)
    key_version_raw, offset = _ref_take_var(blob, offset, u8)
    nonce, offset = _ref_take_var(blob, offset, u8)
    wrapped_dek, offset = _ref_take_var(blob, offset, u16)

    try:
        alg = alg_raw.decode("utf-8")
        key_id = key_id_raw.decode("utf-8")
        key_version = key_version_raw.decode("utf-8") or None
    except UnicodeDecodeError as error:
        raise exc.validation(
            "Malformed envelope: text field is not valid UTF-8",
            code="core.crypto.envelope_bad_encoding",
        ) from error

    return EncryptedEnvelope(
        alg=alg,
        key_id=key_id,
        key_version=key_version,
        nonce=nonce,
        wrapped_dek=wrapped_dek,
        ciphertext=blob[offset:],
    )


def _outcome(parse: Callable[[bytes], EncryptedEnvelope], blob: bytes) -> object:
    try:
        return parse(blob)
    except CoreException as error:
        return (error.kind, error.code, error.summary, error.details)


_texts = st.text(max_size=12)
_envelopes = st.builds(
    EncryptedEnvelope,
    alg=_texts,
    key_id=_texts,
    key_version=st.none() | st.text(min_size=1, max_size=8),
    nonce=st.binary(max_size=24),
    wrapped_dek=st.binary(max_size=48),
    ciphertext=st.binary(max_size=48),
)


@st.composite
def _blobs(draw: st.DrawFn) -> bytes:
    """Valid envelopes, their truncations and single-byte corruptions, and raw noise."""

    packed = pack_envelope(draw(_envelopes))
    choice = draw(st.integers(0, 3))

    if choice == 0:
        return packed

    if choice == 1:
        return packed[: draw(st.integers(0, len(packed)))]

    if choice == 2:
        at = draw(st.integers(0, len(packed) - 1))
        return packed[:at] + bytes([draw(st.integers(0, 255))]) + packed[at + 1 :]

    prefix = draw(st.sampled_from([b"", b"FZE", b"FZEv", b"FZEv\x01"]))
    return prefix + draw(st.binary(max_size=40))


@settings(max_examples=500, deadline=None)
@given(_blobs())
def test_unpack_matches_reference_parser_exactly(blob: bytes) -> None:
    """Same envelope on success; same kind, code, message and details on every failure."""

    assert _outcome(unpack_envelope, blob) == _outcome(_ref_unpack, blob)


@settings(max_examples=300, deadline=None)
@given(_blobs())
def test_any_parsed_envelope_repacks_to_its_exact_bytes(blob: bytes) -> None:
    # The layout has no slack, so parse → pack is the identity on every accepted input.
    try:
        env = unpack_envelope(blob)
    except CoreException:
        return

    assert pack_envelope(env) == blob


@settings(max_examples=200, deadline=None)
@given(_envelopes)
def test_pack_layout_is_unchanged(env: EncryptedEnvelope) -> None:
    def put(payload: bytes, fmt: str) -> bytes:
        return struct.pack(fmt, len(payload)) + payload

    expected = (
        b"FZEv\x01"
        + put(env.alg.encode(), ">B")
        + put(env.key_id.encode(), ">H")
        + put((env.key_version or "").encode(), ">B")
        + put(env.nonce, ">B")
        + put(env.wrapped_dek, ">H")
        + env.ciphertext
    )

    assert pack_envelope(env) == expected


@pytest.mark.parametrize(
    ("field", "value", "limit"),
    [
        ("alg", "a" * 256, 255),
        ("key_id", "k" * 65536, 65535),
        ("key_version", "v" * 256, 255),
        ("nonce", b"n" * 256, 255),
        ("wrapped_dek", b"w" * 65536, 65535),
    ],
)
def test_pack_rejects_an_oversized_field(field: str, value: object, limit: int) -> None:
    with pytest.raises(CoreException) as excinfo:
        pack_envelope(_envelope(**{field: value}))

    assert excinfo.value.code == "core.crypto.envelope_field_too_large"
    assert excinfo.value.summary == f"Envelope field exceeds maximum length {limit}"
    assert excinfo.value.details == {"length": limit + 1, "max": limit}

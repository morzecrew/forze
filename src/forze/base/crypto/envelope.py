"""Self-describing wire format for envelope-encrypted values.

An :class:`EncryptedEnvelope` carries everything needed to decrypt a value
*without* out-of-band metadata: which key (and key version) wrapped the data
key, which AEAD algorithm produced the ciphertext, the per-message nonce, the
wrapped data key, and the ciphertext itself. Because the envelope is
self-describing, rotating the key-encryption key forward never orphans existing
data — each stored value names the key version that decrypts it.

The packed form is a stable, length-prefixed binary layout prefixed with a magic
marker so callers can cheaply distinguish ciphertext from legacy plaintext (see
:func:`is_envelope`). The layout is pure ``struct`` — no third-party dependency.
"""

import struct
from typing import final

import attrs

from ..exceptions import exc

# ----------------------- #

_MAGIC = b"FZEv"
"""Magic marker identifying a packed Forze envelope (``FZE`` + format family)."""

_SCHEME_VERSION = 1
"""Current envelope scheme version (bumped on incompatible layout changes)."""

_HEADER = struct.Struct(">4sB")
"""Magic (4 bytes) + scheme version (1 byte)."""

_U8 = struct.Struct(">B")
_U16 = struct.Struct(">H")

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class EncryptedEnvelope:
    """Decrypt-everything-you-need metadata wrapped around a ciphertext.

    All byte fields are raw (not base64). Use :func:`pack_envelope` /
    :func:`unpack_envelope` to move between this object and its wire form.
    """

    alg: str
    """AEAD algorithm identifier that produced :attr:`ciphertext` (e.g. the
    value of :attr:`forze.base.crypto.Aead.algorithm`)."""

    key_id: str
    """Identifier of the key-encryption key (CMK) that wrapped the data key."""

    key_version: str | None
    """Version of the key-encryption key, when the backend exposes one. Carried
    so a rotated key can still resolve the version that wrapped historical data."""

    nonce: bytes
    """Per-message nonce/IV used by the AEAD seal operation."""

    wrapped_dek: bytes
    """Data-encryption key wrapped (encrypted) under the key-encryption key."""

    ciphertext: bytes
    """AEAD ciphertext (including any authentication tag the algorithm appends)."""


# ....................... #


def _put_var(parts: list[bytes], length_struct: struct.Struct, payload: bytes) -> None:
    max_len = (1 << (length_struct.size * 8)) - 1

    if len(payload) > max_len:
        raise exc.validation(
            f"Envelope field exceeds maximum length {max_len}",
            code="core.crypto.envelope_field_too_large",
            details={"length": len(payload), "max": max_len},
        )

    parts.append(length_struct.pack(len(payload)))
    parts.append(payload)


# ....................... #


def pack_envelope(envelope: EncryptedEnvelope) -> bytes:
    """Serialize an :class:`EncryptedEnvelope` to its self-describing wire form."""

    parts: list[bytes] = [_HEADER.pack(_MAGIC, _SCHEME_VERSION)]

    _put_var(parts, _U8, envelope.alg.encode("utf-8"))
    _put_var(parts, _U16, envelope.key_id.encode("utf-8"))
    _put_var(
        parts,
        _U8,
        b"" if envelope.key_version is None else envelope.key_version.encode("utf-8"),
    )
    _put_var(parts, _U8, envelope.nonce)
    _put_var(parts, _U16, envelope.wrapped_dek)

    # Ciphertext is the unbounded tail — no length prefix needed.
    parts.append(envelope.ciphertext)

    return b"".join(parts)


# ....................... #


def is_envelope(blob: bytes) -> bool:
    """Return whether *blob* begins with the Forze envelope magic marker.

    Cheap discriminator for read paths that must tolerate a mix of encrypted and
    legacy-plaintext values during a migration.
    """

    return len(blob) >= len(_MAGIC) and blob[: len(_MAGIC)] == _MAGIC


# ....................... #


def _take(blob: bytes, offset: int, length: int) -> tuple[bytes, int]:
    end = offset + length

    if end > len(blob):
        raise exc.validation(
            "Truncated envelope: declared field runs past end of buffer",
            code="core.crypto.envelope_truncated",
            details={"offset": offset, "need": length, "size": len(blob)},
        )

    return blob[offset:end], end


# ....................... #


def _take_var(
    blob: bytes,
    offset: int,
    length_struct: struct.Struct,
) -> tuple[bytes, int]:
    if offset + length_struct.size > len(blob):
        raise exc.validation(
            "Truncated envelope: missing length header",
            code="core.crypto.envelope_truncated",
            details={"offset": offset, "size": len(blob)},
        )

    (length,) = length_struct.unpack_from(blob, offset)
    return _take(blob, offset + length_struct.size, length)


# ....................... #


def ensure_algorithm(envelope: EncryptedEnvelope, cipher_algorithm: str) -> None:
    """Guard that *cipher_algorithm* can open *envelope*, else raise a clear error.

    The envelope names the AEAD that produced its ciphertext. If the wired cipher
    differs (e.g. the deployment switched ``AesGcmAead`` → ``ChaCha20Poly1305Aead``
    after data was written), opening would fail as an opaque ``aead_auth_failed``
    tamper error. This surfaces the real cause instead.
    """

    if envelope.alg != cipher_algorithm:
        raise exc.validation(
            f"Envelope was sealed with {envelope.alg!r} but the wired cipher is "
            f"{cipher_algorithm!r}; the matching AEAD is required to decrypt it",
            code="core.crypto.algorithm_mismatch",
            details={"envelope_alg": envelope.alg, "cipher_alg": cipher_algorithm},
        )


# ....................... #


def unpack_envelope(blob: bytes) -> EncryptedEnvelope:
    """Parse a packed envelope, validating its magic and scheme version.

    :raises CoreException: ``validation`` kind when *blob* is not a Forze
        envelope, uses an unsupported scheme version, or is truncated/malformed.
    """

    if not is_envelope(blob):
        raise exc.validation(
            "Not a Forze envelope (missing magic marker)",
            code="core.crypto.envelope_bad_magic",
        )

    if len(blob) < _HEADER.size:
        raise exc.validation(
            "Truncated envelope: missing header",
            code="core.crypto.envelope_truncated",
            details={"size": len(blob)},
        )

    _, scheme = _HEADER.unpack_from(blob, 0)

    if scheme != _SCHEME_VERSION:
        raise exc.validation(
            f"Unsupported envelope scheme version {scheme}",
            code="core.crypto.envelope_unsupported_scheme",
            details={"scheme": scheme, "supported": _SCHEME_VERSION},
        )

    offset = _HEADER.size
    alg_raw, offset = _take_var(blob, offset, _U8)
    key_id_raw, offset = _take_var(blob, offset, _U16)
    key_version_raw, offset = _take_var(blob, offset, _U8)
    nonce, offset = _take_var(blob, offset, _U8)
    wrapped_dek, offset = _take_var(blob, offset, _U16)
    ciphertext = blob[offset:]

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
        ciphertext=ciphertext,
    )

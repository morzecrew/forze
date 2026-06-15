"""Production AEAD implementations backed by ``cryptography`` (OpenSSL + Rust).

These are the default local ciphers for envelope encryption. AES-256-GCM runs
through OpenSSL with AES-NI hardware acceleration; ChaCha20-Poly1305 is the
software-friendly alternative for hosts without AES-NI. Both are authenticated:
:meth:`open` rejects any tampered ciphertext, nonce, or associated data.

The :class:`~forze.base.crypto.Aead` protocol stays the seam — a deployment can
substitute a FIPS module or HSM-backed cipher — but unless there is a specific
reason, :class:`AesGcmAead` is the right default.
"""

import os
from typing import final

import attrs
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM, ChaCha20Poly1305

from ..exceptions import exc

# ----------------------- #

_NONCE_SIZE = 12
"""96-bit nonce — the standard size for both GCM and ChaCha20-Poly1305."""

_AUTH_FAILED_CODE = "core.crypto.aead_auth_failed"
_KEY_INVALID_CODE = "core.crypto.aead_key_invalid"


# ....................... #


def _auth_failed() -> Exception:
    return exc.validation(
        "AEAD authentication failed (tampered ciphertext, wrong key, or aad)",
        code=_AUTH_FAILED_CODE,
    )


# ....................... #


def _key_invalid() -> Exception:
    # Wrong-size key or nonce — a misconfigured Transit key type or a corrupted
    # envelope, distinct from an authentication failure (tamper).
    return exc.validation(
        "AEAD key or nonce has wrong size (check Transit key type or envelope integrity)",
        code=_KEY_INVALID_CODE,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class AesGcmAead:
    """AES-256-GCM via ``cryptography`` — the default local cipher.

    Expects a 32-byte data key (AES-256). The 16-byte GCM tag is appended to the
    ciphertext, matching the :class:`~forze.base.crypto.Aead` contract.
    """

    @property
    def algorithm(self) -> str:
        return "AES-256-GCM"

    # ....................... #

    def seal(
        self,
        *,
        key: bytes,
        plaintext: bytes,
        aad: bytes = b"",
    ) -> tuple[bytes, bytes]:
        nonce = os.urandom(_NONCE_SIZE)
        ciphertext = AESGCM(key).encrypt(nonce, plaintext, aad)
        return nonce, ciphertext

    # ....................... #

    def open(
        self,
        *,
        key: bytes,
        nonce: bytes,
        ciphertext: bytes,
        aad: bytes = b"",
    ) -> bytes:
        try:
            return AESGCM(key).decrypt(nonce, ciphertext, aad)

        except InvalidTag as error:
            raise _auth_failed() from error

        # ValueError: wrong-size key — reachable via a corrupted/truncated envelope or a
        # misconfigured Transit key type (e.g. aes128-gcm96 yielding a 16-byte DEK). A
        # distinct code separates key misconfiguration from genuine tampering.
        except ValueError as error:
            raise _key_invalid() from error


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class ChaCha20Poly1305Aead:
    """ChaCha20-Poly1305 via ``cryptography`` — alternative for non-AES-NI hosts.

    Expects a 32-byte data key. The 16-byte Poly1305 tag is appended to the
    ciphertext.
    """

    @property
    def algorithm(self) -> str:
        return "ChaCha20-Poly1305"

    # ....................... #

    def seal(
        self,
        *,
        key: bytes,
        plaintext: bytes,
        aad: bytes = b"",
    ) -> tuple[bytes, bytes]:
        nonce = os.urandom(_NONCE_SIZE)
        ciphertext = ChaCha20Poly1305(key).encrypt(nonce, plaintext, aad)
        return nonce, ciphertext

    # ....................... #

    def open(
        self,
        *,
        key: bytes,
        nonce: bytes,
        ciphertext: bytes,
        aad: bytes = b"",
    ) -> bytes:
        try:
            return ChaCha20Poly1305(key).decrypt(nonce, ciphertext, aad)

        except InvalidTag as error:
            raise _auth_failed() from error

        # ValueError: wrong-size key or non-12-byte nonce — a misconfigured Transit key
        # type or a corrupted envelope (ChaCha20 requires exactly 12 bytes; GCM tolerates
        # other lengths). A distinct code separates misconfiguration from tampering.
        except ValueError as error:
            raise _key_invalid() from error

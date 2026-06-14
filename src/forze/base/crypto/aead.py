"""AEAD primitive protocol — the pluggable local symmetric cipher.

Envelope encryption splits into two operations: an *async* call to a key
manager that produces a per-message data-encryption key (DEK), and a *sync*,
local, fast symmetric encrypt/decrypt of the payload under that DEK. This module
defines the seam for the second half.

It is a :class:`~typing.Protocol` only — no concrete cipher lives in the
dependency-light ``forze.base`` layer. A production deployment wires an
authenticated cipher (e.g. AES-256-GCM or ChaCha20-Poly1305) from an integration
that owns the corresponding third-party dependency; tests wire an in-memory
implementation from ``forze_mock``.
"""

from typing import Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class Aead(Protocol):
    """Authenticated encryption with associated data, operating on a raw key.

    Implementations MUST be authenticated: :meth:`open` must reject any tampered
    ciphertext, nonce, or associated data. They MUST NOT require network or
    other async I/O — the DEK is already resolved by the time these run.
    """

    @property
    def algorithm(self) -> str:
        """Stable algorithm identifier recorded in the envelope (e.g.
        ``"AES-256-GCM"``). Read on decrypt to select a compatible cipher."""

        ...  # pragma: no cover

    def seal(
        self,
        *,
        key: bytes,
        plaintext: bytes,
        aad: bytes = b"",
    ) -> tuple[bytes, bytes]:
        """Encrypt *plaintext* under *key*, binding *aad*.

        :param key: Raw data-encryption key.
        :param plaintext: Bytes to encrypt.
        :param aad: Associated data authenticated but not encrypted (bind
            context such as tenant id and field name to resist record swaps).
        :returns: ``(nonce, ciphertext)`` where ``ciphertext`` includes the
            authentication tag.
        """

        ...  # pragma: no cover

    def open(
        self,
        *,
        key: bytes,
        nonce: bytes,
        ciphertext: bytes,
        aad: bytes = b"",
    ) -> bytes:
        """Verify and decrypt *ciphertext* produced by :meth:`seal`.

        :param aad: Must equal the value passed to :meth:`seal`.
        :raises CoreException: when authentication fails (tampered ciphertext,
            wrong key, or mismatched ``aad``).
        """

        ...  # pragma: no cover

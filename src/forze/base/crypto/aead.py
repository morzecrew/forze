"""AEAD primitive protocol — the pluggable local symmetric cipher.

Envelope encryption splits into two operations: an *async* call to a key
manager that produces a per-message data-encryption key (DEK), and a *sync*,
local, fast symmetric encrypt/decrypt of the payload under that DEK. This module
defines the seam for the second half.

The seam is a :class:`~typing.Protocol` — no concrete cipher lives in this
module. A production deployment wires an
authenticated cipher (e.g. AES-256-GCM or ChaCha20-Poly1305) from an integration
that owns the corresponding third-party dependency; tests wire an in-memory
implementation from ``forze_mock``.

:func:`bind_aead` keys an AEAD once for a cached data key (:class:`KeyedAead`), so a
caller sealing or opening many values under it does not rebuild the cipher per value.
"""

from typing import Protocol, final, runtime_checkable

import attrs

# ----------------------- #


@runtime_checkable
class Aead(Protocol):
    """Authenticated encryption with associated data, operating on a raw key.

    Implementations MUST be authenticated: :meth:`open` must reject any tampered
    ciphertext, nonce, or associated data. They MUST NOT require network or
    other async I/O — the DEK is already resolved by the time these run.

    An implementation MAY also offer ``bind(key) -> KeyedAead | None`` to build its
    cipher once per key (see :func:`bind_aead`); ``None`` means "use the per-call path".
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


# ....................... #


class KeyedAead(Protocol):
    """An :class:`Aead` bound to one data key — what :func:`bind_aead` returns.

    :meth:`seal` and :meth:`open` behave exactly like the unbound AEAD's, with the key
    fixed. Built once per cached data key so the per-value calls skip rebuilding the
    cipher (and its key schedule) every time.
    """

    def seal(self, *, plaintext: bytes, aad: bytes = b"") -> tuple[bytes, bytes]:
        """:meth:`Aead.seal` under the bound key."""

        ...  # pragma: no cover

    def open(self, *, nonce: bytes, ciphertext: bytes, aad: bytes = b"") -> bytes:
        """:meth:`Aead.open` under the bound key."""

        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class _SupportsBind(Protocol):
    """An AEAD that can pre-build its cipher for one key (the shipped ciphers do)."""

    def bind(self, key: bytes) -> KeyedAead | None: ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class _PerCallAead:
    """Fallback :class:`KeyedAead` for an AEAD that cannot pre-build: passes the key per call."""

    aead: Aead
    """The unbound AEAD."""

    key: bytes = attrs.field(repr=False)
    """Raw data key — ``repr`` suppressed."""

    # ....................... #

    def seal(self, *, plaintext: bytes, aad: bytes = b"") -> tuple[bytes, bytes]:
        return self.aead.seal(key=self.key, plaintext=plaintext, aad=aad)

    # ....................... #

    def open(self, *, nonce: bytes, ciphertext: bytes, aad: bytes = b"") -> bytes:
        return self.aead.open(key=self.key, nonce=nonce, ciphertext=ciphertext, aad=aad)


# ....................... #


def bind_aead(aead: Aead, key: bytes) -> KeyedAead:
    """Bind *aead* to *key* for repeated seals and opens under that one data key.

    Uses the AEAD's own ``bind`` when it has one and the key is usable (the shipped
    ciphers build their ``cryptography`` object once); otherwise the result passes *key*
    to :meth:`Aead.seal` / :meth:`Aead.open` on every call, so any error those raise for
    a bad key is raised unchanged, at the same point.
    """

    bound = aead.bind(key) if isinstance(aead, _SupportsBind) else None

    return bound if bound is not None else _PerCallAead(aead, key)

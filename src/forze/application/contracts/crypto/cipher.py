"""Envelope cipher — composes a key manager and an AEAD into value crypto.

This is the provider-agnostic core of envelope encryption. It performs one async
key-management call plus one local AEAD operation per value, and emits a
self-describing :class:`~forze.base.crypto.EncryptedEnvelope` so the decrypt path
needs nothing but the ciphertext and the matching associated data.

Phase 0 generates a data key per value; data-key caching / reuse (one key per
tenant per key version) is a later optimization layered above this seam.
"""

from typing import final

import attrs

from forze.base.crypto import (
    Aead,
    EncryptedEnvelope,
    ensure_algorithm,
    pack_envelope,
    unpack_envelope,
)

from .ports import KeyManagementPort
from .value_objects import KeyRef

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class EnvelopeCipher:
    """Encrypt/decrypt opaque byte values using envelope encryption."""

    kms: KeyManagementPort
    """Key manager that generates and unwraps data-encryption keys."""

    aead: Aead
    """Local authenticated cipher applied under each data key."""

    # ....................... #

    async def encrypt(
        self,
        plaintext: bytes,
        *,
        key_ref: KeyRef,
        aad: bytes = b"",
    ) -> bytes:
        """Encrypt *plaintext*, returning a packed, self-describing envelope.

        :param key_ref: Key-encryption key to wrap the per-value data key under.
        :param aad: Associated data bound into the ciphertext (e.g. tenant id +
            field name); the identical value is required to decrypt.
        """

        data_key = await self.kms.generate_data_key(key_ref)

        nonce, ciphertext = self.aead.seal(
            key=data_key.plaintext,
            plaintext=plaintext,
            aad=aad,
        )

        envelope = EncryptedEnvelope(
            alg=self.aead.algorithm,
            key_id=data_key.key_id,
            key_version=data_key.key_version,
            nonce=nonce,
            wrapped_dek=data_key.wrapped,
            ciphertext=ciphertext,
        )

        return pack_envelope(envelope)

    # ....................... #

    async def decrypt(self, blob: bytes, *, aad: bytes = b"") -> bytes:
        """Decrypt a packed envelope produced by :meth:`encrypt`.

        The key-encryption key and version are read from the envelope, so values
        wrapped under a now-rotated key still decrypt.

        :param aad: Must equal the value passed to :meth:`encrypt`.
        :raises CoreException: ``validation`` for a malformed envelope, an
            algorithm mismatch between the envelope and the wired cipher, or an
            authentication failure surfaced by the AEAD on tamper / wrong ``aad``.
        """

        envelope = unpack_envelope(blob)
        ensure_algorithm(envelope, self.aead.algorithm)

        data_key = await self.kms.unwrap_data_key(
            wrapped=envelope.wrapped_dek,
            key_ref=KeyRef(key_id=envelope.key_id, version=envelope.key_version),
        )

        return self.aead.open(
            key=data_key,
            nonce=envelope.nonce,
            ciphertext=envelope.ciphertext,
            aad=aad,
        )

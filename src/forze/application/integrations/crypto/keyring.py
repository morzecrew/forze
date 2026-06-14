"""Keyring — the tenant-aware, caching bridge between async KMS and value crypto.

The keyring is the process-wide service that integration adapters call to encrypt
and decrypt byte values. It:

- resolves the tenant's key-encryption key via a :class:`KeyDirectoryPort`
  (single-key or per-tenant / BYOK),
- performs envelope encryption with a :class:`~forze.base.crypto.Aead`, and
- caches data keys so a key-encryption-key round-trip is amortized: an active
  data key is reused for many values on the encrypt path (bounded by
  ``max_dek_messages``), and unwrapped data keys are cached on the decrypt path.

Caching is what later lets a *synchronous* codec encrypt/decrypt after an async
``warm`` — see Phase 2. On the async seams wired now (object storage) it is a
pure latency optimization: a cold value simply pays one KMS call inline.
"""

import asyncio
from typing import final

import attrs

from forze.application.contracts.crypto import (
    KeyDirectoryPort,
    KeyManagementPort,
    KeyRef,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.crypto import Aead, EncryptedEnvelope, pack_envelope, unpack_envelope

# ----------------------- #


@final
@attrs.define(slots=True)
class _ActiveDataKey:
    """A cached data key used for encryption, with a reuse counter."""

    plaintext: bytes
    wrapped: bytes
    key_id: str
    key_version: str | None
    uses: int = 0


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class Keyring:
    """Tenant-aware envelope cipher with data-key caching (a :class:`BytesCipherPort`)."""

    kms: KeyManagementPort
    """Backend that generates and unwraps data keys."""

    aead: Aead
    """Local authenticated cipher applied under each data key."""

    directory: KeyDirectoryPort
    """Resolves a tenant to its key-encryption-key reference."""

    max_dek_messages: int = 1 << 20
    """Reuse an active data key for at most this many encryptions before
    regenerating (bounds GCM nonce-collision risk and limits blast radius)."""

    decrypt_cache_max: int = 1024
    """Maximum number of unwrapped data keys to keep on the decrypt path."""

    _enc_cache: dict[str, _ActiveDataKey] = attrs.field(factory=dict, init=False)
    """key_id → active data key (encrypt path)."""

    _dec_cache: dict[bytes, bytes] = attrs.field(factory=dict, init=False)
    """wrapped data key → plaintext data key (decrypt path)."""

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False, repr=False)
    """Serializes cache fills so a cold key triggers a single KMS call."""

    # ....................... #

    async def encrypt(
        self,
        plaintext: bytes,
        *,
        tenant: TenantIdentity | None,
        aad: bytes = b"",
    ) -> bytes:
        """Encrypt *plaintext* under *tenant*'s key, returning a packed envelope."""

        key_ref = await self.directory.resolve(tenant)
        dek = await self._active_data_key(key_ref)
        nonce, ciphertext = self.aead.seal(
            key=dek.plaintext,
            plaintext=plaintext,
            aad=aad,
        )

        envelope = EncryptedEnvelope(
            alg=self.aead.algorithm,
            key_id=dek.key_id,
            key_version=dek.key_version,
            nonce=nonce,
            wrapped_dek=dek.wrapped,
            ciphertext=ciphertext,
        )

        return pack_envelope(envelope)

    # ....................... #

    async def decrypt(self, blob: bytes, *, aad: bytes = b"") -> bytes:
        """Decrypt a packed envelope; the key is resolved from the envelope itself."""

        envelope = unpack_envelope(blob)
        dek = await self._unwrap(envelope)

        return self.aead.open(
            key=dek,
            nonce=envelope.nonce,
            ciphertext=envelope.ciphertext,
            aad=aad,
        )

    # ....................... #

    async def warm(self, tenant: TenantIdentity | None) -> None:
        """Pre-resolve *tenant*'s active data key so the next encrypt pays no KMS call."""

        key_ref = await self.directory.resolve(tenant)
        await self._active_data_key(key_ref)

    # ....................... #

    async def _active_data_key(self, key_ref: KeyRef) -> _ActiveDataKey:
        async with self._lock:
            cached = self._enc_cache.get(key_ref.key_id)

            if cached is not None and cached.uses < self.max_dek_messages:
                cached.uses += 1
                return cached

            data_key = await self.kms.generate_data_key(key_ref)
            active = _ActiveDataKey(
                plaintext=data_key.plaintext,
                wrapped=data_key.wrapped,
                key_id=data_key.key_id,
                key_version=data_key.key_version,
                uses=1,
            )
            self._enc_cache[key_ref.key_id] = active
            # Seed the decrypt cache so a read-after-write is a hit.
            self._dec_cache[data_key.wrapped] = data_key.plaintext
            return active

    # ....................... #

    async def _unwrap(self, envelope: EncryptedEnvelope) -> bytes:
        cached = self._dec_cache.get(envelope.wrapped_dek)

        if cached is not None:
            return cached

        async with self._lock:
            cached = self._dec_cache.get(envelope.wrapped_dek)

            if cached is not None:
                return cached

            dek = await self.kms.unwrap_data_key(
                wrapped=envelope.wrapped_dek,
                key_ref=KeyRef(key_id=envelope.key_id, version=envelope.key_version),
            )

            if len(self._dec_cache) >= self.decrypt_cache_max:
                self._dec_cache.clear()

            self._dec_cache[envelope.wrapped_dek] = dek
            return dek

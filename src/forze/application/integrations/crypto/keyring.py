"""Keyring — the tenant-aware, caching bridge between async KMS and value crypto.

The keyring is the process-wide service that integration adapters call to encrypt
and decrypt byte values. It:

- resolves the tenant's key-encryption key via a :class:`KeyDirectoryPort`
  (single-key or per-tenant / BYOK),
- performs envelope encryption with a :class:`~forze.base.crypto.Aead`, and
- caches data keys so a key-encryption-key round-trip is amortized: an active
  data key is reused for many values on the encrypt path (bounded by
  ``max_dek_messages``), and unwrapped data keys are cached on the decrypt path.

Caching is also what lets a *synchronous* codec encrypt/decrypt: an async
pre-pass (:meth:`Keyring.warm` before a sync encode, :meth:`Keyring.ensure_unwrapped`
before a sync decode) primes the cache, then :meth:`Keyring.encrypt_sync` /
:meth:`Keyring.decrypt_sync` run purely against it — raising ``cipher_not_warm``
on a cold miss rather than blocking. On the async seams (object storage) the
cache is a pure latency optimization: a cold value simply pays one KMS call inline.
"""

import asyncio
from collections import OrderedDict
from typing import Any, Iterable, final

import attrs

from forze.application.contracts.crypto import (
    CryptoKeyringStats,
    KeyDirectoryPort,
    KeyManagementPort,
    KeyRef,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.crypto import (
    Aead,
    EncryptedEnvelope,
    ensure_algorithm,
    pack_envelope,
    unpack_envelope,
)
from forze.base.exceptions import exc

# ----------------------- #

_NONE_TENANT = "\x00none"
"""Cache key for the no-tenant (single-key) case."""

_LOCK_STRIPES = 64
"""Fixed number of fill-lock stripes (bounds the lock set regardless of key count)."""


def _tenant_cache_key(tenant: TenantIdentity | None) -> str:
    return _NONE_TENANT if tenant is None else str(tenant.tenant_id)


def _not_warm(operation: str) -> Exception:
    return exc.internal(
        f"Keyring not warmed for {operation}: run the async pre-pass "
        f"(warm()/ensure_unwrapped()) before the synchronous codec {operation}.",
        code="core.crypto.cipher_not_warm",
    )


def _lru_get(cache: OrderedDict[Any, Any], key: Any) -> Any:
    value = cache.get(key)

    if value is not None:
        cache.move_to_end(key)

    return value


def _lru_put(cache: OrderedDict[Any, Any], key: Any, value: Any, *, cap: int) -> None:
    cache[key] = value
    cache.move_to_end(key)

    while len(cache) > cap:
        cache.popitem(last=False)


# ....................... #


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
    """Maximum unwrapped data keys to keep on the decrypt path (LRU)."""

    enc_cache_max: int = 1024
    """Maximum active data keys / tenant→key entries to keep (LRU). Bounds memory
    in deployments with many distinct tenants/keys; eviction just re-fetches."""

    _enc_cache: OrderedDict[str, _ActiveDataKey] = attrs.field(
        factory=OrderedDict, init=False
    )
    """key_id → active data key (encrypt path, LRU)."""

    _dec_cache: OrderedDict[bytes, bytes] = attrs.field(
        factory=OrderedDict, init=False
    )
    """wrapped data key → plaintext data key (decrypt path, LRU)."""

    _tenant_key: OrderedDict[str, str] = attrs.field(factory=OrderedDict, init=False)
    """tenant cache key → resolved key_id (lets the sync path skip the directory, LRU)."""

    _locks: tuple[asyncio.Lock, ...] = attrs.field(
        factory=lambda: tuple(asyncio.Lock() for _ in range(_LOCK_STRIPES)),
        init=False,
        repr=False,
    )
    """Fixed stripe of fill locks, indexed by key_id hash. A cold key still triggers a
    single KMS call while different keys (e.g. tenants) fill in parallel; the stripe
    bounds the lock set so it can't grow unbounded with rotating/per-tenant key_ids.
    Two key_ids that collide on a stripe just serialize their fills (harmless)."""

    # Cumulative observability counters (sampled by ``instrument_crypto``). Plain ints:
    # increments happen on the event loop thread / sync codec path, never concurrently.
    _n_generated: int = attrs.field(default=0, init=False)
    _n_unwrapped: int = attrs.field(default=0, init=False)
    _n_enc_hits: int = attrs.field(default=0, init=False)
    _n_dec_hits: int = attrs.field(default=0, init=False)
    _n_cold: int = attrs.field(default=0, init=False)

    # ....................... #

    def stats(self) -> CryptoKeyringStats:
        """Snapshot the keyring's cumulative KMS + cache counters for metrics export."""

        return CryptoKeyringStats(
            data_keys_generated=self._n_generated,
            data_keys_unwrapped=self._n_unwrapped,
            encrypt_cache_hits=self._n_enc_hits,
            decrypt_cache_hits=self._n_dec_hits,
            cold_misses=self._n_cold,
        )

    # ....................... #

    def _lock_for(self, key_id: str) -> asyncio.Lock:
        # Deterministic key_id → stripe mapping: the lock object is stable for a given
        # key_id within the process, so mutual exclusion per key holds.
        return self._locks[hash(key_id) % len(self._locks)]

    # ....................... #

    async def _resolve_key_ref(self, tenant: TenantIdentity | None) -> KeyRef:
        key_ref = await self.directory.resolve(tenant)
        _lru_put(
            self._tenant_key,
            _tenant_cache_key(tenant),
            key_ref.key_id,
            cap=self.enc_cache_max,
        )
        return key_ref

    # ....................... #

    async def encrypt(
        self,
        plaintext: bytes,
        *,
        tenant: TenantIdentity | None,
        aad: bytes = b"",
    ) -> bytes:
        """Encrypt *plaintext* under *tenant*'s key, returning a packed envelope."""

        key_ref = await self._resolve_key_ref(tenant)
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
        ensure_algorithm(envelope, self.aead.algorithm)
        dek = await self._unwrap(envelope)

        return self.aead.open(
            key=dek,
            nonce=envelope.nonce,
            ciphertext=envelope.ciphertext,
            aad=aad,
        )

    # ....................... #

    async def warm(self, tenant: TenantIdentity | None) -> None:
        """Pre-resolve *tenant*'s active data key so a later (a)sync encrypt pays no KMS call.

        Prefetch only — it does **not** spend an encryption from the key's
        ``max_dek_messages`` budget, so warming a key one use short of the limit
        still leaves room for the encrypt the warm was preparing for.
        """

        key_ref = await self._resolve_key_ref(tenant)
        await self._active_data_key(key_ref, consume=False)

    # ....................... #

    async def ensure_unwrapped(self, envelopes: Iterable[EncryptedEnvelope]) -> None:
        """Unwrap and cache the data keys for *envelopes* so sync decrypts hit.

        The read pre-pass: with per-tenant data-key reuse a result set carries
        only a handful of distinct wrapped keys, so this is a few KMS calls
        regardless of row count. A same-process read-after-write is already a
        cache hit and unwraps nothing.
        """

        for envelope in envelopes:
            if envelope.wrapped_dek not in self._dec_cache:
                await self._unwrap(envelope)

    # ....................... #

    def encrypt_sync(
        self,
        plaintext: bytes,
        *,
        tenant: TenantIdentity | None,
        aad: bytes = b"",
    ) -> bytes:
        """Encrypt against the warmed cache (no awaits); requires a prior :meth:`warm`."""

        key_id = _lru_get(self._tenant_key, _tenant_cache_key(tenant))
        active = _lru_get(self._enc_cache, key_id) if key_id is not None else None

        if active is None or active.uses >= self.max_dek_messages:
            self._n_cold += 1
            raise _not_warm("encrypt")

        self._n_enc_hits += 1
        active.uses += 1
        nonce, ciphertext = self.aead.seal(
            key=active.plaintext,
            plaintext=plaintext,
            aad=aad,
        )

        return pack_envelope(
            EncryptedEnvelope(
                alg=self.aead.algorithm,
                key_id=active.key_id,
                key_version=active.key_version,
                nonce=nonce,
                wrapped_dek=active.wrapped,
                ciphertext=ciphertext,
            )
        )

    # ....................... #

    def decrypt_sync(self, blob: bytes, *, aad: bytes = b"") -> bytes:
        """Decrypt against the warmed cache (no awaits); requires a prior unwrap.

        A same-process read-after-write hits the cache seeded at encrypt time;
        otherwise call :meth:`ensure_unwrapped` for the rows first.
        """

        envelope = unpack_envelope(blob)
        ensure_algorithm(envelope, self.aead.algorithm)
        dek = _lru_get(self._dec_cache, envelope.wrapped_dek)

        if dek is None:
            self._n_cold += 1
            raise _not_warm("decrypt")

        self._n_dec_hits += 1

        return self.aead.open(
            key=dek,
            nonce=envelope.nonce,
            ciphertext=envelope.ciphertext,
            aad=aad,
        )

    # ....................... #

    async def _active_data_key(
        self, key_ref: KeyRef, *, consume: bool = True
    ) -> _ActiveDataKey:
        """Resolve (and cache) the active data key for *key_ref*.

        *consume* records that the caller will perform one encryption with the key:
        it bumps the reuse counter and the cache-hit metric. :meth:`warm` passes
        ``consume=False`` to prefetch without spending budget — a non-consuming
        prefetch of a fresh key starts at ``uses=0`` so the encrypt it primes is
        the one that counts.
        """

        async with self._lock_for(key_ref.key_id):
            cached = _lru_get(self._enc_cache, key_ref.key_id)

            if cached is not None and cached.uses < self.max_dek_messages:
                if consume:
                    self._n_enc_hits += 1
                    cached.uses += 1
                return cached

            self._n_generated += 1
            data_key = await self.kms.generate_data_key(key_ref)
            active = _ActiveDataKey(
                plaintext=data_key.plaintext,
                wrapped=data_key.wrapped,
                key_id=data_key.key_id,
                key_version=data_key.key_version,
                uses=1 if consume else 0,
            )
            _lru_put(
                self._enc_cache, key_ref.key_id, active, cap=self.enc_cache_max
            )
            # Seed the decrypt cache so a read-after-write is a hit.
            _lru_put(
                self._dec_cache,
                data_key.wrapped,
                data_key.plaintext,
                cap=self.decrypt_cache_max,
            )
            return active

    # ....................... #

    async def _unwrap(self, envelope: EncryptedEnvelope) -> bytes:
        cached = _lru_get(self._dec_cache, envelope.wrapped_dek)

        if cached is not None:
            self._n_dec_hits += 1
            return cached

        async with self._lock_for(envelope.key_id):
            cached = _lru_get(self._dec_cache, envelope.wrapped_dek)

            if cached is not None:
                self._n_dec_hits += 1
                return cached

            self._n_unwrapped += 1
            dek = await self.kms.unwrap_data_key(
                wrapped=envelope.wrapped_dek,
                key_ref=KeyRef(key_id=envelope.key_id, version=envelope.key_version),
            )
            _lru_put(
                self._dec_cache,
                envelope.wrapped_dek,
                dek,
                cap=self.decrypt_cache_max,
            )
            return dek

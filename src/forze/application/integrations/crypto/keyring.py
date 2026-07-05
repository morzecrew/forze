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
import zlib
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
from forze.base.primitives import current_time_source

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


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class _FrozenFieldCipher:
    """Thread-safe, decrypt-only snapshot of a :class:`Keyring`'s field-cipher path.

    Built by :meth:`Keyring.freeze_decryptor` from pre-resolved data keys, so
    :meth:`decrypt_sync` does pure AEAD against a plain dict — no shared, LRU-mutating cache —
    and is safe to call off the event loop (e.g. under ``run_cpu_map``). A
    :class:`~forze.application.contracts.crypto.FieldCipherPort` structurally: ``warm`` /
    ``ensure_unwrapped`` are no-ops (already resolved) and encrypt is unsupported."""

    aead: Aead
    """The AEAD, shared with the source keyring (stateless, thread-safe)."""

    deks: dict[bytes, bytes] = attrs.field(repr=False)
    """Resolved ``wrapped_dek → data key`` snapshot for this batch (``repr`` suppressed
    — the values are plaintext data keys)."""

    # ....................... #

    async def warm(self, tenant: TenantIdentity | None) -> None:
        return None

    async def ensure_unwrapped(
        self,
        envelopes: Iterable[EncryptedEnvelope],
        *,
        tenant: TenantIdentity | None = None,
    ) -> None:
        return None

    def encrypt_sync(
        self, plaintext: bytes, *, tenant: TenantIdentity | None, aad: bytes = b""
    ) -> bytes:
        raise exc.internal("frozen field cipher is decrypt-only")

    def decrypt_sync(self, blob: bytes, *, aad: bytes = b"") -> bytes:
        envelope = unpack_envelope(blob)
        ensure_algorithm(envelope, self.aead.algorithm)
        dek = self.deks.get(envelope.wrapped_dek)

        if dek is None:
            raise _not_warm("decrypt")

        return self.aead.open(
            key=dek,
            nonce=envelope.nonce,
            ciphertext=envelope.ciphertext,
            aad=aad,
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

    plaintext: bytes = attrs.field(repr=False)
    """Raw data-encryption key — ``repr`` suppressed so no log/trace/debugger dump
    of the keyring or its caches can print it (mirrors ``DataKey.plaintext``)."""

    wrapped: bytes = attrs.field(repr=False)
    key_id: str
    key_version: str | None
    uses: int = 0
    expires_at: float | None = None
    """Monotonic deadline after which this cached key is stale and must be regenerated
    (``None`` = no TTL). Bounds how long a rotated/revoked KEK's data key stays live."""


# ....................... #


@final
@attrs.define(slots=True)
class _CachedDek:
    """An unwrapped data key on the decrypt path, with an optional staleness deadline."""

    plaintext: bytes = attrs.field(repr=False)
    """Raw data-encryption key — ``repr`` suppressed (mirrors ``DataKey.plaintext``)."""

    expires_at: float | None = None
    """Monotonic deadline after which the entry is treated as a miss and re-unwrapped."""


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

    dek_ttl_seconds: float | None = None
    """Optional lifetime for a cached (plaintext) data key, on both the encrypt and
    decrypt paths. ``None`` (default) keeps a data key until LRU eviction or process
    restart — so a KEK rotation or revocation only takes effect after a restart. Set a
    TTL to bound that window: once elapsed, the entry is treated as a miss and the key
    is regenerated / re-unwrapped through the KMS (which re-checks the KEK)."""

    _enc_cache: OrderedDict[str, _ActiveDataKey] = attrs.field(
        factory=OrderedDict, init=False, repr=False
    )
    """key_id → active data key (encrypt path, LRU). ``repr`` suppressed — holds
    plaintext data keys."""

    _dec_cache: OrderedDict[bytes, _CachedDek] = attrs.field(
        factory=OrderedDict, init=False, repr=False
    )
    """wrapped data key → cached plaintext data key (decrypt path, LRU). ``repr``
    suppressed — the entries hold plaintext data keys."""

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
        # Deterministic key_id → stripe mapping via a stable hash (CRC32, like the
        # L1 cache), NOT Python's ``hash()`` which is PYTHONHASHSEED-randomized —
        # so the same key_id picks the same stripe across processes/runs, keeping
        # both per-key mutual exclusion and cross-process simulation replay stable.
        return self._locks[zlib.crc32(key_id.encode("utf-8")) % len(self._locks)]

    # ....................... #

    def _expiry(self) -> float | None:
        """A fresh monotonic deadline for a newly cached key, or ``None`` when no TTL."""

        if self.dek_ttl_seconds is None:
            return None

        return current_time_source().monotonic() + self.dek_ttl_seconds

    # ....................... #

    def _is_expired(self, expires_at: float | None) -> bool:
        """Whether a cached key's deadline has passed (always ``False`` with no TTL)."""

        return expires_at is not None and (
            current_time_source().monotonic() >= expires_at
        )

    # ....................... #

    def _cached_dek(self, wrapped: bytes) -> bytes | None:
        """Return a fresh unwrapped data key from the decrypt cache, or ``None``.

        Drops the entry on a TTL miss so the next resolution re-unwraps through the KMS
        (which re-validates the KEK), bounding a rotated/revoked key's live window.
        """

        entry = _lru_get(self._dec_cache, wrapped)

        if entry is None:
            return None

        if self._is_expired(entry.expires_at):
            del self._dec_cache[wrapped]
            return None

        return entry.plaintext

    # ....................... #

    def _store_dek(self, wrapped: bytes, plaintext: bytes) -> None:
        _lru_put(
            self._dec_cache,
            wrapped,
            _CachedDek(plaintext, self._expiry()),
            cap=self.decrypt_cache_max,
        )

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

    async def decrypt(
        self,
        blob: bytes,
        *,
        aad: bytes = b"",
        tenant: TenantIdentity | None = None,
    ) -> bytes:
        """Decrypt a packed envelope; the key is resolved from the envelope itself.

        When *tenant* is given, the envelope's key id is authorized against the
        tenant's own key before any KMS unwrap (confused-deputy guard).
        """

        envelope = unpack_envelope(blob)
        ensure_algorithm(envelope, self.aead.algorithm)
        dek = await self._unwrap(envelope, tenant=tenant)

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

    async def ensure_unwrapped(
        self,
        envelopes: Iterable[EncryptedEnvelope],
        *,
        tenant: TenantIdentity | None = None,
    ) -> None:
        """Unwrap and cache the data keys for *envelopes* so sync decrypts hit.

        The read pre-pass: with per-tenant data-key reuse a result set carries
        only a handful of distinct wrapped keys, so this is a few KMS calls
        regardless of row count. A same-process read-after-write is already a
        cache hit and unwraps nothing. When *tenant* is given, each envelope's key
        id is authorized against the tenant's key before it is unwrapped.
        """

        for envelope in envelopes:
            if self._cached_dek(envelope.wrapped_dek) is None:
                await self._unwrap(envelope, tenant=tenant)

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

        if (
            active is None
            or active.uses >= self.max_dek_messages
            or self._is_expired(active.expires_at)
        ):
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
        dek = self._cached_dek(envelope.wrapped_dek)

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

    def freeze_decryptor(
        self, envelopes: Iterable[EncryptedEnvelope]
    ) -> _FrozenFieldCipher:
        """Resolve *envelopes*' data keys into a thread-local snapshot for offloaded decrypt.

        Called on the event loop after :meth:`ensure_unwrapped`: reads each warmed data key
        out of the shared decrypt cache into a plain dict, so the returned cipher's
        :meth:`~_FrozenFieldCipher.decrypt_sync` does pure AEAD with no cache mutation — safe
        from a worker thread. A key still missing (never warmed) is left out; decrypting its
        blob then raises ``cipher_not_warm``, exactly as the live path would."""

        deks: dict[bytes, bytes] = {}

        for envelope in envelopes:
            wrapped = envelope.wrapped_dek

            if wrapped not in deks:
                dek = self._cached_dek(wrapped)

                if dek is not None:
                    deks[wrapped] = dek

        return _FrozenFieldCipher(aead=self.aead, deks=deks)

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

            if (
                cached is not None
                and cached.uses < self.max_dek_messages
                and not self._is_expired(cached.expires_at)
            ):
                if consume:
                    self._n_enc_hits += 1
                    cached.uses += 1
                return cached

            data_key = await self.kms.generate_data_key(key_ref)
            self._n_generated += 1  # count only an actual KMS round-trip, not a failed one
            active = _ActiveDataKey(
                plaintext=data_key.plaintext,
                wrapped=data_key.wrapped,
                key_id=data_key.key_id,
                key_version=data_key.key_version,
                uses=1 if consume else 0,
                expires_at=self._expiry(),
            )
            _lru_put(
                self._enc_cache, key_ref.key_id, active, cap=self.enc_cache_max
            )
            # Seed the decrypt cache so a read-after-write is a hit.
            self._store_dek(data_key.wrapped, data_key.plaintext)
            return active

    # ....................... #

    async def _unwrap(
        self, envelope: EncryptedEnvelope, *, tenant: TenantIdentity | None = None
    ) -> bytes:
        cached = self._cached_dek(envelope.wrapped_dek)

        if cached is not None:
            self._n_dec_hits += 1
            return cached

        async with self._lock_for(envelope.key_id):
            cached = self._cached_dek(envelope.wrapped_dek)

            if cached is not None:
                self._n_dec_hits += 1
                return cached

            await self._authorize_key_id(envelope, tenant)
            dek = await self.kms.unwrap_data_key(
                wrapped=envelope.wrapped_dek,
                key_ref=KeyRef(key_id=envelope.key_id, version=envelope.key_version),
            )
            self._n_unwrapped += 1  # count only an actual KMS round-trip, not a failed one
            self._store_dek(envelope.wrapped_dek, dek)
            return dek

    # ....................... #

    async def _authorize_key_id(
        self, envelope: EncryptedEnvelope, tenant: TenantIdentity | None
    ) -> None:
        """Fail closed before a KMS unwrap if the envelope names a key the tenant lacks.

        The envelope's ``key_id`` is attacker-influenced (it comes from stored/received
        bytes), and :meth:`_unwrap` would otherwise ask the backend to unwrap under it —
        letting a caller in one tenant drive a KMS unwrap under another tenant's key
        (a cross-tenant confused-deputy / amplification). When a *tenant* is supplied we
        first resolve its own key and reject a mismatch, so no KMS call is made on an
        unauthorized key id. ``None`` (single-key deployments) skips the check.
        """

        if tenant is None:
            return

        # Prefer the cached tenant→key_id (seeded by encrypt / a prior authorize) so a
        # batch pre-pass does not re-hit the directory per envelope; resolve on a miss.
        expected = _lru_get(self._tenant_key, _tenant_cache_key(tenant))

        if expected is None:
            expected = (await self._resolve_key_ref(tenant)).key_id

        if envelope.key_id != expected:
            raise exc.validation(
                "Envelope key id does not belong to the active tenant; refusing to "
                "unwrap under a key the caller does not own.",
                code="core.crypto.key_id_unauthorized",
            )

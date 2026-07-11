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
from typing import Any, AsyncIterator, Iterable, final

import attrs

from forze.application.contracts.crypto import (
    CryptoKeyringStats,
    KeyDirectoryPort,
    KeyDirectoryWithPrevious,
    KeyManagementPort,
    KeyRef,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.crypto import (
    DEFAULT_CHUNK_SIZE,
    MAX_CHUNK_SIZE,
    Aead,
    ChunkedHeader,
    ChunkedStreamReader,
    ChunkFrame,
    EncryptedEnvelope,
    ensure_algorithm,
    open_chunk,
    pack_chunked_header,
    pack_envelope,
    seal_chunk,
    unpack_chunked_header,
    unpack_envelope,
)
from forze.base.exceptions import exc
from forze.base.primitives import current_time_source

# ----------------------- #

_NONE_TENANT = "\x00none"
"""Cache key for the no-tenant (single-key) case."""

_LOCK_STRIPES = 64
"""Fixed number of fill-lock stripes (bounds the lock set regardless of key count)."""

# ....................... #


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

    # ....................... #

    async def ensure_unwrapped(
        self,
        envelopes: Iterable[EncryptedEnvelope],
        *,
        tenant: TenantIdentity | None = None,
    ) -> None:
        return None

    # ....................... #

    def encrypt_sync(
        self,
        plaintext: bytes,
        *,
        tenant: TenantIdentity | None,
        aad: bytes = b"",
    ) -> bytes:
        raise exc.internal("frozen field cipher is decrypt-only")

    # ....................... #

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


# ....................... #


def _lru_get(cache: OrderedDict[Any, Any], key: Any) -> Any:
    value = cache.get(key)

    if value is not None:
        cache.move_to_end(key)

    return value


# ....................... #


def _lru_put(cache: OrderedDict[Any, Any], key: Any, value: Any, *, cap: int) -> None:
    cache[key] = value
    cache.move_to_end(key)

    while len(cache) > cap:
        cache.popitem(last=False)


# ....................... #


async def _rechunk(
    source: AsyncIterator[bytes],
    chunk_size: int,
) -> AsyncIterator[tuple[bytes, bool]]:
    """Re-chunk *source*'s arbitrary byte runs into ``(chunk, is_final)`` of *chunk_size*.

    Holds one chunk of lookahead so exactly one chunk is flagged final — even when the
    input is empty (one empty final chunk) or an exact multiple of *chunk_size* (no
    spurious trailing empty chunk).
    """

    buffer = bytearray()
    pending: bytes | None = None

    async for piece in source:
        buffer.extend(piece)

        while len(buffer) >= chunk_size:
            full = bytes(buffer[:chunk_size])
            del buffer[:chunk_size]

            if pending is not None:
                yield pending, False

            pending = full

    if pending is None:
        yield bytes(buffer), True  # whole (possibly empty) input fit in one chunk
        return

    if buffer:
        yield pending, False
        yield bytes(buffer), True

    else:
        yield pending, True  # the last full chunk terminates the stream


# ....................... #


@final
@attrs.define(slots=True)
class _ActiveDataKey:
    """A cached data key used for encryption, with a reuse counter."""

    plaintext: bytes = attrs.field(repr=False)
    """Raw data-encryption key — ``repr`` suppressed so no log/trace/debugger dump
    of the keyring or its caches can print it (mirrors ``DataKey.plaintext``)."""

    wrapped: bytes = attrs.field(repr=False)
    """Wrapped data-encryption key — ``repr`` suppressed (mirrors ``DataKey.wrapped``)."""

    key_id: str
    """ID of the data key."""

    key_version: str | None
    """Version of the data key."""

    uses: int = 0
    """Number of times the data key has been used."""

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
@attrs.define(slots=True, frozen=True)
class _ChunkedStreamOpener:
    """Random-access opener over one chunked object with its data key resolved.

    Built by :meth:`Keyring.open_chunked_stream`; opens an individual frame at its index
    against the pre-unwrapped data key, so a ranged reader can decrypt only the chunks a
    byte range covers.
    """

    aead: Aead
    """Local authenticated cipher applied under each data key."""

    _dek: bytes = attrs.field(repr=False, alias="dek")
    """Pre-unwrapped data key."""

    _aad: bytes = attrs.field(repr=False, alias="aad")
    """Additional authenticated data."""

    chunk_size: int
    """Size of each chunk."""

    header_len: int
    """Length of the header."""

    # ....................... #

    def open_frame(self, index: int, frame: ChunkFrame) -> bytes:
        return open_chunk(
            self.aead,
            key=self._dek,
            base_aad=self._aad,
            index=index,
            frame=frame,
        )


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

    # ....................... #

    _enc_cache: OrderedDict[str, _ActiveDataKey] = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    """key_id → active data key (encrypt path, LRU). ``repr`` suppressed — holds
    plaintext data keys."""

    _dec_cache: OrderedDict[bytes, _CachedDek] = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    """wrapped data key → cached plaintext data key (decrypt path, LRU). ``repr``
    suppressed — the entries hold plaintext data keys."""

    _tenant_key: OrderedDict[str, str] = attrs.field(factory=OrderedDict, init=False)
    """tenant cache key → resolved key_id (lets the sync path skip the directory, LRU)."""

    _tenant_prev_key: OrderedDict[str, str] = attrs.field(
        factory=OrderedDict,
        init=False,
    )
    """tenant cache key → the tenant's *previous* key_id during a KEK-migration overlap
    (LRU). Only a *present* previous key is cached — an absence is never memoized, so a
    directory that opens an overlap later is honored at once (see :meth:`_previous_key_id`)."""

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

    async def encrypt_stream(
        self,
        plaintext: AsyncIterator[bytes],
        *,
        tenant: TenantIdentity | None,
        aad: bytes = b"",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        """Encrypt *plaintext* as a chunked-AEAD stream (bounded to one chunk in memory).

        One data key is generated for the whole stream and wrapped in the header; each
        re-chunked piece is sealed under it with an AAD binding *aad* plus the chunk's
        position and terminator flag.
        """

        if chunk_size < 1 or chunk_size > MAX_CHUNK_SIZE:
            raise exc.validation(
                f"Chunk size must be in [1, {MAX_CHUNK_SIZE}], got {chunk_size}",
                code="core.crypto.chunked_bad_chunk_size",
            )

        key_ref = await self._resolve_key_ref(tenant)
        data_key = await self.kms.generate_data_key(key_ref)
        self._n_generated += 1

        yield pack_chunked_header(
            ChunkedHeader(
                alg=self.aead.algorithm,
                key_id=data_key.key_id,
                key_version=data_key.key_version,
                wrapped_dek=data_key.wrapped,
                chunk_size=chunk_size,
            )
        )

        index = 0

        async for chunk, is_final in _rechunk(plaintext, chunk_size):
            yield seal_chunk(
                self.aead,
                key=data_key.plaintext,
                base_aad=aad,
                index=index,
                is_final=is_final,
                plaintext=chunk,
            )
            index += 1

    # ....................... #

    async def decrypt_stream(
        self,
        ciphertext: AsyncIterator[bytes],
        *,
        aad: bytes = b"",
        tenant: TenantIdentity | None = None,
    ) -> AsyncIterator[bytes]:
        """Decrypt a chunked-AEAD stream, yielding plaintext one chunk at a time.

        The header's key id is authorized against *tenant* (when given) before the data
        key is unwrapped. A stream that ends without a terminating chunk (truncation) or
        carries trailing bytes after it is rejected.
        """

        reader = ChunkedStreamReader()
        header: ChunkedHeader | None = None
        dek: bytes | None = None
        index = 0
        seen_final = False

        async for piece in ciphertext:
            reader.feed(piece)

            if header is None:
                header = reader.take_header()

                if header is None:
                    continue

                if header.alg != self.aead.algorithm:
                    raise exc.validation(
                        f"Stream was sealed with {header.alg!r} but the wired cipher is "
                        f"{self.aead.algorithm!r}; the matching AEAD is required to decrypt it",
                        code="core.crypto.algorithm_mismatch",
                    )

                await self._authorize_key_id_value(header.key_id, tenant)
                dek = await self.kms.unwrap_data_key(
                    wrapped=header.wrapped_dek,
                    key_ref=KeyRef(key_id=header.key_id, version=header.key_version),
                )
                self._n_unwrapped += 1

            for frame in reader.take_frames():
                if seen_final:
                    raise exc.validation(
                        "Chunked stream carries a frame after its final chunk",
                        code="core.crypto.chunked_trailing_data",
                    )

                yield open_chunk(
                    self.aead,
                    key=dek,  # type: ignore[arg-type]  # set once the header is parsed
                    base_aad=aad,
                    index=index,
                    frame=frame,
                )
                index += 1
                seen_final = frame.is_final

        if header is None:
            raise exc.validation(
                "Chunked stream ended before its header was complete",
                code="core.crypto.chunked_truncated",
            )

        if not seen_final:
            raise exc.validation(
                "Chunked stream ended without a final chunk (truncated)",
                code="core.crypto.chunked_truncated",
            )

        if reader.has_buffered_bytes():
            raise exc.validation(
                "Chunked stream carries trailing bytes after its final chunk",
                code="core.crypto.chunked_trailing_data",
            )

    # ....................... #

    async def open_chunked_stream(
        self,
        header_bytes: bytes,
        *,
        aad: bytes = b"",
        tenant: TenantIdentity | None = None,
    ) -> _ChunkedStreamOpener:
        """Parse a chunked header and return a random-access opener (data key unwrapped once).

        The header's key id is authorized against *tenant* (confused-deputy guard) before
        the KMS unwrap, mirroring :meth:`decrypt_stream`.
        """

        header, header_len = unpack_chunked_header(header_bytes)

        if header.alg != self.aead.algorithm:
            raise exc.validation(
                f"Stream was sealed with {header.alg!r} but the wired cipher is "
                f"{self.aead.algorithm!r}; the matching AEAD is required to decrypt it",
                code="core.crypto.algorithm_mismatch",
            )

        await self._authorize_key_id_value(header.key_id, tenant)

        dek = await self.kms.unwrap_data_key(
            wrapped=header.wrapped_dek,
            key_ref=KeyRef(key_id=header.key_id, version=header.key_version),
        )

        self._n_unwrapped += 1

        return _ChunkedStreamOpener(
            aead=self.aead,
            dek=dek,
            aad=aad,
            chunk_size=header.chunk_size,
            header_len=header_len,
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
        self,
        envelopes: Iterable[EncryptedEnvelope],
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
        self,
        key_ref: KeyRef,
        *,
        consume: bool = True,
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

            # count only an actual KMS round-trip, not a failed one
            self._n_generated += 1

            active = _ActiveDataKey(
                plaintext=data_key.plaintext,
                wrapped=data_key.wrapped,
                key_id=data_key.key_id,
                key_version=data_key.key_version,
                uses=1 if consume else 0,
                expires_at=self._expiry(),
            )
            _lru_put(self._enc_cache, key_ref.key_id, active, cap=self.enc_cache_max)

            # Seed the decrypt cache so a read-after-write is a hit.
            self._store_dek(data_key.wrapped, data_key.plaintext)

            return active

    # ....................... #

    async def _unwrap(
        self,
        envelope: EncryptedEnvelope,
        *,
        tenant: TenantIdentity | None = None,
    ) -> bytes:
        # Authorize the key id against the tenant *before* any cache lookup, so a warm
        # cached ``wrapped_dek`` can never return a data key for an envelope the tenant is
        # not entitled to (a cross-tenant confused-deputy on a cache hit). The check is
        # cheap and cached (``_tenant_key`` LRU); a ``None`` tenant is a no-op.
        await self._authorize_key_id(envelope, tenant)

        cached = self._cached_dek(envelope.wrapped_dek)

        if cached is not None:
            self._n_dec_hits += 1

            return cached

        async with self._lock_for(envelope.key_id):
            cached = self._cached_dek(envelope.wrapped_dek)

            if cached is not None:
                self._n_dec_hits += 1

                return cached

            dek = await self.kms.unwrap_data_key(
                wrapped=envelope.wrapped_dek,
                key_ref=KeyRef(key_id=envelope.key_id, version=envelope.key_version),
            )

            # count only an actual KMS round-trip, not a failed one
            self._n_unwrapped += 1
            self._store_dek(envelope.wrapped_dek, dek)

            return dek

    # ....................... #

    async def _authorize_key_id(
        self,
        envelope: EncryptedEnvelope,
        tenant: TenantIdentity | None,
    ) -> None:
        """Fail closed before a KMS unwrap if the envelope names a key the tenant lacks.

        The envelope's ``key_id`` is attacker-influenced (it comes from stored/received
        bytes), and :meth:`_unwrap` would otherwise ask the backend to unwrap under it —
        letting a caller in one tenant drive a KMS unwrap under another tenant's key
        (a cross-tenant confused-deputy / amplification). When a *tenant* is supplied we
        first resolve its own key and reject a mismatch, so no KMS call is made on an
        unauthorized key id. ``None`` (single-key deployments) skips the check.
        """

        await self._authorize_key_id_value(envelope.key_id, tenant)

    # ....................... #

    async def _previous_key_id(self, tenant: TenantIdentity | None) -> str | None:
        """*tenant*'s previous key id during a KEK-migration overlap, else ``None``.

        A directory that does not implement :class:`KeyDirectoryWithPrevious` has no
        overlap, so this is a no-op for the ordinary case.

        Only a *present* previous key is cached. An absent one is deliberately **not**
        memoized: a store-backed directory can open an overlap at any moment, and a
        cached absence would keep rejecting the outgoing key's envelopes until the entry
        aged out — stranding the very migration the overlap exists to enable. The cost is
        confined to the path that does not match the tenant's current key, which in
        steady state is the rejection path (about to raise); during a migration the
        previous key resolves once and is then served from the cache, which is what keeps
        a lookup-based directory (one List call per resolve) cheap across a sweep.
        """

        directory = self.directory

        if not isinstance(directory, KeyDirectoryWithPrevious):
            return None

        cache_key = _tenant_cache_key(tenant)
        cached = _lru_get(self._tenant_prev_key, cache_key)

        if cached is not None:
            return cached

        previous = await directory.resolve_previous(tenant)

        if previous is None:
            return None

        _lru_put(
            self._tenant_prev_key,
            cache_key,
            previous.key_id,
            cap=self.enc_cache_max,
        )

        return previous.key_id

    # ....................... #

    async def _authorize_key_id_value(
        self,
        key_id: str,
        tenant: TenantIdentity | None,
    ) -> None:
        """The key-id/tenant confused-deputy check over a bare key id (envelope or stream)."""

        if tenant is None:
            return

        # Prefer the cached tenant→key_id (seeded by encrypt / a prior authorize) so a
        # batch pre-pass does not re-hit the directory per envelope; resolve on a miss.
        expected = _lru_get(self._tenant_key, _tenant_cache_key(tenant))

        if expected is None:
            expected = (await self._resolve_key_ref(tenant)).key_id

        if key_id == expected:
            return

        # A KEK migration opens a read overlap: the tenant's *previous* key stays
        # readable so a sweep can re-encrypt onto the current one. Writes never use it.
        if key_id == await self._previous_key_id(tenant):
            return

        raise exc.validation(
            "Envelope key id does not belong to the active tenant; refusing to "
            "unwrap under a key the caller does not own.",
            code="core.crypto.key_id_unauthorized",
        )

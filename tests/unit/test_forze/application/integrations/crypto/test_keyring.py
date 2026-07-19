"""Tests for the :class:`Keyring` — caching, rotation-safe decrypt, per-tenant keys."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    ChaCha20Poly1305Aead,
    KeyRef,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.crypto.keyring import _LOCK_STRIPES
from forze.base.crypto import unpack_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import bind_time_source
from forze_mock import MockKeyManagement

# ----------------------- #


@attrs.define(slots=True)
class _CountingKms:
    """Wraps a key manager to count backend round-trips for cache assertions."""

    inner: MockKeyManagement
    generated: int = 0
    unwrapped: int = 0

    async def generate_data_key(self, key_ref: KeyRef):  # type: ignore[no-untyped-def]
        self.generated += 1
        return await self.inner.generate_data_key(key_ref)

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        self.unwrapped += 1
        return await self.inner.unwrap_data_key(wrapped=wrapped, key_ref=key_ref)


# ....................... #


def _keyring(directory=None, **kw) -> Keyring:  # type: ignore[no-untyped-def]
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=directory or StaticKeyDirectory(KeyRef(key_id="cmk")),
        **kw,
    )


# ....................... #


async def test_round_trip() -> None:
    ring = _keyring()

    blob = await ring.encrypt(b"secret", tenant=None)

    assert await ring.decrypt(blob) == b"secret"


# ....................... #


async def test_aad_must_match() -> None:
    ring = _keyring()
    blob = await ring.encrypt(b"secret", tenant=None, aad=b"ctx-a")

    with pytest.raises(CoreException) as excinfo:
        await ring.decrypt(blob, aad=b"ctx-b")

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


async def test_active_data_key_is_reused_across_encryptions() -> None:
    kms = _CountingKms(MockKeyManagement())
    ring = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    for _ in range(5):
        await ring.encrypt(b"x", tenant=None)

    assert kms.generated == 1  # one data key shared across the batch


# ....................... #


async def test_data_key_regenerated_after_use_bound() -> None:
    kms = _CountingKms(MockKeyManagement())
    ring = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        max_dek_messages=2,
    )

    for _ in range(5):
        await ring.encrypt(b"x", tenant=None)

    assert kms.generated == 3  # 2 + 2 + 1 across the use bound


# ....................... #


async def test_decrypt_caches_unwrapped_key() -> None:
    kms = _CountingKms(MockKeyManagement())
    ring = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    blob = await ring.encrypt(b"secret", tenant=None)

    # Fresh keyring so the encrypt-seeded decrypt cache does not mask the unwrap.
    reader = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    await reader.decrypt(blob)
    await reader.decrypt(blob)

    assert kms.unwrapped == 1  # second decrypt hit the cache


# ....................... #


async def test_decrypt_is_rotation_safe() -> None:
    """A blob written under one key version decrypts after the directory rotates."""

    shared_kms = MockKeyManagement()
    writer = Keyring(
        kms=shared_kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk", version="v1")),
    )
    blob = await writer.encrypt(b"old data", tenant=None)

    # Directory now resolves a new active version; the old blob still decrypts
    # because the envelope self-describes its key version.
    rotated = Keyring(
        kms=shared_kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk", version="v2")),
    )

    assert await rotated.decrypt(blob) == b"old data"


# ....................... #


async def test_per_tenant_keys_differ() -> None:
    ring = _keyring(
        directory=TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/cmk",
            default_key_id="default",
        )
    )
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    env_a = unpack_envelope(await ring.encrypt(b"a", tenant=tenant_a))
    env_b = unpack_envelope(await ring.encrypt(b"b", tenant=tenant_b))

    assert env_a.key_id != env_b.key_id
    assert str(tenant_a.tenant_id) in env_a.key_id


# ....................... #


async def test_warm_pre_resolves_active_key() -> None:
    kms = _CountingKms(MockKeyManagement())
    ring = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    await ring.warm(None)
    assert kms.generated == 1

    await ring.encrypt(b"x", tenant=None)
    assert kms.generated == 1  # encrypt reused the warmed key


# ....................... #


async def test_warm_does_not_consume_dek_budget() -> None:
    """Warming must not spend an encryption from ``max_dek_messages``.

    With a budget of one, the warmed key must still have room for the encrypt the
    warm was preparing — otherwise the very next ``encrypt_sync`` goes cold.
    """

    ring = _keyring(max_dek_messages=1)

    await ring.warm(None)

    # The warmed key still has its single use available.
    blob = ring.encrypt_sync(b"secret", tenant=None, aad=b"ctx")
    assert ring.decrypt_sync(blob, aad=b"ctx") == b"secret"


# ....................... #


async def test_decrypt_rejects_algorithm_mismatch() -> None:
    """A blob sealed under one AEAD fails clearly against a different wired cipher."""

    shared_kms = MockKeyManagement()
    writer = Keyring(
        kms=shared_kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    blob = await writer.encrypt(b"secret", tenant=None)

    reader = Keyring(
        kms=shared_kms,
        aead=ChaCha20Poly1305Aead(),  # deployment swapped the cipher
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    with pytest.raises(CoreException) as excinfo:
        await reader.decrypt(blob)

    assert excinfo.value.kind is ExceptionKind.VALIDATION
    assert excinfo.value.code == "core.crypto.algorithm_mismatch"


# ....................... #
# Synchronous fast path (the codec bridge)


async def test_sync_round_trip_after_warm() -> None:
    ring = _keyring()
    await ring.warm(None)

    blob = ring.encrypt_sync(b"secret", tenant=None, aad=b"ctx")

    assert ring.decrypt_sync(blob, aad=b"ctx") == b"secret"


# ....................... #


def test_encrypt_sync_without_warm_raises() -> None:
    # An async-only KMS (any real backend): no sync fill, so the pre-pass stays mandatory.
    ring = Keyring(
        kms=_CountingKms(MockKeyManagement()),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    with pytest.raises(CoreException) as excinfo:
        ring.encrypt_sync(b"secret", tenant=None)

    assert excinfo.value.kind is ExceptionKind.INTERNAL
    assert excinfo.value.code == "core.crypto.cipher_not_warm"


# ....................... #


async def test_decrypt_sync_cold_then_pre_pass() -> None:
    """A fresh reader (cross-process) must run the read pre-pass before sync decode."""

    writer = _keyring()
    await writer.warm(None)
    blob = writer.encrypt_sync(b"secret", tenant=None)
    envelope = unpack_envelope(blob)

    reader = _keyring()  # cold cache, as in another process

    with pytest.raises(CoreException) as excinfo:
        reader.decrypt_sync(blob)
    assert excinfo.value.code == "core.crypto.cipher_not_warm"

    await reader.ensure_unwrapped([envelope])
    assert reader.decrypt_sync(blob) == b"secret"


# ....................... #


async def test_same_process_decrypt_sync_hits_seeded_cache() -> None:
    """Encrypt seeds the decrypt cache, so a read-after-write needs no pre-pass."""

    ring = _keyring()
    await ring.warm(None)
    blob = ring.encrypt_sync(b"secret", tenant=None)

    assert ring.decrypt_sync(blob) == b"secret"  # no ensure_unwrapped needed


# ....................... #


async def test_ensure_unwrapped_is_deduplicated() -> None:
    kms = _CountingKms(MockKeyManagement())
    writer = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    await writer.warm(None)
    envelopes = [unpack_envelope(writer.encrypt_sync(b"x", tenant=None)) for _ in range(4)]

    reader = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    before = kms.unwrapped
    await reader.ensure_unwrapped(envelopes)

    # All four share one reused data key → a single unwrap.
    assert kms.unwrapped - before == 1


# ....................... #
# Bounded LRU caches (hardening)


def _per_tenant_keyring(**kw) -> Keyring:  # type: ignore[no-untyped-def]
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/cmk", default_key_id="default"
        ),
        **kw,
    )


async def test_enc_cache_evicts_least_recently_used() -> None:
    # An async-only KMS so an evicted key cannot be refilled inline — the eviction
    # itself (not the sync-fill convenience) is what this test observes.
    ring = Keyring(
        kms=_CountingKms(MockKeyManagement()),
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/cmk", default_key_id="default"
        ),
        enc_cache_max=1,
    )
    a = TenantIdentity(tenant_id=uuid4())
    b = TenantIdentity(tenant_id=uuid4())

    await ring.warm(a)
    await ring.warm(b)  # evicts a's active key (cap 1)

    # b is warm; a was evicted → its sync encrypt fails closed.
    ring.encrypt_sync(b"x", tenant=b)
    with pytest.raises(CoreException) as excinfo:
        ring.encrypt_sync(b"x", tenant=a)
    assert excinfo.value.code == "core.crypto.cipher_not_warm"


async def test_decrypt_cache_is_lru_not_clear_all() -> None:
    """At capacity the decrypt cache evicts one entry, not the whole cache."""

    ring = _per_tenant_keyring(decrypt_cache_max=2, enc_cache_max=8)
    tenants = [TenantIdentity(tenant_id=uuid4()) for _ in range(3)]

    blobs = []
    for t in tenants:
        await ring.warm(t)
        blobs.append(ring.encrypt_sync(b"v", tenant=t))

    # 3 distinct data keys seeded into a cap-2 cache → only the oldest is evicted.
    with pytest.raises(CoreException):
        ring.decrypt_sync(blobs[0])  # evicted
    assert ring.decrypt_sync(blobs[1]) == b"v"  # retained
    assert ring.decrypt_sync(blobs[2]) == b"v"  # retained


def test_fill_lock_is_stable_per_key_and_bounded() -> None:
    """The fill lock is deterministic per key_id (mutual exclusion holds) and the
    stripe set is bounded, so it can't grow with rotating/per-tenant key_ids."""

    ring = _per_tenant_keyring()

    # Same key_id → same lock object (the per-key exclusivity invariant).
    key = "tenant/abc/cmk"
    assert ring._lock_for(key) is ring._lock_for(key)  # type: ignore[attr-defined]

    # Many distinct key_ids spread across multiple stripes (cold fills still
    # parallelize in the common case) but never grow the lock set past the stripe count.
    locks = {id(ring._lock_for(f"tenant/{i}/cmk")) for i in range(500)}  # type: ignore[attr-defined]
    assert 1 < len(locks) <= _LOCK_STRIPES


# ....................... #


async def test_repr_never_leaks_plaintext_data_keys() -> None:
    """No ``repr`` of the keyring, its caches, or a frozen decryptor may print a raw DEK.

    A log line, DST trace, or debugger dump that reprs a keyring must not expose the
    plaintext data-encryption keys it caches — the ``repr=False`` discipline that
    ``DataKey.plaintext`` already follows.
    """

    ring = _keyring()
    blob = await ring.encrypt(b"secret", tenant=None)

    active = next(iter(ring._enc_cache.values()))  # type: ignore[attr-defined]
    plaintext_dek = active.plaintext
    dek_literal = repr(plaintext_dek)  # e.g. b'\\x1f...'

    assert plaintext_dek  # the fixture actually cached a key
    # The realistic dump vectors: repr of the keyring, of an active-key entry, of the
    # encrypt cache (an OrderedDict of _ActiveDataKey, whose own repr is now guarded),
    # and of a frozen decryptor. The bare decrypt cache is a plain OrderedDict[bytes,
    # bytes] that no code logs directly; repr=False keeps it out of the keyring repr.
    assert dek_literal not in repr(ring)
    assert dek_literal not in repr(active)
    assert dek_literal not in repr(ring._enc_cache)  # type: ignore[attr-defined]

    frozen = ring.freeze_decryptor([unpack_envelope(blob)])
    assert dek_literal not in repr(frozen)


# ....................... #


@attrs.define(slots=True)
class _ManualClock:
    """A time source whose monotonic reading is advanced by the test."""

    t: float = 0.0

    def now(self) -> datetime:
        return datetime(2020, 1, 1, tzinfo=UTC)

    def uuid(self):  # type: ignore[no-untyped-def]
        return uuid4()

    def monotonic(self) -> float:
        return self.t


async def test_dek_ttl_expires_encrypt_and_decrypt_cache() -> None:
    """With a TTL, an elapsed data key is regenerated / re-unwrapped through the KMS,
    so a rotated or revoked KEK stops being served from cache within the window."""

    clock = _ManualClock()
    kms = _CountingKms(inner=MockKeyManagement())

    with bind_time_source(clock):
        ring = Keyring(
            kms=kms,
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
            dek_ttl_seconds=10.0,
        )

        blob = await ring.encrypt(b"secret", tenant=None)
        assert kms.generated == 1
        assert await ring.decrypt(blob) == b"secret"
        assert kms.unwrapped == 0  # served from the encrypt-seeded decrypt cache

        # Within the TTL: still cached, no new KMS round-trips.
        clock.t = 5.0
        await ring.encrypt(b"again", tenant=None)
        assert kms.generated == 1

        # Past the TTL: both caches miss and re-resolve through the KMS.
        clock.t = 11.0
        await ring.encrypt(b"third", tenant=None)
        assert kms.generated == 2

        assert await ring.decrypt(blob) == b"secret"
        assert kms.unwrapped == 1  # the original data key was re-unwrapped after expiry


async def test_dek_ttl_none_keeps_key_indefinitely() -> None:
    """The default (no TTL) keeps a data key cached regardless of elapsed time."""

    clock = _ManualClock()
    kms = _CountingKms(inner=MockKeyManagement())

    with bind_time_source(clock):
        ring = Keyring(
            kms=kms,
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        )

        await ring.encrypt(b"secret", tenant=None)
        clock.t = 1_000_000.0
        await ring.encrypt(b"again", tenant=None)

        assert kms.generated == 1  # never regenerated


# ....................... #
# confused-deputy guard: an envelope's key id is authorized against the tenant


def _per_tenant_ring(kms) -> Keyring:  # type: ignore[no-untyped-def]
    return Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/cmk",
            default_key_id="default",
        ),
    )


async def test_decrypt_rejects_foreign_tenant_key_id_without_kms_call() -> None:
    """A caller cannot make the backend unwrap a key id its tenant does not own —
    the mismatch fails closed before any KMS unwrap (cross-tenant confused-deputy)."""

    kms = _CountingKms(MockKeyManagement())
    writer = _per_tenant_ring(kms)
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    blob = await writer.encrypt(b"a-secret", tenant=tenant_a)

    # A fresh reader (cold cache) so the unwrap path — not a cache hit — is exercised.
    reader = _per_tenant_ring(kms)
    unwrapped_before = kms.unwrapped

    with pytest.raises(CoreException) as excinfo:
        await reader.decrypt(blob, tenant=tenant_b)

    assert excinfo.value.kind is ExceptionKind.VALIDATION
    assert excinfo.value.code == "core.crypto.key_id_unauthorized"
    assert kms.unwrapped == unwrapped_before  # no KMS unwrap on the unauthorized key id

    # The rightful tenant still decrypts (the guard only blocks a mismatch).
    assert await reader.decrypt(blob, tenant=tenant_a) == b"a-secret"


async def test_authorization_runs_on_a_warm_cache_hit() -> None:
    """A foreign tenant is rejected even when the wrapped_dek is already cached — the
    key-id guard runs before the DEK cache lookup, so a warm entry can't bypass it."""

    kms = _CountingKms(MockKeyManagement())
    ring = _per_tenant_ring(kms)
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    # Tenant A legitimately decrypts, warming the decrypt cache for this wrapped_dek.
    blob = await ring.encrypt(b"a-secret", tenant=tenant_a)
    assert await ring.decrypt(blob, tenant=tenant_a) == b"a-secret"

    # The same blob (tenant A's key id) presented under tenant B is still refused,
    # despite the warm cache entry.
    with pytest.raises(CoreException) as excinfo:
        await ring.decrypt(blob, tenant=tenant_b)
    assert excinfo.value.code == "core.crypto.key_id_unauthorized"


async def test_decrypt_without_tenant_skips_authorization() -> None:
    """The single-key path (tenant=None) is unchanged — no key-id authorization."""

    ring = _keyring()
    blob = await ring.encrypt(b"secret", tenant=None)

    assert await ring.decrypt(blob) == b"secret"


async def test_ensure_unwrapped_rejects_foreign_key_id() -> None:
    """The sync-decode pre-pass authorizes each envelope's key id against the tenant."""

    kms = _CountingKms(MockKeyManagement())
    writer = _per_tenant_ring(kms)
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    blob = await writer.encrypt(b"a-secret", tenant=tenant_a)
    envelope = unpack_envelope(blob)

    reader = _per_tenant_ring(kms)

    with pytest.raises(CoreException) as excinfo:
        await reader.ensure_unwrapped([envelope], tenant=tenant_b)

    assert excinfo.value.code == "core.crypto.key_id_unauthorized"


async def test_ensure_unwrapped_rejects_foreign_key_id_on_warm_cache_hit() -> None:
    """The pre-pass guard also runs when the wrapped_dek is already cached — a warm
    entry (left by another tenant's legitimate call) must not skip the key-id check."""

    kms = _CountingKms(MockKeyManagement())
    ring = _per_tenant_ring(kms)
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    # Tenant A's encrypt seeds the decrypt cache for this wrapped_dek.
    blob = await ring.encrypt(b"a-secret", tenant=tenant_a)
    envelope = unpack_envelope(blob)

    unwrapped_before = kms.unwrapped

    with pytest.raises(CoreException) as excinfo:
        await ring.ensure_unwrapped([envelope], tenant=tenant_b)

    assert excinfo.value.code == "core.crypto.key_id_unauthorized"
    assert kms.unwrapped == unwrapped_before  # rejected with no backend call


async def test_ensure_unwrapped_warm_hit_with_own_tenant_skips_kms() -> None:
    """A warm entry under the rightful tenant still passes the guard without a KMS
    unwrap — the cache keeps saving the round-trip on the authorized path."""

    kms = _CountingKms(MockKeyManagement())
    ring = _per_tenant_ring(kms)
    tenant_a = TenantIdentity(tenant_id=uuid4())

    blob = await ring.encrypt(b"a-secret", tenant=tenant_a)
    envelope = unpack_envelope(blob)

    unwrapped_before = kms.unwrapped
    await ring.ensure_unwrapped([envelope], tenant=tenant_a)

    assert kms.unwrapped == unwrapped_before  # warm hit: no unwrap needed
    assert ring.decrypt_sync(blob) == b"a-secret"


# ....................... #
# synchronous fill — the computation-only key backend path (mock wiring)


class _AsyncOnlyDirectory:
    """A directory reachable only through the async port (e.g. one that fetches
    customer-registered key references) — the sync guard must fail closed on it."""

    async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
        _ = tenant
        return KeyRef(key_id="cmk")


def test_encrypt_sync_fills_inline_under_a_sync_key_backend() -> None:
    """A cold sync encrypt under sync KMS + sync directory generates inline instead of
    raising — the whole point of the seam: the mock adapters need no async pre-pass."""

    ring = _keyring()  # MockKeyManagement + StaticKeyDirectory: both halves synchronous

    blob = ring.encrypt_sync(b"secret", tenant=None)

    # The fill seeded the decrypt cache too, so the read-after-write decrypts sync.
    assert ring.decrypt_sync(blob) == b"secret"

    stats = ring.stats()
    assert stats.data_keys_generated == 1
    assert stats.cold_misses == 0

    # A second encrypt reuses the filled key (a plain cache hit, no new generate).
    ring.encrypt_sync(b"more", tenant=None)
    assert ring.stats().data_keys_generated == 1
    assert ring.stats().encrypt_cache_hits == 1


def test_decrypt_sync_still_requires_a_pre_pass_even_under_a_sync_kms() -> None:
    """The fill is deliberately asymmetric: a bare ``decrypt_sync`` has no tenant, so it
    never fills — the key-ownership guard would be skippable otherwise. The synchronous
    pre-pass (``ensure_unwrapped_sync``) is the sanctioned entry and runs the guard."""

    writer = _keyring()
    blob = writer.encrypt_sync(b"secret", tenant=None)
    envelope = unpack_envelope(blob)

    reader = _keyring()  # cold cache, as in another process

    with pytest.raises(CoreException) as excinfo:
        reader.decrypt_sync(blob)
    assert excinfo.value.code == "core.crypto.cipher_not_warm"

    reader.ensure_unwrapped_sync([envelope])
    assert reader.decrypt_sync(blob) == b"secret"


def test_ensure_unwrapped_sync_refuses_a_foreign_key_id() -> None:
    """The confused-deputy guard holds on the synchronous path: an envelope naming
    another tenant's key is refused before any unwrap — same code as the async twin."""

    ring = _per_tenant_keyring()
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    blob = ring.encrypt_sync(b"a-secret", tenant=tenant_a)
    envelope = unpack_envelope(blob)

    with pytest.raises(CoreException) as excinfo:
        ring.ensure_unwrapped_sync([envelope], tenant=tenant_b)

    assert excinfo.value.code == "core.crypto.key_id_unauthorized"

    # The rightful tenant passes the same guard.
    ring.ensure_unwrapped_sync([envelope], tenant=tenant_a)
    assert ring.decrypt_sync(blob) == b"a-secret"


def test_ensure_unwrapped_sync_honors_a_migration_overlap() -> None:
    """During a KEK migration the previous key stays readable — the sync guard resolves
    the overlap through the directory's synchronous surface, mirroring the async path."""

    old = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="old-cmk")),
    )
    tenant = TenantIdentity(tenant_id=uuid4())
    blob = old.encrypt_sync(b"legacy", tenant=tenant)
    envelope = unpack_envelope(blob)

    migrating = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(
            KeyRef(key_id="new-cmk"), previous_key_ref=KeyRef(key_id="old-cmk")
        ),
    )

    migrating.ensure_unwrapped_sync([envelope], tenant=tenant)
    assert migrating.decrypt_sync(blob) == b"legacy"

    # Once the overlap is dropped, the old key id is foreign again.
    closed = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="new-cmk")),
    )
    with pytest.raises(CoreException) as excinfo:
        closed.ensure_unwrapped_sync([envelope], tenant=tenant)
    assert excinfo.value.code == "core.crypto.key_id_unauthorized"


def test_ensure_unwrapped_sync_is_a_noop_for_an_async_kms() -> None:
    """With a real (I/O) KMS the sync pre-pass does nothing: no fill, no blocking call —
    the async pre-pass stays mandatory and a cold sync decrypt still fails closed."""

    writer = _keyring()
    blob = writer.encrypt_sync(b"secret", tenant=None)
    envelope = unpack_envelope(blob)

    kms = _CountingKms(MockKeyManagement())
    reader = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    reader.ensure_unwrapped_sync([envelope], tenant=None)

    assert kms.unwrapped == 0
    with pytest.raises(CoreException) as excinfo:
        reader.decrypt_sync(blob)
    assert excinfo.value.code == "core.crypto.cipher_not_warm"


def test_sync_guard_fails_closed_under_an_async_only_directory() -> None:
    """Sync KMS but async-only directory: the tenant's own key cannot be resolved
    without awaiting, so the guard demands the async pre-pass (``cipher_not_warm``)
    rather than skipping authorization or wrongly refusing."""

    ring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=_AsyncOnlyDirectory(),
    )
    tenant = TenantIdentity(tenant_id=uuid4())

    producer = _keyring()  # same key id, sync directory
    blob = producer.encrypt_sync(b"secret", tenant=None)
    envelope = unpack_envelope(blob)

    with pytest.raises(CoreException) as excinfo:
        ring.ensure_unwrapped_sync([envelope], tenant=tenant)

    assert excinfo.value.code == "core.crypto.cipher_not_warm"

    # Without a tenant there is nothing to authorize — the fill itself proceeds.
    ring.ensure_unwrapped_sync([envelope])
    assert ring.decrypt_sync(blob) == b"secret"

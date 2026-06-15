"""Tests for the :class:`Keyring` — caching, rotation-safe decrypt, per-tenant keys."""

from __future__ import annotations

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
from forze.base.crypto import unpack_envelope
from forze.base.exceptions import CoreException, ExceptionKind
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
    ring = _keyring()

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
    ring = _per_tenant_keyring(enc_cache_max=1)
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


async def test_distinct_keys_use_distinct_locks() -> None:
    """Cold fills for different key_ids do not serialize on a shared lock."""

    ring = _per_tenant_keyring()
    a = TenantIdentity(tenant_id=uuid4())
    b = TenantIdentity(tenant_id=uuid4())

    await ring.warm(a)
    await ring.warm(b)

    # Different key_ids → different lock objects.
    lock_a = ring._lock_for(f"tenant/{a.tenant_id}/cmk")  # type: ignore[attr-defined]
    lock_b = ring._lock_for(f"tenant/{b.tenant_id}/cmk")  # type: ignore[attr-defined]
    assert lock_a is not lock_b

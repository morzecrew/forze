"""Keyring observability counters (`Keyring.stats()`)."""

from __future__ import annotations

import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    DataKey,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze_mock import MockKeyManagement

# ----------------------- #


def _ring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


class _BoomKms:
    """A key backend whose every call fails (a down/unreachable KMS)."""

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        raise RuntimeError("kms down")

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        raise RuntimeError("kms down")


def _boom_ring() -> Keyring:
    return Keyring(
        kms=_BoomKms(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


# ....................... #


async def test_encrypt_path_counts_generate_then_cache_hits() -> None:
    ring = _ring()

    await ring.warm(None)  # cold → KMS generate
    blob = ring.encrypt_sync(b"hello", tenant=None)  # warmed reuse

    stats = ring.stats()
    assert stats.data_keys_generated == 1
    assert stats.encrypt_cache_hits == 1
    assert stats.cold_misses == 0

    # The encrypt seeds the decrypt cache, so a same-process read-after-write hits.
    ring.decrypt_sync(blob)
    assert ring.stats().decrypt_cache_hits == 1
    assert ring.stats().data_keys_unwrapped == 0


async def test_async_encrypt_reuses_active_key() -> None:
    ring = _ring()

    await ring.encrypt(b"a", tenant=None)  # generate
    await ring.encrypt(b"b", tenant=None)  # reuse

    stats = ring.stats()
    assert stats.data_keys_generated == 1
    assert stats.encrypt_cache_hits == 1


async def test_async_decrypt_unwraps_then_caches() -> None:
    producer = _ring()
    await producer.warm(None)
    blob = producer.encrypt_sync(b"secret", tenant=None)

    # Fresh keyring (cold decrypt cache) → one KMS unwrap, then a cache hit.
    reader = _ring()
    await reader.decrypt(blob)
    await reader.decrypt(blob)

    stats = reader.stats()
    assert stats.data_keys_unwrapped == 1
    assert stats.decrypt_cache_hits == 1


async def test_failed_generate_does_not_count() -> None:
    """A KMS error must not bump data_keys_generated (else the metric overcounts)."""

    ring = _boom_ring()

    with pytest.raises(RuntimeError):
        await ring.encrypt(b"x", tenant=None)

    assert ring.stats().data_keys_generated == 0


async def test_failed_unwrap_does_not_count() -> None:
    """A KMS error must not bump data_keys_unwrapped."""

    producer = _ring()
    await producer.warm(None)
    blob = producer.encrypt_sync(b"secret", tenant=None)

    reader = _boom_ring()  # cold decrypt cache → reaches the failing unwrap

    with pytest.raises(RuntimeError):
        await reader.decrypt(blob)

    assert reader.stats().data_keys_unwrapped == 0


async def test_cold_sync_calls_count_cold_misses() -> None:
    producer = _ring()
    await producer.warm(None)
    blob = producer.encrypt_sync(b"secret", tenant=None)

    cold = _ring()  # never warmed

    with pytest.raises(CoreException):
        cold.decrypt_sync(blob)

    with pytest.raises(CoreException):
        cold.encrypt_sync(b"x", tenant=None)

    stats = cold.stats()
    assert stats.cold_misses == 2
    assert stats.decrypt_cache_hits == 0
    assert stats.encrypt_cache_hits == 0

"""Distributed cache body encryption (DocumentCache.cipher): ciphertext at rest in the
backend cache, plaintext model after a cold read."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.integrations.crypto import Keyring, is_encrypted_payload
from forze.application.integrations.document.cache import DocumentCache
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockKeyManagement
from forze_mock.adapters import MockCacheAdapter, MockState

# ----------------------- #


class _Doc(BaseModel):
    id: UUID
    rev: int
    secret: str


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _doc_cache(cache: MockCacheAdapter, cipher: Keyring | None) -> DocumentCache[_Doc]:
    return DocumentCache(
        read_model_type=_Doc,
        read_codec=PydanticModelCodec(_Doc),
        document_name="docs",
        cache=cache,
        cipher=cipher,
        cipher_tenant=lambda: None,
    )


async def _no_fault() -> _Doc:
    raise AssertionError("should be served from cache, not the gateway")


# ....................... #


@pytest.mark.asyncio
async def test_distributed_cache_body_is_sealed_then_read_back() -> None:
    state = MockState()
    cache = MockCacheAdapter(state=state, namespace="docs")
    keyring = _keyring()

    doc = _Doc(id=uuid4(), rev=1, secret="hunter2")
    await _doc_cache(cache, keyring).set_one(doc)

    # Ciphertext at rest: the backend cache holds the envelope wrapper, not the model.
    raw = await cache.get(str(doc.id))
    assert is_encrypted_payload(raw)
    assert "hunter2" not in str(raw)

    # A fresh reader (cold L1) decrypts it back to the plaintext model.
    got = await _doc_cache(cache, keyring).get_read_through(
        doc.id,
        fetch_on_cache_fault=_no_fault,
        fetch_on_miss_without_lock=_no_fault,
    )
    assert got.secret == "hunter2"


@pytest.mark.asyncio
async def test_without_cipher_caches_plaintext() -> None:
    state = MockState()
    cache = MockCacheAdapter(state=state, namespace="docs")

    doc = _Doc(id=uuid4(), rev=1, secret="plain")
    await _doc_cache(cache, None).set_one(doc)

    raw = await cache.get(str(doc.id))
    assert not is_encrypted_payload(raw)


@pytest.mark.asyncio
async def test_cross_key_aad_rejects_transplant() -> None:
    """A sealed entry copied to another pk's slot fails to decrypt (AAD binds the pk)."""

    state = MockState()
    cache = MockCacheAdapter(state=state, namespace="docs")
    keyring = _keyring()

    doc = _Doc(id=uuid4(), rev=1, secret="s")
    await _doc_cache(cache, keyring).set_one(doc)
    sealed = await cache.get(str(doc.id))

    # Move the ciphertext into a different pk's slot.
    other = uuid4()
    await cache.set_versioned(str(other), "1", sealed, ttl=None)

    with pytest.raises(Exception):
        await _doc_cache(cache, keyring).get_read_through(
            other,
            fetch_on_cache_fault=_no_fault,
            fetch_on_miss_without_lock=_no_fault,
        )

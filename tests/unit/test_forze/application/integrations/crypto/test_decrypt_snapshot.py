"""Frozen decrypt snapshot: parity with the live codec and thread-safety off the loop.

``EncryptingModelCodec.freeze_for_decrypt`` returns a copy whose ciphers read pre-resolved
keys from thread-local dicts (no shared, LRU-mutating cache), so a batch decrypt can run
under ``run_cpu_map`` without the worker threads racing on the keyring's / deterministic
cipher's caches.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from uuid import uuid4

from pydantic import BaseModel

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import (
    DeterministicFieldCipher,
    EncryptingModelCodec,
    Keyring,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import run_cpu_map
from forze.base.serialization import default_model_codec

from forze_mock import MockKeyManagement

_tenant_var: ContextVar[TenantIdentity | None] = ContextVar("test_tenant", default=None)

# ----------------------- #


class _Profile(BaseModel):
    id: str
    name: str
    email: str
    prefs: dict[str, str] = {}


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _codec(
    ring: Keyring,
    *,
    fields: frozenset[str] = frozenset({"email", "prefs"}),
    searchable: frozenset[str] = frozenset(),
    det: DeterministicFieldCipher | None = None,
) -> EncryptingModelCodec[_Profile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=ring,
        fields=fields,
        searchable_fields=searchable,
        deterministic=det,
        tenant_provider=lambda: None,
    )


def _rows(codec: EncryptingModelCodec[_Profile], n: int) -> list[dict]:
    return [
        codec.encode_persistence_mapping(
            _Profile(id=str(i), name=f"n{i}", email=f"e{i}@x.com", prefs={"k": str(i)})
        )
        for i in range(n)
    ]


# ....................... #


async def test_frozen_codec_decrypts_identically_to_live() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()
    rows = _rows(codec, 5)
    await codec.prepare_decrypt(rows)

    frozen = codec.freeze_for_decrypt(rows)
    assert frozen is not None

    for row in rows:
        assert frozen.decode_mapping(dict(row)) == codec.decode_mapping(dict(row))
        assert frozen.decrypt_mapping(dict(row)) == codec.decrypt_mapping(dict(row))


async def test_frozen_codec_covers_searchable_fields() -> None:
    ring = _keyring()
    det = DeterministicFieldCipher(root=b"x" * 32)
    codec = _codec(
        ring,
        fields=frozenset({"prefs"}),
        searchable=frozenset({"email"}),
        det=det,
    )
    await codec.prepare_encrypt()
    rows = _rows(codec, 4)
    await codec.prepare_decrypt(rows)

    frozen = codec.freeze_for_decrypt(rows)
    assert frozen is not None

    for row in rows:
        assert frozen.decode_mapping(dict(row)) == codec.decode_mapping(dict(row))


def test_frozen_codec_is_thread_safe() -> None:
    # The whole point: many rows decrypted concurrently across threads (as run_cpu_map's
    # workers would, across concurrent requests) with no race on the ciphers' shared LRU
    # caches — because the frozen ciphers read thread-local key snapshots.
    ring = _keyring()
    det = DeterministicFieldCipher(root=b"y" * 32)
    codec = _codec(
        ring,
        fields=frozenset({"email", "prefs"}),
        searchable=frozenset(),
        det=det,
    )
    asyncio.run(codec.prepare_encrypt())
    rows = _rows(codec, 300)
    asyncio.run(codec.prepare_decrypt(rows))

    frozen = codec.freeze_for_decrypt(rows)
    assert frozen is not None

    with ThreadPoolExecutor(max_workers=8) as pool:
        decoded = list(pool.map(lambda r: frozen.decode_mapping(dict(r)), rows))

    assert len(decoded) == 300
    assert all(isinstance(p, _Profile) for p in decoded)
    assert {p.id for p in decoded} == {str(i) for i in range(300)}


async def test_frozen_codec_is_decrypt_only() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()
    rows = _rows(codec, 2)
    await codec.prepare_decrypt(rows)

    frozen = codec.freeze_for_decrypt(rows)
    assert frozen is not None

    # The snapshot ciphers cannot encrypt — the copy is strictly for reads.
    import pytest

    with pytest.raises(CoreException):
        frozen.encode_persistence_mapping(
            _Profile(id="9", name="z", email="z@x.com")
        )


def test_freeze_returns_none_without_snapshot_support() -> None:
    # A field cipher that cannot snapshot (no freeze_decryptor) → the codec declines to
    # freeze and the caller decrypts inline.
    class _PlainCipher:
        async def warm(self, tenant) -> None: ...
        async def ensure_unwrapped(self, envelopes) -> None: ...
        def encrypt_sync(self, plaintext, *, tenant, aad=b"") -> bytes:
            return plaintext
        def decrypt_sync(self, blob, *, aad=b"") -> bytes:
            return blob

    codec = EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=_PlainCipher(),  # type: ignore[arg-type]
        fields=frozenset({"email"}),
        tenant_provider=lambda: None,
    )

    assert codec.freeze_for_decrypt([{"email": "x"}]) is None


async def test_offload_via_run_cpu_map_preserves_tenant_aad() -> None:
    # The end-to-end offload path: decrypt a large batch through the real run_cpu_map (thread
    # pool). The AAD is tenant-bound, so the tenant ContextVar must be copied into the worker
    # or decrypt would fail — this proves the whole path (frozen ciphers + context copy).
    ring = _keyring()
    codec = EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=ring,
        fields=frozenset({"email", "prefs"}),
        tenant_provider=_tenant_var.get,
    )
    tenant = TenantIdentity(tenant_id=uuid4())
    token = _tenant_var.set(tenant)

    try:
        await codec.prepare_encrypt()
        rows = [
            codec.encode_persistence_mapping(
                _Profile(id=str(i), name=f"n{i}", email=f"e{i}@x.com", prefs={"k": str(i)})
            )
            for i in range(80)  # >= _DECRYPT_OFFLOAD_THRESHOLD
        ]
        await codec.prepare_decrypt(rows)

        frozen = codec.freeze_for_decrypt(rows)
        assert frozen is not None

        decoded = await run_cpu_map(rows, lambda r: frozen.decode_mapping(dict(r)))

        assert len(decoded) == 80
        assert decoded[3].email == "e3@x.com"  # AAD matched in the worker → correct decrypt
        assert {p.id for p in decoded} == {str(i) for i in range(80)}

    finally:
        _tenant_var.reset(token)

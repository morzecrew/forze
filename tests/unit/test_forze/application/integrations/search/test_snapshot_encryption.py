"""Search result snapshot at-rest encryption: seal stored record models, open on replay."""

from __future__ import annotations

import json
from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.crypto import Keyring
from forze.application.contracts.search import SearchResultSnapshotSpec
from forze.application.integrations.search import (
    SearchResultSnapshot,
    resolve_snapshot_cipher,
)
from forze.application.integrations.search.snapshot import _SEALED_PREFIX
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement
from forze_mock.adapters.search.snapshot import MockSearchResultSnapshotAdapter
from forze_mock.state import MockState

# ----------------------- #


def test_resolve_snapshot_cipher_fail_closed_without_keyring() -> None:
    # An encrypted route must not silently snapshot plaintext when no keyring is wired.
    with pytest.raises(CoreException) as ei:
        resolve_snapshot_cipher(encrypted=True, keyring=None)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.search.snapshot_encryption_wiring"


def test_resolve_snapshot_cipher_none_when_not_encrypted() -> None:
    assert resolve_snapshot_cipher(encrypted=False, keyring=None) is None


# ----------------------- #


class _Hit(BaseModel):
    id: str
    ssn: str


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _snap(cipher: Keyring | None) -> SearchResultSnapshot:
    # store is unused by the seal/open helpers under test.
    return SearchResultSnapshot(
        store=object(),  # type: ignore[arg-type]
        cipher=cipher,
        cipher_tenant=lambda: None,
    )


@attrs.define(slots=True)
class _CountingKeyring:
    """Delegates to a real keyring while counting the async warm pre-passes."""

    inner: Keyring
    warm_calls: int = 0
    unwrap_calls: int = 0

    async def warm(self, tenant: Any) -> None:
        self.warm_calls += 1
        await self.inner.warm(tenant)

    async def ensure_unwrapped(self, envelopes: Any) -> None:
        self.unwrap_calls += 1
        await self.inner.ensure_unwrapped(envelopes)

    def encrypt_sync(self, plaintext: bytes, *, tenant: Any, aad: bytes = b"") -> bytes:
        return self.inner.encrypt_sync(plaintext, tenant=tenant, aad=aad)

    def decrypt_sync(self, blob: bytes, *, aad: bytes = b"") -> bytes:
        return self.inner.decrypt_sync(blob, aad=aad)

    async def encrypt(self, plaintext: bytes, *, tenant: Any, aad: bytes = b"") -> bytes:
        return await self.inner.encrypt(plaintext, tenant=tenant, aad=aad)

    async def decrypt(self, blob: bytes, *, aad: bytes = b"") -> bytes:
        return await self.inner.decrypt(blob, aad=aad)


@pytest.mark.asyncio
async def test_batch_warms_once_then_sync_seals_and_opens() -> None:
    counting = _CountingKeyring(inner=_keyring())
    snap = SearchResultSnapshot(
        store=object(),  # type: ignore[arg-type]
        cipher=counting,  # type: ignore[arg-type]
        cipher_tenant=lambda: None,
    )
    keys = [json.dumps({"id": str(i), "ssn": f"s{i}"}) for i in range(25)]

    sealed = await snap._seal_ids(keys, run_id="run-1")
    opened = await snap._open_ids(sealed, run_id="run-1")

    assert opened == keys  # round-trips for the whole batch
    assert counting.warm_calls == 1  # one warm for 25 records, not 25
    assert counting.unwrap_calls == 1  # one decrypt pre-pass for the whole batch


# ....................... #


@pytest.mark.asyncio
async def test_seal_hides_models_and_open_round_trips() -> None:
    snap = _snap(_keyring())
    keys = [SearchResultSnapshot.result_record_key_string(_Hit(id="1", ssn="secret"))]

    sealed = await snap._seal_ids(keys, run_id="run-1")

    # At rest the record is a sealed token, not the model JSON.
    assert sealed[0].startswith(_SEALED_PREFIX)
    assert "secret" not in sealed[0]

    opened = await snap._open_ids(sealed, run_id="run-1")
    assert opened == keys
    assert SearchResultSnapshot.hydrate_result_record_key(opened[0], _Hit).ssn == "secret"


@pytest.mark.asyncio
async def test_no_cipher_is_plaintext_passthrough() -> None:
    snap = _snap(None)
    keys = ['{"id":"1","ssn":"x"}']

    assert await snap._seal_ids(keys, run_id="r") == keys
    assert await snap._open_ids(keys, run_id="r") == keys


@pytest.mark.asyncio
async def test_legacy_plaintext_keys_still_open() -> None:
    # A run written before encryption was enabled (plaintext JSON keys, no sentinel).
    snap = _snap(_keyring())
    legacy = [json.dumps({"id": "1", "ssn": "x"})]

    assert await snap._open_ids(legacy, run_id="r") == legacy


@pytest.mark.asyncio
async def test_open_fails_closed_when_sealed_but_no_cipher() -> None:
    sealed = await _snap(_keyring())._seal_ids(['{"id":"1"}'], run_id="r")

    with pytest.raises(CoreException) as ei:
        await _snap(None)._open_ids(sealed, run_id="r")

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.search.snapshot_encryption_wiring"


@pytest.mark.asyncio
async def test_streaming_sink_seals_per_chunk_and_round_trips() -> None:
    # The chunked streaming build seals each chunk under the run id; replay opens them all
    # — identical at-rest protection to the one-shot ``put_simple_ordered_hits`` path.
    rs_spec = SearchResultSnapshotSpec(name="snap", enabled=True, chunk_size=2)
    store = MockSearchResultSnapshotAdapter(state=MockState(), spec=rs_spec)
    snap = SearchResultSnapshot(
        store=store, cipher=_keyring(), cipher_tenant=lambda: None
    )
    keys = [
        SearchResultSnapshot.result_record_key_string(_Hit(id=str(i), ssn=f"secret-{i}"))
        for i in range(5)
    ]

    sink = snap.open_simple_hit_sink(snap_opt=None, rs_spec=rs_spec, fp_computed="fp")
    await sink.add(keys[:3])
    await sink.add(keys[3:])
    handle = await sink.finish(pool_len_before_cap=len(keys))

    assert handle.total == 5

    stored = await store.get_id_range(handle.id, 0, 10, expected_fingerprint="fp")
    assert stored is not None
    assert all(record.startswith(_SEALED_PREFIX) for record in stored)
    assert not any("secret" in record for record in stored)

    assert await snap._open_ids(stored, run_id=handle.id) == keys


@pytest.mark.asyncio
async def test_aad_binds_run_id() -> None:
    snap = _snap(_keyring())
    sealed = await snap._seal_ids(['{"id":"1"}'], run_id="run-A")

    # The same sealed token cannot be opened under a different run id (AAD mismatch).
    with pytest.raises(CoreException):
        await snap._open_ids(sealed, run_id="run-B")

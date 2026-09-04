"""Idempotency result-cache encryption: seal on commit, open on replay, fail-closed."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.idempotency import IdempotencyPort, IdempotencyRecord
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.idempotency import (
    EncryptingIdempotencyPort,
    encrypting_idempotency_port,
)
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement, MockState
from forze_mock.adapters.idempotency import MockIdempotencyAdapter

# ----------------------- #


@attrs.define(slots=True)
class _FakeStore(IdempotencyPort):
    """In-memory idempotency port recording exactly what bytes were committed."""

    records: dict[tuple[str, str | None], IdempotencyRecord] = attrs.field(factory=dict)

    async def begin(
        self, op: str, key: str | None, payload_hash: str
    ) -> IdempotencyRecord | None:
        return self.records.get((op, key))

    async def commit(
        self, op: str, key: str | None, payload_hash: str, record: IdempotencyRecord
    ) -> None:
        self.records[(op, key)] = record

    async def fail(self, op: str, key: str | None, payload_hash: str) -> None:
        self.records.pop((op, key), None)


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _wrapped(store: _FakeStore) -> EncryptingIdempotencyPort:
    return EncryptingIdempotencyPort(
        inner=store, cipher=_keyring(), tenant_provider=lambda: None
    )


# ....................... #


@pytest.mark.asyncio
async def test_commit_seals_result_and_begin_opens_it() -> None:
    store = _FakeStore()
    port = _wrapped(store)

    await port.commit("create_order", "idem-1", "h", IdempotencyRecord(result=b'{"id":1}'))

    # At rest the cached result is a sealed envelope, not the plaintext bytes.
    sealed = store.records[("create_order", "idem-1")].result
    assert is_envelope(sealed)
    assert b'{"id":1}' not in sealed

    # Replay opens it back to the original plaintext.
    replayed = await port.begin("create_order", "idem-1", "h")
    assert replayed is not None
    assert replayed.result == b'{"id":1}'


@pytest.mark.asyncio
async def test_legacy_plaintext_record_still_replays() -> None:
    store = _FakeStore()
    # A record written before encryption was enabled (plaintext bytes, no envelope).
    store.records[("op", "k")] = IdempotencyRecord(result=b'{"v":2}')

    replayed = await _wrapped(store).begin("op", "k", "h")

    assert replayed is not None and replayed.result == b'{"v":2}'


@pytest.mark.asyncio
async def test_no_key_is_passthrough() -> None:
    store = _FakeStore()
    port = _wrapped(store)

    # key=None means idempotency is skipped; nothing is sealed.
    await port.commit("op", None, "h", IdempotencyRecord(result=b"raw"))
    assert store.records[("op", None)].result == b"raw"


@pytest.mark.asyncio
async def test_aad_binds_op_and_key() -> None:
    store = _FakeStore()
    port = _wrapped(store)

    await port.commit("op-a", "k", "h", IdempotencyRecord(result=b"secret"))
    sealed = store.records[("op-a", "k")].result

    # Transplant the ciphertext to a different (op, key) — AAD mismatch must fail to open.
    store.records[("op-b", "k")] = IdempotencyRecord(result=sealed)
    with pytest.raises(CoreException):
        await port.begin("op-b", "k", "h")


def test_fail_closed_without_keyring() -> None:
    with pytest.raises(CoreException) as ei:
        encrypting_idempotency_port(
            _FakeStore(), cipher=None, tenant_provider=lambda: None, spec_name="orders"
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.idempotency.encryption_wiring"


@pytest.mark.asyncio
async def test_the_wrapper_does_not_disarm_the_ownership_fence() -> None:
    """Encryption is transparent to claim ownership: it forwards, it does not carry.

    The wrapper delegates ``commits_in_transaction`` explicitly, so the obvious question is
    whether the owner needs the same treatment. It does not — the fence lives in the inner
    store, which reads the ambient invocation itself — and this is the assertion that keeps
    the answer true: a wrapper that swallowed or re-derived the owner would let a reclaimed
    claim be committed through the encrypting path but not the plain one.
    """

    state = MockState()

    def _store(owner: UUID) -> IdempotencyPort:
        return encrypting_idempotency_port(
            MockIdempotencyAdapter(
                state=state,
                namespace="idem",
                ttl=timedelta(milliseconds=50),
                owner_provider=lambda: owner,
            ),
            cipher=_keyring(),
            tenant_provider=lambda: None,
            spec_name="idem",
        )

    displaced = _store(uuid4())

    assert await displaced.begin("op", "k", "hash") is None

    await asyncio.sleep(0.1)

    reclaimer = _store(uuid4())

    assert await reclaimer.begin("op", "k", "hash") is None

    with pytest.raises(CoreException) as ei:
        await displaced.commit("op", "k", "hash", IdempotencyRecord(result=b"stale"))

    assert ei.value.kind == ExceptionKind.CONFLICT

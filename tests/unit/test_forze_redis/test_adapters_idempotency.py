import hashlib
from datetime import timedelta
from typing import Any, Sequence
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.idempotency import IdempotencyRecord
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.codecs import JsonCodec
from forze.base.exceptions import CoreException
from forze_redis.adapters.codecs import RedisKeyCodec
from forze_redis.adapters.idempotency import RedisIdempotencyAdapter
from forze_redis.kernel.scripts import IDEMPOTENCY_COMMIT, IDEMPOTENCY_RELEASE

_TID = UUID("12345678-1234-5678-1234-567812345678")
_NS = "test"
_CODEC = JsonCodec()


def _digest(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _pending_bytes(payload_hash: str) -> bytes:
    return _CODEC.dumps({"st": "P", "ph": payload_hash})


# --------------------------------------------------------------------------- #
# A minimal in-memory client that actually executes the two idempotency Lua
# scripts (byte-exact compare-and-set / compare-and-delete), so the fencing and
# collision guarantees are exercised end-to-end rather than mocked away.
# --------------------------------------------------------------------------- #


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        if nx and key in self.store:
            return False
        if xx and key not in self.store:
            return False
        self.store[key] = value
        return True

    async def get(self, key: str) -> Any:
        return self.store.get(key)

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                removed += 1
        return removed

    async def run_script(
        self, script: str, keys: Sequence[str], args: Sequence[Any]
    ) -> str:
        if script is IDEMPOTENCY_COMMIT:
            meta_k, body_k = keys
            expected_pending, done_meta, body, _ex = args
            if self.store.get(meta_k) != expected_pending:
                return "0"
            self.store[body_k] = body
            self.store[meta_k] = done_meta
            return "1"

        if script is IDEMPOTENCY_RELEASE:
            meta_k, body_k = keys
            (expected_pending,) = args
            if self.store.get(meta_k) != expected_pending:
                return "0"
            return str(await self.delete(meta_k, body_k))

        raise AssertionError(f"unexpected script: {script!r}")


@pytest.fixture
def mock_redis_client() -> MagicMock:
    client = MagicMock()
    client.set = AsyncMock(return_value=True)
    client.get = AsyncMock(return_value=None)
    client.delete = AsyncMock(return_value=1)
    client.run_script = AsyncMock(return_value="1")
    return client


def _adapter(client: object, *, tenant: bool) -> RedisIdempotencyAdapter:
    kwargs: dict[str, Any] = {}
    if tenant:
        kwargs = {
            "tenant_aware": True,
            "tenant_provider": lambda: TenantIdentity(tenant_id=_TID),
        }
    return RedisIdempotencyAdapter(
        client=client,  # type: ignore[arg-type]
        namespace=_NS,
        ttl=timedelta(seconds=60),
        **kwargs,
    )


@pytest.fixture
def adapter_with_tenant(mock_redis_client: MagicMock) -> RedisIdempotencyAdapter:
    return _adapter(mock_redis_client, tenant=True)


@pytest.fixture
def adapter_without_tenant(mock_redis_client: MagicMock) -> RedisIdempotencyAdapter:
    return _adapter(mock_redis_client, tenant=False)


def _meta_with_tenant(key: str = "test-key") -> str:
    return f"tenant:{_TID}:idempotency:{_NS}:op:{_digest(key)}"


def _meta_without_tenant(key: str = "test-key") -> str:
    return f"idempotency:{_NS}:op:{_digest(key)}"


def _body_with_tenant(key: str = "test-key") -> str:
    return f"tenant:{_TID}:idempotency-body:{_NS}:op:{_digest(key)}"


def _body_without_tenant(key: str = "test-key") -> str:
    return f"idempotency-body:{_NS}:op:{_digest(key)}"


def _meta(adapter: RedisIdempotencyAdapter, op: str, key: str) -> str:
    return adapter._RedisIdempotencyAdapter__meta_key(op, key)  # type: ignore[attr-defined]


def _body(adapter: RedisIdempotencyAdapter, op: str, key: str) -> str:
    return adapter._RedisIdempotencyAdapter__body_key(op, key)  # type: ignore[attr-defined]


# --------------------------------------------------------------------------- #
# Key format (hashed key + separate body scope).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_key_generation_with_tenant(
    adapter_with_tenant: RedisIdempotencyAdapter,
) -> None:
    assert _meta(adapter_with_tenant, "op", "test-key") == _meta_with_tenant()


@pytest.mark.asyncio
async def test_key_generation_without_tenant(
    adapter_without_tenant: RedisIdempotencyAdapter,
) -> None:
    assert _meta(adapter_without_tenant, "op", "test-key") == _meta_without_tenant()


@pytest.mark.asyncio
async def test_body_key_generation_with_tenant(
    adapter_with_tenant: RedisIdempotencyAdapter,
) -> None:
    assert _body(adapter_with_tenant, "op", "test-key") == _body_with_tenant()


# --------------------------------------------------------------------------- #
# BUG 1: an untrusted key cannot corrupt another key's stored body.
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_key_body_suffix_cannot_alias_another_meta(
    adapter_with_tenant: RedisIdempotencyAdapter,
) -> None:
    # The historical corruption: __meta_key(op, "k:body") == __body_key(op, "k").
    # Distinct scope + hashing make every meta/body pair structurally disjoint.
    assert _meta(adapter_with_tenant, "op", "k:body") != _body(
        adapter_with_tenant, "op", "k"
    )
    assert ":idempotency:" in _meta(adapter_with_tenant, "op", "k:body")
    assert ":idempotency-body:" in _body(adapter_with_tenant, "op", "k")


@pytest.mark.asyncio
async def test_kbody_request_cannot_overwrite_k_body() -> None:
    client = _FakeRedis()
    adapter = _adapter(client, tenant=True)

    # Request for key "k" completes and stores its result body.
    assert await adapter.begin("op", "k", "hash-k") is None
    await adapter.commit("op", "k", "hash-k", IdempotencyRecord(result=b"body-of-k"))
    k_body_key = _body(adapter, "op", "k")
    assert client.store[k_body_key] == b"body-of-k"

    # A separate request whose key is the adversarial "k:body" runs to commit.
    assert await adapter.begin("op", "k:body", "hash-kb") is None
    await adapter.commit(
        "op", "k:body", "hash-kb", IdempotencyRecord(result=b"body-of-kbody")
    )

    # "k"'s stored body is untouched, and a replay of "k" returns its own bytes.
    assert client.store[k_body_key] == b"body-of-k"
    replay = await adapter.begin("op", "k", "hash-k")
    assert replay is not None
    assert replay.result == b"body-of-k"


@pytest.mark.asyncio
async def test_fail_kbody_cannot_delete_k_body() -> None:
    client = _FakeRedis()
    adapter = _adapter(client, tenant=True)

    await adapter.begin("op", "k", "hash-k")
    await adapter.commit("op", "k", "hash-k", IdempotencyRecord(result=b"body-of-k"))
    k_meta_key = _meta(adapter, "op", "k")
    k_body_key = _body(adapter, "op", "k")

    # fail("k:body") must not touch "k"'s meta/body keys.
    await adapter.fail("op", "k:body", "hash-kb")

    assert k_meta_key in client.store
    assert client.store[k_body_key] == b"body-of-k"


# --------------------------------------------------------------------------- #
# BUG 2: commit is fenced to the caller's own pending claim.
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_commit_fails_when_claim_reacquired_by_other_writer() -> None:
    client = _FakeRedis()
    adapter = _adapter(client, tenant=True)

    # Writer A claims the key.
    assert await adapter.begin("op", "k", "hash-A") is None
    meta_key = _meta(adapter, "op", "k")
    body_key = _body(adapter, "op", "k")

    # A's claim lapsed and writer B re-acquired it with a different payload hash.
    client.store[meta_key] = _pending_bytes("hash-B")

    # A's commit must not overwrite B's claim nor write a body.
    with pytest.raises(CoreException, match="Idempotency commit failed"):
        await adapter.commit("op", "k", "hash-A", IdempotencyRecord(result=b"A-body"))

    assert client.store[meta_key] == _pending_bytes("hash-B")
    assert body_key not in client.store


@pytest.mark.asyncio
async def test_commit_fails_when_claim_missing() -> None:
    client = _FakeRedis()
    adapter = _adapter(client, tenant=True)

    # No begin() first: nothing to fence against.
    with pytest.raises(CoreException, match="Idempotency commit failed"):
        await adapter.commit("op", "k", "hash-A", IdempotencyRecord(result=b"A-body"))


@pytest.mark.asyncio
async def test_commit_succeeds_for_own_pending_claim() -> None:
    client = _FakeRedis()
    adapter = _adapter(client, tenant=True)

    await adapter.begin("op", "k", "hash-A")
    await adapter.commit("op", "k", "hash-A", IdempotencyRecord(result=b"A-body"))

    assert client.store[_body(adapter, "op", "k")] == b"A-body"
    replay = await adapter.begin("op", "k", "hash-A")
    assert replay is not None
    assert replay.result == b"A-body"


@pytest.mark.asyncio
async def test_commit_uses_cas_script_with_pending_and_done_meta(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    await adapter_with_tenant.commit(
        "op", "test-key", "hash123", IdempotencyRecord(result=b"test-body")
    )

    mock_redis_client.run_script.assert_awaited_once()
    script, keys, args = mock_redis_client.run_script.call_args[0]
    assert script is IDEMPOTENCY_COMMIT
    assert keys == [_meta_with_tenant(), _body_with_tenant()]
    assert args[0] == _pending_bytes("hash123")  # fence: caller's own claim
    assert args[1] == _CODEC.dumps({"st": "D", "ph": "hash123"})
    assert args[2] == b"test-body"


@pytest.mark.asyncio
async def test_commit_no_key(
    adapter_with_tenant: RedisIdempotencyAdapter, mock_redis_client: MagicMock
) -> None:
    await adapter_with_tenant.commit(
        "op", None, "hash123", IdempotencyRecord(result=b"test-body")
    )
    mock_redis_client.run_script.assert_not_called()


@pytest.mark.asyncio
async def test_commit_failed_missing_or_expired(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.run_script.return_value = "0"

    with pytest.raises(CoreException, match="Idempotency commit failed"):
        await adapter_with_tenant.commit(
            "op", "test-key", "hash123", IdempotencyRecord(result=b"test-body")
        )


# --------------------------------------------------------------------------- #
# fail() uses the compare-and-delete script.
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fail_uses_release_script(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    await adapter_with_tenant.fail("op", "test-key", "hash123")

    mock_redis_client.run_script.assert_awaited_once()
    script, keys, args = mock_redis_client.run_script.call_args[0]
    assert script is IDEMPOTENCY_RELEASE
    assert keys == [_meta_with_tenant(), _body_with_tenant()]
    assert args == [_pending_bytes("hash123")]


@pytest.mark.asyncio
async def test_fail_no_key_is_noop(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    await adapter_with_tenant.fail("op", None, "hash123")
    mock_redis_client.run_script.assert_not_called()


@pytest.mark.asyncio
async def test_fail_leaves_done_record_untouched() -> None:
    client = _FakeRedis()
    adapter = _adapter(client, tenant=True)

    await adapter.begin("op", "k", "hash-A")
    await adapter.commit("op", "k", "hash-A", IdempotencyRecord(result=b"A-body"))

    # A record already flipped to DONE is not a pending claim: fail() is a no-op.
    await adapter.fail("op", "k", "hash-A")

    assert _meta(adapter, "op", "k") in client.store
    assert client.store[_body(adapter, "op", "k")] == b"A-body"


@pytest.mark.asyncio
async def test_fail_leaves_other_payload_claim_untouched() -> None:
    client = _FakeRedis()
    adapter = _adapter(client, tenant=True)

    await adapter.begin("op", "k", "hash-A")
    # Another writer's pending claim (different ph) must survive our fail().
    client.store[_meta(adapter, "op", "k")] = _pending_bytes("hash-B")

    await adapter.fail("op", "k", "hash-A")

    assert client.store[_meta(adapter, "op", "k")] == _pending_bytes("hash-B")


# --------------------------------------------------------------------------- #
# begin() paths (unchanged semantics over the new key format).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_begin_success_with_tenant(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.set.return_value = True
    result = await adapter_with_tenant.begin("op", "test-key", "hash123")

    mock_redis_client.set.assert_called_once()
    args, _kwargs = mock_redis_client.set.call_args
    assert args[0] == _meta_with_tenant()
    assert result is None


@pytest.mark.asyncio
async def test_begin_no_key(
    adapter_with_tenant: RedisIdempotencyAdapter, mock_redis_client: MagicMock
) -> None:
    result = await adapter_with_tenant.begin("op", None, "hash123")
    mock_redis_client.set.assert_not_called()
    assert result is None


@pytest.mark.asyncio
async def test_begin_conflict_not_readable(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.set.side_effect = [False, False]
    mock_redis_client.get.return_value = None

    with pytest.raises(CoreException, match="not readable"):
        await adapter_with_tenant.begin("op", "test-key", "hash123")


@pytest.mark.asyncio
async def test_begin_conflict_hash_mismatch(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.set.return_value = False
    mock_redis_client.get.return_value = _pending_bytes("wrong_hash")

    with pytest.raises(CoreException, match="Payload hash mismatch"):
        await adapter_with_tenant.begin("op", "test-key", "hash123")


# --------------------------------------------------------------------------- #
# RedisKeyCodec.join hardening.
# --------------------------------------------------------------------------- #


def test_join_rejects_empty_part() -> None:
    # The old ``if p`` dropped ``""``, aliasing it with an absent ``None`` part.
    codec = RedisKeyCodec(namespace=_NS)
    with pytest.raises(CoreException, match="must not be empty"):
        codec.join(None, "scope", "")


def test_join_drops_none_trailing_part() -> None:
    # ``None`` remains an absent optional segment (counter relies on this) and is
    # therefore distinct from a rejected empty part.
    codec = RedisKeyCodec(namespace=_NS)
    assert codec.join(None, "scope", None) == f"scope:{_NS}"


@pytest.mark.parametrize("part", [":a", "a:"])
def test_join_no_longer_aliases_edge_separators(part: str) -> None:
    # Previously ``strip(sep)`` normalized these to alias ``"a"``; kept verbatim
    # they stay distinct keys (embedded separators are legitimate, not rejected).
    codec = RedisKeyCodec(namespace=_NS)
    assert codec.join(None, "scope", part) != codec.join(None, "scope", "a")


def test_join_passes_embedded_separator_verbatim() -> None:
    # Operation names / logical keys legitimately contain ``:``.
    codec = RedisKeyCodec(namespace=_NS)
    assert codec.join(None, "scope", "orders:42") == f"scope:{_NS}:orders:42"

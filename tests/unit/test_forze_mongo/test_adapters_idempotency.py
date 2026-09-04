"""Unit tests for :class:`MongoIdempotencyStore` (mocked client)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from itertools import count
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.idempotency import IdempotencyRecord, IdempotencySpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_mongo.adapters.idempotency import MongoIdempotencyStore
from forze_mongo.execution.deps.configs import MongoIdempotencyConfig

# ----------------------- #

OP = "place_order"
HASH_A = "hash-aaaa"


def _client() -> AsyncMock:
    client = AsyncMock()

    @asynccontextmanager
    async def _detached() -> AsyncIterator[None]:
        yield

    # ``detached`` is a *sync* call returning an async context manager, so it must not be
    # an AsyncMock attribute (awaiting the mock would replace the context manager).
    client.detached = MagicMock(side_effect=_detached)
    return client


def _store(
    client: AsyncMock,
    *,
    tenant: UUID | None = None,
) -> MongoIdempotencyStore:
    tenant_aware = tenant is not None

    return MongoIdempotencyStore(
        client=client,
        spec=IdempotencySpec(name="idem"),
        config=MongoIdempotencyConfig(
            collection=("app", "idem"),
            tenant_aware=tenant_aware,
        ),
        tenant_aware=tenant_aware,
        tenant_provider=(lambda: TenantIdentity(tenant_id=tenant)) if tenant else (lambda: None),
    )


# ....................... #


def test_commits_in_transaction_is_declared() -> None:
    """The co-located claim the hook reads to drive the in-transaction record write."""

    assert _store(_client()).commits_in_transaction is True


@pytest.mark.asyncio
async def test_a_null_key_touches_the_store_at_all() -> None:
    """No key, no I/O: begin/commit/fail return without reaching the client."""

    client = _client()
    store = _store(client)

    assert await store.begin(OP, None, HASH_A) is None
    await store.commit(OP, None, HASH_A, IdempotencyRecord(result=b"x"))
    await store.fail(OP, None, HASH_A)

    client.find_one_and_update.assert_not_awaited()
    client.update_one.assert_not_awaited()
    client.delete_one.assert_not_awaited()


@pytest.mark.asyncio
async def test_claim_and_release_run_detached_commit_does_not() -> None:
    """``begin`` / ``fail`` leave the caller's session; ``commit`` rides it — the whole
    co-located design in one assertion."""

    client = _client()
    store = _store(client)
    client.find_one_and_update = AsyncMock(
        side_effect=lambda _c, _f, u, **_kw: dict(u["$setOnInsert"]),
    )
    client.update_one = AsyncMock(return_value=1)

    await store.begin(OP, "k", HASH_A)
    assert client.detached.call_count == 1

    await store.fail(OP, "k", HASH_A)
    assert client.detached.call_count == 2

    await store.commit(OP, "k", HASH_A, IdempotencyRecord(result=b"x"))
    assert client.detached.call_count == 2


@pytest.mark.asyncio
async def test_commit_without_a_pending_claim_is_a_conflict() -> None:
    """A matched count of zero means the claim expired or was taken by another writer:
    refuse so the business transaction rolls back with it."""

    client = _client()
    client.update_one = AsyncMock(return_value=0)
    store = _store(client)

    with pytest.raises(CoreException) as ei:
        await store.commit(OP, "k", HASH_A, IdempotencyRecord(result=b"x"))

    assert ei.value.kind == ExceptionKind.CONFLICT


@pytest.mark.asyncio
async def test_commit_and_fail_are_fenced_on_the_claim() -> None:
    """Both address the document by ``_id`` *and* require the caller's own pending claim,
    so neither can overwrite or release another writer's."""

    client = _client()
    client.update_one = AsyncMock(return_value=1)
    store = _store(client)

    await store.commit(OP, "k", HASH_A, IdempotencyRecord(result=b"x"))
    commit_filter = client.update_one.await_args.args[1]

    await store.fail(OP, "k", HASH_A)
    fail_filter = client.delete_one.await_args.args[1]

    for flt in (commit_filter, fail_filter):
        assert flt["_id"] == store._doc_id(OP, "k", None)
        assert flt["payload_hash"] == HASH_A
        assert flt["status"] == "pending"


def test_doc_id_is_unambiguous_for_separator_contents() -> None:
    store = _store(_client())

    # Without the length prefix these two pairs would collide on "a|b|c".
    assert store._doc_id("a|b", "c", None) != store._doc_id("a", "b|c", None)


@pytest.mark.asyncio
async def test_tenant_resolved_once_so_field_and_key_agree() -> None:
    """The tenant is resolved once per call: the stored ``tenant_id`` and the ``_id`` tag
    come from the same resolution, even against a provider whose answer changes."""

    seq = count(1)
    client = _client()
    client.find_one_and_update = AsyncMock(
        side_effect=lambda _c, _f, u, **_kw: dict(u["$setOnInsert"]),
    )
    store = MongoIdempotencyStore(
        client=client,
        spec=IdempotencySpec(name="idem"),
        config=MongoIdempotencyConfig(collection=("app", "idem"), tenant_aware=True),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=UUID(int=next(seq))),
    )

    assert await store.begin(OP, "k", HASH_A) is None

    flt, update = client.find_one_and_update.await_args.args[1:3]
    tag = flt["_id"].split("|")[0].removeprefix("tenant:")
    assert update["$setOnInsert"]["tenant_id"] == tag


@pytest.mark.asyncio
async def test_a_live_claim_for_another_payload_is_refused() -> None:
    """The key is bound to its first payload: a second one is refused rather than served
    the first's result."""

    client = _client()
    client.find_one_and_update = AsyncMock(
        return_value={
            "claim_token": "someone-else",
            "payload_hash": "other-hash",
            "status": "done",
            "result": b"x",
            # Live, not expired: an expired document takes the reclaim path instead, which
            # is decided before the payload hash is ever looked at.
            "expires_at": utcnow() + timedelta(hours=1),
        },
    )
    store = _store(client)

    with pytest.raises(CoreException) as ei:
        await store.begin(OP, "k", HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT
    assert "hash" in ei.value.summary.lower()

"""Integration tests for the Mongo co-located idempotency store.

The shared battery (``test_mongo_idempotency_conformance``) covers the promises every store
makes. This file covers what makes *this* store co-located: ``commit`` riding the caller's
session so the record and the business writes are atomic, and ``begin`` / ``fail`` running
detached so a claim outlives the transaction it is guarding.

# covers: IdempotencyPort.begin
# covers: IdempotencyPort.commit
# covers: IdempotencyPort.fail
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.idempotency import IdempotencyRecord, IdempotencySpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_mongo.adapters.idempotency import MongoIdempotencyStore
from forze_mongo.execution.deps.configs import MongoIdempotencyConfig
from forze_mongo.kernel.client import MongoClient

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

OP = "place_order"
HASH_A = "hash-aaaa"
RESULT = b'{"order":"o-1"}'

_TENANT_A = UUID("00000000-0000-0000-0000-00000000000a")
_TENANT_B = UUID("00000000-0000-0000-0000-00000000000b")


async def _store(
    client: MongoClient,
    *,
    ttl: timedelta = timedelta(hours=1),
    tenant: UUID | None = None,
    coll_name: str | None = None,
    owner: UUID | None = None,
) -> MongoIdempotencyStore:
    db_name = (await client.db()).name
    tenant_aware = tenant is not None

    return MongoIdempotencyStore(
        client=client,
        spec=IdempotencySpec(name="idem", ttl=ttl),
        config=MongoIdempotencyConfig(
            collection=(db_name, coll_name or f"idem_{uuid4().hex[:8]}"),
            tenant_aware=tenant_aware,
        ),
        tenant_aware=tenant_aware,
        tenant_provider=(lambda: TenantIdentity(tenant_id=tenant)) if tenant else (lambda: None),
        owner_provider=lambda: owner,
    )


# ....................... #


async def test_commit_rolls_back_with_the_business_transaction(
    mongo_client_replica: MongoClient,
) -> None:
    """The record is written on the caller's session: a rollback takes it with the effect,
    so the duplicate re-executes rather than replaying a result that never happened."""

    store = await _store(mongo_client_replica)
    key = f"k-{uuid4().hex[:8]}"

    assert await store.begin(OP, key, HASH_A) is None

    with pytest.raises(RuntimeError, match="rollback"):
        async with mongo_client_replica.transaction():
            await store.commit(OP, key, HASH_A, IdempotencyRecord(result=RESULT))
            raise RuntimeError("rollback")

    # The claim survives (it was taken detached), the record does not: still in progress.
    with pytest.raises(CoreException) as ei:
        await store.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT


async def test_commit_survives_a_committed_transaction(
    mongo_client_replica: MongoClient,
) -> None:
    store = await _store(mongo_client_replica)
    key = f"k-{uuid4().hex[:8]}"

    assert await store.begin(OP, key, HASH_A) is None

    async with mongo_client_replica.transaction():
        await store.commit(OP, key, HASH_A, IdempotencyRecord(result=RESULT))

    replayed = await store.begin(OP, key, HASH_A)

    assert replayed is not None
    assert replayed.result == RESULT


async def test_claim_survives_a_rolled_back_transaction(
    mongo_client_replica: MongoClient,
) -> None:
    """``begin`` is detached: the claim guards the operation even when the transaction it
    was taken inside rolls back, so a duplicate cannot slip in behind the rollback."""

    store = await _store(mongo_client_replica)
    key = f"k-{uuid4().hex[:8]}"

    with pytest.raises(RuntimeError, match="rollback"):
        async with mongo_client_replica.transaction():
            assert await store.begin(OP, key, HASH_A) is None
            raise RuntimeError("rollback")

    with pytest.raises(CoreException) as ei:
        await store.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT


async def test_release_survives_a_rolled_back_transaction(
    mongo_client_replica: MongoClient,
) -> None:
    """``fail`` is detached too: the release is not undone by the failure that caused it,
    which is the whole point of releasing a claim on the error path."""

    store = await _store(mongo_client_replica)
    key = f"k-{uuid4().hex[:8]}"

    assert await store.begin(OP, key, HASH_A) is None

    with pytest.raises(RuntimeError, match="rollback"):
        async with mongo_client_replica.transaction():
            await store.fail(OP, key, HASH_A)
            raise RuntimeError("rollback")

    assert await store.begin(OP, key, HASH_A) is None


# ....................... #


async def test_an_expired_claim_is_reclaimable(mongo_client: MongoClient) -> None:
    """A claim whose TTL lapsed is taken over in place — an operation that crashed without
    releasing does not block its key forever."""

    store = await _store(mongo_client, ttl=timedelta(milliseconds=1))
    key = f"k-{uuid4().hex[:8]}"

    assert await store.begin(OP, key, HASH_A) is None
    await asyncio.sleep(0.05)

    assert await store.begin(OP, key, HASH_A) is None


async def test_an_expired_record_re_executes_instead_of_replaying(
    mongo_client: MongoClient,
) -> None:
    """The dedup window is a window: past the TTL the record is gone, by design."""

    store = await _store(mongo_client, ttl=timedelta(milliseconds=1))
    key = f"k-{uuid4().hex[:8]}"

    assert await store.begin(OP, key, HASH_A) is None
    await store.commit(OP, key, HASH_A, IdempotencyRecord(result=RESULT))
    await asyncio.sleep(0.05)

    assert await store.begin(OP, key, HASH_A) is None


async def test_a_reclaim_clears_the_previous_result(mongo_client: MongoClient) -> None:
    """Taking over an expired document drops its result, so the bytes of a completed
    operation are not readable past the expiry that retired them.

    Asserted against the stored document: every path that reads a reclaimed document
    refuses it for being pending, which would hide a result left lying in it.
    """

    coll_name = f"idem_{uuid4().hex[:8]}"
    aging = await _store(mongo_client, ttl=timedelta(milliseconds=1), coll_name=coll_name)
    store = await _store(mongo_client, coll_name=coll_name)
    key = f"k-{uuid4().hex[:8]}"

    await aging.begin(OP, key, HASH_A)
    await aging.commit(OP, key, HASH_A, IdempotencyRecord(result=RESULT))
    await asyncio.sleep(0.05)

    assert await store.begin(OP, key, HASH_A) is None  # reclaims the expired record

    doc = await mongo_client.find_one(
        await mongo_client.collection(coll_name),
        {"_id": store._doc_id(OP, key, None)},
    )

    assert doc is not None
    assert doc["result"] is None
    assert doc["status"] == "pending"


async def test_a_document_without_an_expiry_is_reclaimable(mongo_client: MongoClient) -> None:
    """A document this store did not write has no ``expires_at``, and is taken over rather
    than blocking its key.

    Asserted against a real collection because the bug this pins lives in the driver's own
    semantics: a Mongo range query does not match a *missing* field, so a reclaim filtered
    only on ``expires_at <= now`` refused the very documents the expiry judgement had just
    cleared for takeover — and refused them permanently. A mocked client cannot see it.
    """

    coll_name = f"idem_{uuid4().hex[:8]}"
    store = await _store(mongo_client, coll_name=coll_name)
    key = f"k-{uuid4().hex[:8]}"

    coll = await mongo_client.collection(coll_name)
    await mongo_client.insert_one(
        coll,
        {
            "_id": store._doc_id(OP, key, None),
            "op": OP,
            "idem_key": key,
            "payload_hash": HASH_A,
            "status": "pending",
        },
    )

    assert await store.begin(OP, key, HASH_A) is None

    # And the takeover is a real claim, not a pass-through: the next caller is held off.
    with pytest.raises(CoreException) as ei:
        await store.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT


async def test_only_one_racer_reclaims_an_expired_claim(mongo_client: MongoClient) -> None:
    """Duplicates finding the same expired claim: one takes it, the rest are refused — an
    expired claim must not become permission for everyone to run at once.

    The claim is aged by a short-TTL store and reclaimed through a normal-TTL one, so the
    winner's own claim is live for the losers to collide with; racing two short-TTL stores
    would only prove that a 1ms claim expires.
    """

    coll_name = f"idem_{uuid4().hex[:8]}"
    aging = await _store(mongo_client, ttl=timedelta(milliseconds=1), coll_name=coll_name)
    store = await _store(mongo_client, coll_name=coll_name)
    key = f"k-{uuid4().hex[:8]}"

    assert await aging.begin(OP, key, HASH_A) is None
    await asyncio.sleep(0.05)

    outcomes = await asyncio.gather(
        *(store.begin(OP, key, HASH_A) for _ in range(8)),
        return_exceptions=True,
    )

    claimed = [o for o in outcomes if o is None]
    refused = [o for o in outcomes if isinstance(o, CoreException)]

    assert len(claimed) == 1, outcomes
    assert len(refused) == 7, outcomes
    assert all(e.kind == ExceptionKind.CONFLICT for e in refused)


async def test_concurrent_fresh_claims_have_exactly_one_winner(
    mongo_client: MongoClient,
) -> None:
    store = await _store(mongo_client)
    key = f"k-{uuid4().hex[:8]}"

    outcomes = await asyncio.gather(
        *(store.begin(OP, key, HASH_A) for _ in range(8)),
        return_exceptions=True,
    )

    assert len([o for o in outcomes if o is None]) == 1, outcomes
    assert all(
        isinstance(o, CoreException) and o.kind == ExceptionKind.CONFLICT
        for o in outcomes
        if o is not None
    )


# ....................... #


async def test_claims_are_scoped_per_tenant(mongo_client: MongoClient) -> None:
    """Two tenants sharing one collection never replay or block each other's operations."""

    coll_name = f"idem_{uuid4().hex[:8]}"
    store_a = await _store(mongo_client, tenant=_TENANT_A, coll_name=coll_name)
    store_b = await _store(mongo_client, tenant=_TENANT_B, coll_name=coll_name)
    key = f"k-{uuid4().hex[:8]}"

    assert await store_a.begin(OP, key, HASH_A) is None
    assert await store_b.begin(OP, key, HASH_A) is None

    await store_a.commit(OP, key, HASH_A, IdempotencyRecord(result=RESULT))

    # A's record does not answer for B, whose own operation is still in flight.
    with pytest.raises(CoreException) as ei:
        await store_b.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT


async def test_operations_do_not_share_a_key(mongo_client: MongoClient) -> None:
    """The same idempotency key under two operations is two independent claims."""

    store = await _store(mongo_client)
    key = f"k-{uuid4().hex[:8]}"

    assert await store.begin(OP, key, HASH_A) is None
    assert await store.begin("cancel_order", key, HASH_A) is None


async def test_one_invocation_claiming_twice_is_still_refused(
    mongo_client: MongoClient,
) -> None:
    """The claim token and the owner answer different questions, and must stay separate.

    ``begin`` tells its own fresh insert from a live document it merely read back by the
    per-call ``claim_token``. The owner cannot do that job: it is deliberately *stable*
    across one invocation's calls, so folding the two together would make an invocation's
    second ``begin`` match its own earlier claim and read as a fresh insert — two callers
    believing they hold the key, which is what the whole plane exists to prevent.
    """

    owner = uuid4()
    store = await _store(mongo_client, owner=owner)
    key = f"k-{uuid4().hex[:8]}"

    assert await store.begin(OP, key, HASH_A) is None

    with pytest.raises(CoreException) as ei:
        await store.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT


async def test_a_document_without_an_owner_is_committable(mongo_client: MongoClient) -> None:
    """A claim written before the ``owner`` field existed stays its caller's to complete.

    The document has no ``owner`` key at all, and a Mongo equality filter on ``None``
    matches a *missing* field as well as a null one — which is the whole reason the fence
    can be added without a migration. An ``$exists``-style predicate would refuse these
    documents forever and take live requests down through a rolling deploy.
    """

    coll_name = f"idem_{uuid4().hex[:8]}"
    store = await _store(mongo_client, coll_name=coll_name, owner=uuid4())
    key = f"k-{uuid4().hex[:8]}"

    db_name = (await mongo_client.db()).name
    coll = await mongo_client.collection(coll_name, db_name=db_name)

    await coll.insert_one(
        {
            "_id": f"{len(OP)}:{OP}|{key}",
            "op": OP,
            "idem_key": key,
            "payload_hash": HASH_A,
            "tenant_id": None,
            "status": "pending",
            "result": None,
            "expires_at": utcnow() + timedelta(hours=1),
            "claim_token": uuid4().hex,
        }
    )

    await store.commit(OP, key, HASH_A, IdempotencyRecord(result=RESULT))

    replayed = await store.begin(OP, key, HASH_A)

    assert replayed is not None
    assert replayed.result == RESULT


async def test_a_live_claim_of_another_invocation_is_still_a_conflict(
    mongo_client: MongoClient,
) -> None:
    """A duplicate arriving while the first operation runs is refused, owner or not.

    The other half of keeping the token and the owner apart: ``begin`` decides "is this
    document mine, or one I read back" from the per-call token, never from the owner. A
    second invocation reads back a live claim whose token *and* owner differ from its own,
    and both facts must land it in the same place — in progress.
    """

    coll_name = f"idem_{uuid4().hex[:8]}"
    first = await _store(mongo_client, coll_name=coll_name, owner=uuid4())
    second = await _store(mongo_client, coll_name=coll_name, owner=uuid4())
    key = f"k-{uuid4().hex[:8]}"

    assert await first.begin(OP, key, HASH_A) is None

    with pytest.raises(CoreException) as ei:
        await second.begin(OP, key, HASH_A)

    assert ei.value.kind == ExceptionKind.CONFLICT

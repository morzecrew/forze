"""What the Mongo durable-run store has to get right on its own, against a real server.

The shared differential (`test_mongo_durable_conformance`) pins the semantics every engine
owes the port. This file covers the mechanisms Mongo had to invent because Postgres gets
them from SQL: a batch claim with no row locks, and convergence that leans on an index the
application creates rather than on a table constraint.

Both are *concurrency* properties, so both are driven concurrently. A sequential test cannot
see either: the second scanner finds no candidate to steal, and the second enqueue finds the
first already written.

# covers: DurableRunStorePort.enqueue
# covers: DurableRunStorePort.claim_abandoned
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
    is_encrypted_payload,
)
from forze.application.contracts.durable.function import DurableScheduleRecord
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_mock import MockKeyManagement
from forze_mongo.adapters.durable import MongoDurableRunStore, MongoDurableScheduleStore
from forze_mongo.execution.deps.configs import (
    MongoDurableRunConfig,
    MongoDurableScheduleConfig,
)
from forze_mongo.kernel.client import MongoClient

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_TENANT_A = UUID("00000000-0000-0000-0000-0000000000aa")
_TENANT_B = UUID("00000000-0000-0000-0000-0000000000bb")


@pytest_asyncio.fixture
async def run_collection(mongo_client: MongoClient) -> tuple[str, str]:
    db_name = (await mongo_client.db()).name
    name = f"durable_run_{uuid4().hex[:8]}"
    coll = await mongo_client.collection(name, db_name=db_name)
    await coll.create_index(
        [("idempotency_key", 1)],
        unique=True,
        partialFilterExpression={"idempotency_key": {"$type": "string"}},
    )

    return db_name, name


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _store(
    client: MongoClient,
    collection: tuple[str, str],
    *,
    tenant: UUID | None = None,
) -> MongoDurableRunStore:
    return MongoDurableRunStore(
        client=client,
        config=MongoDurableRunConfig(collection=collection),
        tenant_provider=(lambda: TenantIdentity(tenant_id=tenant)) if tenant else (lambda: None),
    )


# ....................... #


class _GatedClient:
    """A real client that holds ``update_many`` open until a gate is released.

    The only way to pin the batch claim's exclusivity: two scanners have to have *both* read
    their candidates before either writes, and `asyncio.gather` does not reliably produce
    that ordering against a local server — the first scanner usually finishes outright, and
    the test then passes with the mechanism it exists to check never exercised.
    """

    def __init__(self, inner: MongoClient, *, arrived: asyncio.Event, gate: asyncio.Event):
        self._inner = inner
        self._arrived = arrived
        self._gate = gate

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def update_many(self, *args: Any, **kwargs: Any) -> int:
        self._arrived.set()
        await self._gate.wait()

        return await self._inner.update_many(*args, **kwargs)


async def test_a_stale_candidate_read_claims_nothing(
    mongo_client: MongoClient, run_collection: tuple[str, str]
) -> None:
    """The stand-in for ``FOR UPDATE SKIP LOCKED``, on the schedule that needs it.

    Mongo's candidate read holds no lock, so two scanners routinely select the same runs.
    What keeps them apart is the claimable predicate being re-evaluated by the server inside
    the batch update: the slow scanner below reads its candidates, another scanner claims
    every one of them, and only then does the slow write land. It must claim nothing.

    Without that re-check it claims all of them — bumping ``attempts`` past the fence the
    other scanner is holding, so a worker keeps executing a run that is no longer its own.
    Both halves are asserted, because the theft is silent from the thief's side.
    """

    store = _store(mongo_client, run_collection)
    await asyncio.gather(*(store.enqueue(f"fn{i}", input_json=None) for i in range(3)))

    arrived = asyncio.Event()
    gate = asyncio.Event()
    slow = _store(
        cast(MongoClient, _GatedClient(mongo_client, arrived=arrived, gate=gate)),
        run_collection,
    )

    pending = asyncio.create_task(slow.claim_abandoned(limit=8, lease_for=timedelta(minutes=5)))
    await asyncio.wait_for(arrived.wait(), timeout=10)

    # The slow scanner has its candidates and has not written yet. Everything it selected is
    # claimed out from under it here.
    winner = await store.claim_abandoned(limit=8, lease_for=timedelta(minutes=5))

    assert len(winner) == 3

    gate.set()
    loser = await asyncio.wait_for(pending, timeout=10)

    assert [record.run_id for record in loser] == []

    for record in winner:
        renewal = await store.renew(
            record.run_id, lease_for=timedelta(minutes=5), fence=record.attempts
        )

        assert renewal.held, f"{record.run_id} was reclaimed out from under its holder"


async def test_two_scanners_never_claim_the_same_run(
    mongo_client: MongoClient, run_collection: tuple[str, str]
) -> None:
    """The same rule under ordinary concurrency rather than a forced schedule."""

    store = _store(mongo_client, run_collection)
    enqueued = [await store.enqueue(f"fn{i}", input_json={"i": i}) for i in range(8)]

    first, second = await asyncio.gather(
        store.claim_abandoned(limit=8, lease_for=timedelta(minutes=5)),
        store.claim_abandoned(limit=8, lease_for=timedelta(minutes=5)),
    )

    claimed = [*first, *second]
    ids = [record.run_id for record in claimed]

    assert sorted(ids) == sorted(set(ids)), "a run was handed to two scanners"
    assert set(ids) <= {record.run_id for record in enqueued}

    # Every claim handed out is still live at the fence it carries — the property a stolen
    # claim breaks even when the run ids happen not to overlap.
    for record in claimed:
        renewal = await store.renew(
            record.run_id, lease_for=timedelta(minutes=5), fence=record.attempts
        )

        assert renewal.held, f"{record.run_id} was reclaimed out from under its holder"


async def test_concurrent_enqueues_of_one_key_converge(
    mongo_client: MongoClient, run_collection: tuple[str, str]
) -> None:
    """The partial unique index is load-bearing, not decorative.

    The upsert filter converges the sequential case on its own; two writers reaching the
    upsert together do not, and the index is what turns the loser's insert into a conflict
    instead of a second run under one key.
    """

    store = _store(mongo_client, run_collection)

    results = await asyncio.gather(
        *(store.enqueue("fn", input_json={"n": i}, idempotency_key="same") for i in range(6)),
        return_exceptions=True,
    )

    runs = [r for r in results if not isinstance(r, BaseException)]

    assert runs, f"every concurrent enqueue failed: {results}"
    assert len({run.run_id for run in runs}) == 1, "one idempotency key produced two runs"

    # Whoever lost the race still sees the winner's input, never its own.
    winner = await store.load(runs[0].run_id)

    assert winner is not None
    assert all(run.input_json == winner.input_json for run in runs)


async def test_two_tenants_may_reuse_one_idempotency_key(
    mongo_client: MongoClient, run_collection: tuple[str, str]
) -> None:
    """A shared tagged collection has one index over every tenant's keys.

    So the stored key is tenant-scoped: a scheduler's ``{schedule_id}:{fire_epoch}`` is the
    same string for every tenant, and without the prefix the second tenant's fire would
    converge onto the first tenant's run — one tenant's schedule silently firing for another.
    """

    a = _store(mongo_client, run_collection, tenant=_TENANT_A)
    b = _store(mongo_client, run_collection, tenant=_TENANT_B)

    run_a = await a.enqueue("fn", input_json={"t": "a"}, idempotency_key="nightly:1")
    run_b = await b.enqueue("fn", input_json={"t": "b"}, idempotency_key="nightly:1")

    assert run_a.run_id != run_b.run_id
    assert (run_a.tenant_id, run_b.tenant_id) == (_TENANT_A, _TENANT_B)
    # The record surfaces the key the caller passed, not the scoped form it is stored under.
    assert (run_a.idempotency_key, run_b.idempotency_key) == ("nightly:1", "nightly:1")

    # Re-submitting under one tenant converges on that tenant's run, not the other's.
    again = await a.enqueue("fn", input_json={"t": "a2"}, idempotency_key="nightly:1")

    assert again.run_id == run_a.run_id
    assert again.input_json == {"t": "a"}


async def test_a_bound_tenant_claims_and_lists_only_its_own_runs(
    mongo_client: MongoClient, run_collection: tuple[str, str]
) -> None:
    """Recovery unbound sweeps every tenant; bound, it stays inside one."""

    a = _store(mongo_client, run_collection, tenant=_TENANT_A)
    b = _store(mongo_client, run_collection, tenant=_TENANT_B)
    unbound = _store(mongo_client, run_collection)

    await a.enqueue("a-run", input_json=None)
    await b.enqueue("b-run", input_json=None)

    listed = await a.list_runs(limit=10)

    assert [record.name for record in listed.records] == ["a-run"]

    claimed_by_a = await a.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))

    assert [record.name for record in claimed_by_a] == ["a-run"]

    # B's run was never A's to take, and the unbound scan — the deployment shape the runner
    # uses to recover a tagged collection — still reaches it.
    claimed_unbound = await unbound.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))

    assert [record.name for record in claimed_unbound] == ["b-run"]


async def test_run_payloads_are_sealed_at_rest_and_load_in_the_clear(
    mongo_client: MongoClient, run_collection: tuple[str, str]
) -> None:
    """Input and output are the run's business payload, so a wired keyring seals both.

    Asserted against the stored document, not just the round trip: a store that returned
    plaintext because it never encrypted would pass a round-trip check perfectly.
    """

    store = MongoDurableRunStore(
        client=mongo_client,
        config=MongoDurableRunConfig(collection=run_collection),
        cipher=_keyring(),
        tenant_provider=lambda: TenantIdentity(tenant_id=_TENANT_A),
    )

    run = await store.enqueue("fn", input_json={"card": "4111"}, idempotency_key="sealed")
    claimed = await store.begin(run.run_id, lease_for=timedelta(minutes=5))

    assert claimed is not None
    assert claimed.input_json == {"card": "4111"}

    await store.complete(run.run_id, output_json={"receipt": "r-1"}, fence=claimed.attempts)

    loaded = await store.load(run.run_id)

    assert loaded is not None
    assert (loaded.input_json, loaded.output_json) == ({"card": "4111"}, {"receipt": "r-1"})

    db_name, coll_name = run_collection
    coll = await mongo_client.collection(coll_name, db_name=db_name)
    stored = await mongo_client.find_one(coll, {"_id": run.run_id})

    assert stored is not None
    assert is_encrypted_payload(stored["input"])
    assert is_encrypted_payload(stored["output"])
    assert "4111" not in str(stored) and "r-1" not in str(stored)


async def test_a_tenant_aware_store_cannot_run_the_unbound_recovery_sweep(
    mongo_client: MongoClient, run_collection: tuple[str, str]
) -> None:
    """Why the run config's ``tenant_aware`` should stay off for a shared collection.

    ``tenant_aware`` fails closed when no tenant is bound — correct for a store that must
    never read across tenants, and wrong for this one, whose recovery sweep is *defined* as
    running unbound over a tagged collection so a crashed run of any tenant is picked up.
    Asserted rather than left in a docstring, since the failure only appears in the
    deployment shape (multi-tenant, recovering) that is hardest to exercise by hand.
    """

    store = MongoDurableRunStore(
        client=mongo_client,
        config=MongoDurableRunConfig(collection=run_collection, tenant_aware=True),
        tenant_aware=True,
        tenant_provider=lambda: None,
    )

    with pytest.raises(CoreException) as ei:
        await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))

    assert ei.value.kind == ExceptionKind.AUTHENTICATION

    # The same store bound to a tenant works, which is what makes this a wiring choice
    # rather than a broken configuration.
    bound = MongoDurableRunStore(
        client=mongo_client,
        config=MongoDurableRunConfig(collection=run_collection, tenant_aware=True),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=_TENANT_A),
    )

    assert await bound.claim_abandoned(limit=10, lease_for=timedelta(minutes=5)) == []


async def test_a_bound_scheduler_claims_only_its_own_due_schedules(
    mongo_client: MongoClient,
) -> None:
    """The schedule store's tenant scoping, which the shared scenario runs unbound.

    A schedule id is a caller's own string — ``nightly`` for every tenant that registers
    one — so both halves matter: two tenants keep distinct schedules under one id, and a
    bound scheduler's claim never reaches across to fire someone else's.
    """

    db_name = (await mongo_client.db()).name
    collection = (db_name, f"durable_schedule_{uuid4().hex[:8]}")
    due_at = utcnow() - timedelta(minutes=1)

    def store(tenant: UUID) -> MongoDurableScheduleStore:
        return MongoDurableScheduleStore(
            client=mongo_client,
            config=MongoDurableScheduleConfig(collection=collection),
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    for tenant, name in ((_TENANT_A, "a-fn"), (_TENANT_B, "b-fn")):
        await store(tenant).put(
            DurableScheduleRecord(
                schedule_id="nightly",
                name=name,
                cron="0 3 * * *",
                next_fire_at=due_at,
            )
        )

    a_due = await store(_TENANT_A).claim_due(now=utcnow(), limit=10)
    b_due = await store(_TENANT_B).claim_due(now=utcnow(), limit=10)

    assert [record.name for record in a_due] == ["a-fn"]
    assert [record.name for record in b_due] == ["b-fn"]
    # Both surface the id the caller registered, not the scoped form it is stored under.
    assert {record.schedule_id for record in (*a_due, *b_due)} == {"nightly"}

    # A's advance moves A's schedule only, so B still fires on its own cadence.
    assert await store(_TENANT_A).advance(
        "nightly", from_fire_at=due_at, to_fire_at=due_at + timedelta(hours=1)
    )
    assert [r.name for r in await store(_TENANT_B).claim_due(now=utcnow(), limit=10)] == ["b-fn"]

    # And deleting A's leaves B's registered.
    assert await store(_TENANT_A).delete("nightly")
    assert await store(_TENANT_B).load("nightly") is not None

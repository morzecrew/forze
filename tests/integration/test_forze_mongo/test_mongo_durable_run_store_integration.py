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

from forze.application.contracts.tenancy import TenantIdentity
from forze_mongo.adapters.durable import MongoDurableRunStore
from forze_mongo.execution.deps.configs import MongoDurableRunConfig
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

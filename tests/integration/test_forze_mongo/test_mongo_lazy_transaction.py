"""Integration tests for ``lazy_transaction``: defer session + startTransaction.

With ``lazy_transaction`` enabled, opening a transaction scope acquires no server
session and starts no transaction until the first operation runs — so pre-op
compute does not count against MongoDB's ``transactionLifetimeLimitSeconds``.

Requires a replica set (``mongo_replica_container``): MongoDB transactions only
work on replica set members. Once materialized the scope must behave like one
transaction — writes commit on clean exit and abort on error or cancellation,
nested scopes reuse it — all while never starting a transaction for a scope that
runs no operation.
"""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.client.value_objects import MongoConfig

# ----------------------- #


@pytest_asyncio.fixture(scope="function")
async def lazy_mongo_replica(mongo_replica_container) -> MongoClient:
    """Lazy Mongo client against the replica set (transactions enabled)."""

    uri = "mongodb://localhost:27017/?replicaSet=rs0"
    db_name = f"forze_lazy_{uuid4().hex[:8]}"

    client = MongoClient()
    await client.initialize(
        uri, db_name=db_name, config=MongoConfig(lazy_transaction=True)
    )
    yield client
    await client.close()


def _bound_session(client: MongoClient):
    """The effective session for the current context (white-box), or ``None``.

    Reads through the same accessor the client uses, so it reflects a materialized
    lazy scope (whose session lives on the pending object, not ``__ctx_session``).
    """

    return client._MongoClient__current_session()  # type: ignore[attr-defined]


def _pending(client: MongoClient):
    """The pending lazy-transaction state in the current context (white-box)."""

    return client._MongoClient__ctx_pending.get()  # type: ignore[attr-defined]


# ....................... #
# The headline: a scope with no operation starts no session / transaction.


@pytest.mark.asyncio
async def test_lazy_scope_defers_session_until_first_op(
    lazy_mongo_replica: MongoClient,
) -> None:
    """No session is bound until the first operation; it materializes on use and
    the write commits on clean exit."""

    client = lazy_mongo_replica
    coll = await client.collection(f"lazy_{uuid4().hex[:8]}")

    async with client.transaction():
        # Logically in a transaction, but nothing acquired or started yet.
        assert client.is_in_transaction() is True
        assert _bound_session(client) is None
        assert _pending(client) is not None

        await client.insert_one(coll, {"value": 1})

        # The first operation materialized the session + transaction.
        assert _bound_session(client) is not None

    # No session leaks past the scope, and the write committed.
    assert _bound_session(client) is None
    doc = await client.find_one(coll, {"value": 1})
    assert doc is not None and doc["value"] == 1


@pytest.mark.asyncio
async def test_clean_exit_commits_and_error_aborts(
    lazy_mongo_replica: MongoClient,
) -> None:
    """Clean exit commits the materialized write; an error after materialization
    aborts it (the bare-aclose-commits-on-error trap)."""

    client = lazy_mongo_replica
    coll = await client.collection(f"lazy_{uuid4().hex[:8]}")

    async with client.transaction():
        await client.insert_one(coll, {"value": "kept"})

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with client.transaction():
            await client.insert_one(coll, {"value": "dropped"})
            raise Boom

    assert await client.count(coll, {"value": "kept"}) == 1
    assert await client.count(coll, {"value": "dropped"}) == 0


@pytest.mark.asyncio
async def test_empty_lazy_scope_is_a_noop(
    lazy_mongo_replica: MongoClient,
) -> None:
    """A lazy scope that runs no operation starts nothing and leaves the client
    immediately usable."""

    client = lazy_mongo_replica
    coll = await client.collection(f"lazy_{uuid4().hex[:8]}")

    async with client.transaction():
        pass  # never materialized

    await client.insert_one(coll, {"value": 1})
    assert await client.count(coll, {}) == 1


# ....................... #
# Nested scopes reuse the single transaction.


@pytest.mark.asyncio
async def test_nested_scope_reuses_transaction(
    lazy_mongo_replica: MongoClient,
) -> None:
    """A nested scope reuses the root session/transaction; both writes commit
    atomically as one transaction."""

    client = lazy_mongo_replica
    coll = await client.collection(f"lazy_{uuid4().hex[:8]}")

    async with client.transaction():
        await client.insert_one(coll, {"value": 1})
        session_outer = _bound_session(client)

        async with client.transaction():
            assert _bound_session(client) is session_outer  # same session reused
            await client.insert_one(coll, {"value": 2})

    assert await client.count(coll, {}) == 2


@pytest.mark.asyncio
async def test_nested_scope_before_first_op_materializes_lazily(
    lazy_mongo_replica: MongoClient,
) -> None:
    """A nested scope entered before any operation works: the first operation
    (in the nested level) materializes the single shared transaction."""

    client = lazy_mongo_replica
    coll = await client.collection(f"lazy_{uuid4().hex[:8]}")

    async with client.transaction():
        assert _bound_session(client) is None
        async with client.transaction():
            await client.insert_one(coll, {"value": 1})
            assert _bound_session(client) is not None

    assert await client.count(coll, {}) == 1


# ....................... #
# Cancellation.


@pytest.mark.asyncio
async def test_cancel_before_materialization_holds_nothing(
    lazy_mongo_replica: MongoClient,
) -> None:
    """Cancelling a lazy scope that never ran an operation releases nothing and
    leaves the client usable."""

    client = lazy_mongo_replica
    coll = await client.collection(f"lazy_{uuid4().hex[:8]}")
    parked = asyncio.Event()

    async def worker() -> None:
        async with client.transaction():
            parked.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(worker())
    await asyncio.wait_for(parked.wait(), timeout=5)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await client.insert_one(coll, {"value": 1})
    assert await client.count(coll, {}) == 1


@pytest.mark.asyncio
async def test_first_op_in_child_context_does_not_leak_token(
    lazy_mongo_replica: MongoClient,
) -> None:
    """Regression: the first operation may materialize the scope in a *different*
    context than the one that opened it (the resilience executor runs operations
    in a child context). The session must not be bound to a context var — its
    token could not be reset across contexts — so it rides the pending object and
    the scope commits cleanly."""

    client = lazy_mongo_replica
    coll = await client.collection(f"lazy_{uuid4().hex[:8]}")

    async with client.transaction():
        # create_task copies the context: the materializing op runs in a child
        # context while the scope exit unwinds in this (parent) context.
        await asyncio.create_task(client.insert_one(coll, {"value": 1}))
        # The parent context sees the materialized session (fall-through).
        assert _bound_session(client) is not None

    assert await client.count(coll, {"value": 1}) == 1


@pytest.mark.asyncio
async def test_cancel_after_materialization_aborts(
    lazy_mongo_replica: MongoClient,
) -> None:
    """Cancelling a materialized lazy scope aborts its write."""

    client = lazy_mongo_replica
    coll = await client.collection(f"lazy_{uuid4().hex[:8]}")
    materialized = asyncio.Event()

    async def worker() -> None:
        async with client.transaction():
            await client.insert_one(coll, {"value": 1})
            materialized.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(worker())
    await asyncio.wait_for(materialized.wait(), timeout=5)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await client.count(coll, {}) == 0

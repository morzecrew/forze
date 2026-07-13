"""Optimistic-concurrency conflicts inside Mongo transactions (replica set).

A write conflict inside a Mongo multi-document transaction aborts the whole
server transaction. Any gateway-level retry that re-runs the read-modify-write
on the same session therefore cannot succeed — the server rejects further
operations on the aborted transaction (NoSuchTransaction). The gateway must
surface the original conflict as a clean ``concurrency`` error in one attempt
and let the owner of the transaction scope re-run the whole scope.
"""

import asyncio
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.execution.deps.utils import doc_write_gw
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class OccDoc(Document):
    name: str


class OccCreate(CreateDocumentCmd):
    name: str


class OccUpdate(BaseDTO):
    name: str | None = None


def _write_types() -> DocumentWriteTypes[OccDoc, OccCreate, OccUpdate]:
    return DocumentWriteTypes(
        domain=OccDoc,
        create_cmd=OccCreate,
        update_cmd=OccUpdate,
    )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_tx_write_conflict_one_wins_loser_gets_clean_concurrency(
    mongo_client_replica: MongoClient,
) -> None:
    """Two transactions update the same document: exactly one commits; the loser
    surfaces the original write conflict as ``concurrency`` — never a
    NoSuchTransaction-derived error from retrying on the aborted session."""

    client = mongo_client_replica
    db_name = (await client.db()).name
    collection = f"occ_tx_{uuid4().hex[:8]}"
    database = await client.db()
    await database.create_collection(collection)

    ctx = context_from_deps(Deps.plain({MongoClientDepKey: client}))
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=(db_name, collection),
        history_enabled=False,
        tenant_aware=False,
    )

    created = await write.create(OccCreate(name="initial"))

    winner_wrote = asyncio.Event()
    loser_done = asyncio.Event()

    async def winner() -> None:
        async with client.transaction():
            await write.update(created.id, OccUpdate(name="winner"))
            winner_wrote.set()
            # Hold the transaction (and the document) open until the loser has
            # collided, so the conflict is guaranteed.
            await asyncio.wait_for(loser_done.wait(), timeout=60)

    async def loser() -> CoreException:
        await asyncio.wait_for(winner_wrote.wait(), timeout=60)

        try:
            async with client.transaction():
                await write.update(created.id, OccUpdate(name="loser"))

        except CoreException as e:
            return e

        finally:
            loser_done.set()

        raise AssertionError("loser transaction unexpectedly succeeded")

    _, loser_exc = await asyncio.gather(winner(), loser())

    # The loser sees the write conflict itself, mapped to a clean retryable
    # concurrency error — not the aborted-transaction (NoSuchTransaction) error
    # produced by re-running the operation on the dead session.
    assert loser_exc.kind is ExceptionKind.CONCURRENCY
    assert "Write conflict during transaction" in str(loser_exc)
    assert "Transaction aborted due to conflict" not in str(loser_exc)

    # Exactly one writer committed: the winner's change with a single rev bump.
    final = await write.read_gw.get(created.id)
    assert final.name == "winner"
    assert final.rev == created.rev + 1

    # The client is fully usable afterwards: the loser can re-run the whole
    # transaction scope and now succeeds against the committed state.
    async with client.transaction():
        retried = await write.update(created.id, OccUpdate(name="loser-retry"))

    final2 = await write.read_gw.get(created.id)
    assert final2.name == "loser-retry"
    assert final2.rev == retried[0].rev


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stale_rev_inside_tx_surfaces_concurrency_and_aborts_cleanly(
    mongo_client_replica: MongoClient,
) -> None:
    """A stale-revision conflict inside a transaction cannot be healed by an
    in-scope retry (the snapshot read re-reads the same stale state), so it must
    surface as ``concurrency`` and the transaction must abort cleanly."""

    client = mongo_client_replica
    db_name = (await client.db()).name
    collection = f"occ_stale_{uuid4().hex[:8]}"
    database = await client.db()
    await database.create_collection(collection)

    ctx = context_from_deps(Deps.plain({MongoClientDepKey: client}))
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=(db_name, collection),
        history_enabled=False,
        tenant_aware=False,
    )

    created = await write.create(OccCreate(name="initial"))

    in_tx = asyncio.Event()
    bumped = asyncio.Event()

    async def stale_updater() -> CoreException:
        try:
            async with client.transaction():
                # Pin the snapshot with a transactional read, then let a
                # competing writer commit before this transaction writes.
                await write.read_gw.get(created.id)
                in_tx.set()
                await asyncio.wait_for(bumped.wait(), timeout=60)

                await write.update(created.id, OccUpdate(name="stale"))

        except CoreException as e:
            return e

        raise AssertionError("stale update unexpectedly succeeded")

    async def committed_writer() -> None:
        await asyncio.wait_for(in_tx.wait(), timeout=60)
        await write.update(created.id, OccUpdate(name="fresh"))
        bumped.set()

    stale_exc, _ = await asyncio.gather(stale_updater(), committed_writer())

    assert stale_exc.kind is ExceptionKind.CONCURRENCY
    assert "Transaction aborted due to conflict" not in str(stale_exc)

    # The committed writer's state stands, and the client is usable afterwards.
    final = await write.read_gw.get(created.id)
    assert final.name == "fresh"
    assert final.rev == created.rev + 1

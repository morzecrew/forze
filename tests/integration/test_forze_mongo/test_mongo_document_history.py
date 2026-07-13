"""Integration tests for Mongo document revision history and OCC validation.

Covers the end-to-end write path with a history collection configured: one
snapshot per revision, and the three-way stale-revision decision (historical
snapshot vs current document vs update) — non-overlapping changes merge,
genuine conflicts reject, and the precondition ladder (future rev, missing
snapshot) surfaces the same codes as the other backends.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

pytest.importorskip("pymongo")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.constants import HISTORY_SOURCE_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps


class HistDoc(Document):
    name: str
    due: datetime


class HistCreate(CreateDocumentCmd):
    name: str
    due: datetime


class HistUpdate(BaseDTO):
    name: str | None = None
    due: datetime | None = None


class HistRead(ReadDocument):
    name: str
    due: datetime


DUE_V1 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
DUE_V2 = datetime(2027, 2, 2, 12, 0, tzinfo=timezone.utc)


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="mongo_hist_ns",
        read=HistRead,
        write={
            "domain": HistDoc,
            "create_cmd": HistCreate,
            "update_cmd": HistUpdate,
        },
        history_enabled=True,
    )


async def _ctx(
    mongo_client: MongoClient,
    *,
    collection: str,
    history_collection: str,
) -> tuple[ExecutionContext, str]:
    db_name = (await mongo_client.db()).name
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db_name, collection),
            write=(db_name, collection),
            history=(db_name, history_collection),
        )
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )

    return ctx, f"{db_name}.{collection}"


async def _history_count(
    mongo_client: MongoClient,
    history_collection: str,
    source: str,
) -> int:
    db_name = (await mongo_client.db()).name
    coll = await mongo_client.collection(history_collection, db_name=db_name)

    return int(await coll.count_documents({HISTORY_SOURCE_FIELD: source}))


@pytest.mark.asyncio
async def test_history_snapshot_written_per_revision(mongo_client: MongoClient) -> None:
    """Create and each update append one snapshot to the history collection."""
    suf = uuid4().hex[:8]
    hist = f"doc_hist_h_{suf}"
    ctx, source = await _ctx(
        mongo_client, collection=f"doc_hist_{suf}", history_collection=hist
    )
    cmd = ctx.document.command(_spec())

    doc = await cmd.create(HistCreate(name="v1", due=DUE_V1))
    assert doc.rev == 1
    assert await _history_count(mongo_client, hist, source) == 1

    updated = await cmd.update(doc.id, doc.rev, HistUpdate(name="v2"))
    assert updated.rev == 2
    assert await _history_count(mongo_client, hist, source) == 2


@pytest.mark.asyncio
async def test_stale_rev_nonconflicting_update_merges(mongo_client: MongoClient) -> None:
    """A stale-rev update touching a different field than the concurrent writer merges.

    The three-way check diffs (historical snapshot -> current) against
    (historical snapshot -> update); disjoint touch sets are no conflict, and
    the stale writer must not clobber the concurrent writer's field.
    """
    suf = uuid4().hex[:8]
    ctx, _ = await _ctx(
        mongo_client, collection=f"doc_hist_{suf}", history_collection=f"doc_hist_h_{suf}"
    )
    cmd = ctx.document.command(_spec())

    doc = await cmd.create(HistCreate(name="v1", due=DUE_V1))

    # Concurrent writer moves `due`; the document is now at rev 2.
    moved = await cmd.update(doc.id, doc.rev, HistUpdate(due=DUE_V2))
    assert moved.rev == 2

    # Stale client (still at rev 1) changes only `name`.
    updated = await cmd.update(doc.id, 1, HistUpdate(name="v2"))

    assert updated.rev == 3
    assert updated.name == "v2"
    assert updated.due == DUE_V2


@pytest.mark.asyncio
async def test_stale_rev_conflicting_update_rejected(mongo_client: MongoClient) -> None:
    """Both writers changing the same field to different values is a conflict."""
    suf = uuid4().hex[:8]
    ctx, _ = await _ctx(
        mongo_client, collection=f"doc_hist_{suf}", history_collection=f"doc_hist_h_{suf}"
    )
    cmd = ctx.document.command(_spec())

    doc = await cmd.create(HistCreate(name="v1", due=DUE_V1))
    await cmd.update(doc.id, doc.rev, HistUpdate(name="theirs"))

    with pytest.raises(CoreException) as exc_info:
        await cmd.update(doc.id, 1, HistUpdate(name="ours"))

    assert exc_info.value.code == "historical_consistency_violation"


@pytest.mark.asyncio
async def test_stale_rev_identical_datetime_resend_does_not_false_conflict(
    mongo_client: MongoClient,
) -> None:
    """Regression: OCC validation compares all inputs in python-mode space.

    A stale-rev update that echoes the identical datetime it read (no intent to
    change it) while another writer concurrently changed that field used to
    raise a false ``historical_consistency_violation`` when the historical
    snapshot was compared json-mode (ISO strings) against the python-mode
    update mapping. The echoed field wins last-write style; the genuinely
    changed field (``name``) does not overlap the concurrent change.
    """
    suf = uuid4().hex[:8]
    ctx, _ = await _ctx(
        mongo_client, collection=f"doc_hist_{suf}", history_collection=f"doc_hist_h_{suf}"
    )
    cmd = ctx.document.command(_spec())

    doc = await cmd.create(HistCreate(name="v1", due=DUE_V1))

    moved = await cmd.update(doc.id, doc.rev, HistUpdate(due=DUE_V2))
    assert moved.rev == 2

    updated = await cmd.update(doc.id, 1, HistUpdate(name="v2", due=DUE_V1))

    assert updated.rev == 3
    assert updated.name == "v2"
    # The echo is treated as intent (last write wins on echoed fields).
    assert updated.due == DUE_V1


@pytest.mark.asyncio
async def test_future_rev_rejected(mongo_client: MongoClient) -> None:
    """A presented revision ahead of the stored one can have no history to check."""
    suf = uuid4().hex[:8]
    ctx, _ = await _ctx(
        mongo_client, collection=f"doc_hist_{suf}", history_collection=f"doc_hist_h_{suf}"
    )
    cmd = ctx.document.command(_spec())

    doc = await cmd.create(HistCreate(name="v1", due=DUE_V1))

    with pytest.raises(CoreException) as exc_info:
        await cmd.update(doc.id, doc.rev + 1, HistUpdate(name="v2"))

    assert exc_info.value.code == "revision_mismatch"


@pytest.mark.asyncio
async def test_missing_history_snapshot_is_retryable_precondition(
    mongo_client: MongoClient,
) -> None:
    """A stale rev whose snapshot is gone asks the caller to retry, not conflict."""
    suf = uuid4().hex[:8]
    hist = f"doc_hist_h_{suf}"
    ctx, source = await _ctx(
        mongo_client, collection=f"doc_hist_{suf}", history_collection=hist
    )
    cmd = ctx.document.command(_spec())

    doc = await cmd.create(HistCreate(name="v1", due=DUE_V1))
    await cmd.update(doc.id, doc.rev, HistUpdate(name="theirs"))

    # Simulate trimmed/lost history: drop every snapshot for this collection.
    db_name = (await mongo_client.db()).name
    coll = await mongo_client.collection(hist, db_name=db_name)
    await coll.delete_many({HISTORY_SOURCE_FIELD: source})

    with pytest.raises(CoreException) as exc_info:
        await cmd.update(doc.id, 1, HistUpdate(name="ours"))

    assert exc_info.value.code == "history_not_found_retry"

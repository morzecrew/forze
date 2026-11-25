"""Integration: the audit collection on a real Mongo, with rows from before RFC 0079 P2.

Mongo needs no migration for `surface`, `execution_id` and `correlation_id`: a row written
before them lacks the fields. It must read back as `None` and be found by the `$null` query
the audit page gives, which the in-memory store cannot prove, since it never holds a document
missing a field its model declares.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.audit import AuditOutcome
from forze.base.primitives import utcnow
from forze_kits.integrations.audit import audit_record_spec
from forze_kits.integrations.audit.record import AuditCreate
from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.execution.deps.configs import MongoDocumentConfig
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


async def test_a_row_from_before_the_surface_reads_back_none_and_matches_null(
    mongo_client: MongoClient,
) -> None:
    trail = audit_record_spec(f"audit_{uuid4().hex[:8]}")
    db = (await mongo_client.db()).name
    name = str(trail.name)
    ctx = context_from_deps(
        MongoDepsModule(
            client=mongo_client,
            rw_documents={name: MongoDocumentConfig(read=(db, name), write=(db, name))},
        )()
    )
    command = ctx.document.command(trail)
    execution_id, correlation_id = uuid4(), uuid4()

    for surface in ("cli", "legacy"):
        await command.create(
            AuditCreate(
                action="thing.create",
                outcome=AuditOutcome.ALLOWED,
                at=utcnow(),
                surface=surface,
                execution_id=execution_id,
                correlation_id=correlation_id,
            )
        )

    # The legacy row, as one written before the fields existed: they are absent, not null.
    coll = await mongo_client.collection(name, db_name=db)
    await coll.update_one(
        {"surface": "legacy"},
        {"$unset": {"surface": "", "execution_id": "", "correlation_id": ""}},
    )

    query = ctx.document.query(trail)
    legacy = await query.find_many(filters={"$values": {"surface": {"$null": True}}})
    live = await query.find_many(filters={"$values": {"surface": "cli"}})

    assert [(r.surface, r.execution_id, r.correlation_id) for r in legacy.hits] == [
        (None, None, None)
    ]
    assert [(r.surface, r.execution_id, r.correlation_id) for r in live.hits] == [
        ("cli", execution_id, correlation_id)
    ]

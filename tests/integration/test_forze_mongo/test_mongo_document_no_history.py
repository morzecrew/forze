"""Integration tests for Mongo documents without revision history (lighter path)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import ConflictError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps.deps import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient


class PlainDoc(Document):
    label: str


class PlainCreate(CreateDocumentCmd):
    label: str


class PlainUpdate(BaseDTO):
    label: str | None = None


class PlainRead(ReadDocument):
    label: str


@pytest.mark.asyncio
async def test_mongo_document_without_history_roundtrip(
    mongo_client: MongoClient,
) -> None:
    """``history_enabled=False`` and no ``history`` in config skips history gateway."""
    collection = f"plain_{uuid4().hex[:8]}"
    db_name = mongo_client.db().name

    spec = DocumentSpec(
        name="plain_ns",
        read=PlainRead,
        write={
            "domain": PlainDoc,
            "create_cmd": PlainCreate,
            "update_cmd": PlainUpdate,
        },
    )
    configurable = ConfigurableMongoDocument(
        config={
            "read": (db_name, collection),
            "write": (db_name, collection),
        }
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    doc = await cmd.create(PlainCreate(label="first"))
    assert doc.rev == 1

    loaded = await query.get(doc.id)
    assert loaded.label == "first"

    updated = await cmd.update(doc.id, doc.rev, PlainUpdate(label="second"))
    assert updated.label == "second"
    assert updated.rev == 2

    await cmd.kill(doc.id)
    assert await query.count() == 0


@pytest.mark.asyncio
async def test_mongo_no_history_revision_conflict_still_enforced(
    mongo_client: MongoClient,
) -> None:
    """Without history, stale ``rev`` on update still fails (in-memory consistency check)."""
    collection = f"plain_rev_{uuid4().hex[:8]}"
    db_name = mongo_client.db().name

    spec = DocumentSpec(
        name="plain_rev_ns",
        read=PlainRead,
        write={
            "domain": PlainDoc,
            "create_cmd": PlainCreate,
            "update_cmd": PlainUpdate,
        },
    )
    configurable = ConfigurableMongoDocument(
        config={
            "read": (db_name, collection),
            "write": (db_name, collection),
        }
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    cmd = ctx.doc_command(spec)

    doc = await cmd.create(PlainCreate(label="v1"))
    await cmd.update(doc.id, doc.rev, PlainUpdate(label="v2"))

    with pytest.raises(ConflictError, match="Revision mismatch"):
        await cmd.update(doc.id, 1, PlainUpdate(label="bad"))

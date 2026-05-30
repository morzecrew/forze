"""Integration tests for Firestore documents without revision history."""

from __future__ import annotations

from forze.base.exceptions import CoreException
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.platform import FirestoreClient
from tests.support.execution_context import context_from_deps

class PlainDoc(Document):
    label: str

class PlainCreate(CreateDocumentCmd):
    label: str

class PlainUpdate(BaseDTO):
    label: str | None = None

class PlainRead(ReadDocument):
    label: str

def _ctx(
    client: FirestoreClient,
    collection: str,
) -> ExecutionContext:
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    return context_from_deps(Deps.plain(
            {
                FirestoreClientDepKey: client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            })
    )

@pytest.mark.asyncio
async def test_firestore_document_without_history_roundtrip(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    spec = DocumentSpec(
        name="plain_ns",
        read=PlainRead,
        write={
            "domain": PlainDoc,
            "create_cmd": PlainCreate,
            "update_cmd": PlainUpdate,
        },
    )
    ctx = _ctx(firestore_client, unique_collection)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

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
async def test_firestore_no_history_revision_conflict_still_enforced(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"plain_rev_{uuid4().hex[:8]}"
    spec = DocumentSpec(
        name="plain_rev_ns",
        read=PlainRead,
        write={
            "domain": PlainDoc,
            "create_cmd": PlainCreate,
            "update_cmd": PlainUpdate,
        },
    )
    ctx = _ctx(firestore_client, collection)
    cmd = ctx.document.command(spec)

    doc = await cmd.create(PlainCreate(label="v1"))
    await cmd.update(doc.id, doc.rev, PlainUpdate(label="v2"))

    with pytest.raises(CoreException, match="Revision mismatch"):
        await cmd.update(doc.id, 1, PlainUpdate(label="bad"))

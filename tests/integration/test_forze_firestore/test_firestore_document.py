"""Integration tests for Firestore documents with revision history."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import ConflictError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.execution.deps.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.platform import FirestoreClient


class MyDoc(Document):
    name: str


class MyCreateDoc(CreateDocumentCmd):
    name: str


class MyUpdateDoc(BaseDTO):
    name: str | None = None


class MyReadDoc(ReadDocument):
    name: str


@pytest.mark.asyncio
async def test_firestore_document_adapter_roundtrip_with_history(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"docs_{uuid4().hex[:8]}"
    history_collection = f"{collection}_history"

    spec = DocumentSpec(
        name="my_docs_ns",
        read=MyReadDoc,
        write={
            "domain": MyDoc,
            "create_cmd": MyCreateDoc,
            "update_cmd": MyUpdateDoc,
        },
        history_enabled=True,
    )

    configurable = ConfigurableFirestoreDocument(
        config={
            "read": ("(default)", collection),
            "write": ("(default)", collection),
            "history": ("(default)", history_collection),
        }
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    adapter = ctx.document.command(spec)
    query = ctx.document.query(spec)

    created = await adapter.create(MyCreateDoc(name="alpha"))
    created_2 = await adapter.create(MyCreateDoc(name="beta"))
    assert created.rev == 1

    fetched = await adapter.get(created.id)
    assert fetched.name == "alpha"

    filtered: QueryFilterExpression = {"$values": {"name": {"$eq": "alpha"}}}
    found = await query.find(filtered)
    assert found is not None
    assert found.id == created.id

    updated = await adapter.update(
        created.id,
        created.rev,
        MyUpdateDoc(name="alpha-2"),
    )
    assert updated.name == "alpha-2"
    assert updated.rev == 2

    with pytest.raises(ConflictError, match="Historical consistency violation"):
        await adapter.update(created.id, 1, MyUpdateDoc(name="alpha-3"))

    await adapter.kill(created_2.id)
    assert await query.count() == 1

    hist_coll = await firestore_client.collection(history_collection)
    history_rows = await firestore_client.query_stream(
        hist_coll,
        filters=None,
        limit=50,
    )
    assert len(history_rows) >= 2

    await adapter.kill(created.id)
    assert await query.count() == 0

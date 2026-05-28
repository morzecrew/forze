"""Integration tests: Pydantic computed fields are not persisted to Firestore."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.platform import FirestoreClient

from tests.integration._computed_field_models import (
    ComputedCreate,
    ComputedReadDoc,
    ComputedStoredDoc,
    ComputedUpdate,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firestore_document_computed_field_roundtrip_not_persisted(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"computed_{uuid4().hex[:8]}"

    spec = DocumentSpec(
        name="computed_docs",
        read=ComputedReadDoc,
        write={
            "domain": ComputedStoredDoc,
            "create_cmd": ComputedCreate,
            "update_cmd": ComputedUpdate,
        },
    )

    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
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

    created = await adapter.create(ComputedCreate(value=4))
    assert created.doubled == 8

    coll = await firestore_client.collection(collection)
    raw = await firestore_client.get_document(coll, str(created.id))
    assert raw is not None
    assert "doubled" not in raw

    fetched = await adapter.get(created.id)
    assert fetched.doubled == 8

    updated = await adapter.update(
        created.id,
        created.rev,
        ComputedUpdate(value=9),
    )
    assert updated.doubled == 18

    raw_after = await firestore_client.get_document(coll, str(created.id))
    assert raw_after is not None
    assert "doubled" not in raw_after
    assert raw_after["value"] == 9

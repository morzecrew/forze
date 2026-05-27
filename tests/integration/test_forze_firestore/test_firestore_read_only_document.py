"""Integration tests for read-only Firestore document factory."""

from __future__ import annotations

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.execution.deps.deps import (
    ConfigurableFirestoreDocument,
    ConfigurableFirestoreReadOnlyDocument,
)
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.platform import FirestoreClient


class RoDoc(Document):
    label: str


class RoCreate(CreateDocumentCmd):
    label: str


class RoUpdate(BaseDTO):
    label: str | None = None


class RoRead(ReadDocument):
    label: str


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_only_document_query_without_writes(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"ro_{unique_collection}"
    spec = DocumentSpec(
        name="ro_ns",
        read=RoRead,
        write={
            "domain": RoDoc,
            "create_cmd": RoCreate,
            "update_cmd": RoUpdate,
        },
    )
    factory = ConfigurableFirestoreReadOnlyDocument(
        config={"read": ("(default)", collection)},
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: factory,
            }
        )
    )

    writable = ConfigurableFirestoreDocument(
        config={
            "read": ("(default)", collection),
            "write": ("(default)", collection),
        },
    )
    seed_ctx = ExecutionContext(
        deps=Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: writable,
                DocumentCommandDepKey: writable,
            }
        )
    )
    seeded = await seed_ctx.document.command(spec).create(RoCreate(label="seed"))

    loaded = await ctx.document.query(spec).get(seeded.id)
    assert loaded.label == "seed"

    with pytest.raises(CoreException, match="document_command"):
        await ctx.document.command(spec).create(RoCreate(label="fail"))

"""Integration tests: lenient read fields and write-omit fields on Firestore documents.

Exercised end-to-end through the spec → factory → gateway path, so the factory wiring
of ``DocumentSpec.lenient_read_fields`` / ``write_omit_fields`` is covered too.
"""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import context_from_deps


class OmitRead(ReadDocument):
    name: str
    label: str = "n/a"


class OmitDomain(Document):
    name: str
    label: str = "n/a"  # not persisted to the collection


class OmitCreate(CreateDocumentCmd):
    name: str


class OmitUpdate(BaseDTO):
    name: str | None = None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firestore_write_omit_field_stripped(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"fs_omit_{uuid4().hex[:8]}"

    spec = DocumentSpec(
        name="omit_docs",
        read=OmitRead,
        write={
            "domain": OmitDomain,
            "create_cmd": OmitCreate,
            "update_cmd": OmitUpdate,
        },
        write_omit_fields={"label"},
    )
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    adapter = ctx.document.command(spec)

    created = await adapter.create(OmitCreate(name="Ada"))
    assert created.label == "n/a"

    # The field is genuinely absent from the stored document.
    coll = await firestore_client.collection(collection)
    raw = await firestore_client.get_document(coll, str(created.id))
    assert raw is not None
    assert "label" not in raw
    assert raw["name"] == "Ada"


class LenientRead(ReadDocument):
    name: str
    nickname: str = "anon"  # declared on the read model, not stored


class LenientDomain(Document):
    name: str


class LenientCreate(CreateDocumentCmd):
    name: str


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firestore_lenient_read_field_hydrates_default(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"fs_lenient_{uuid4().hex[:8]}"

    spec = DocumentSpec(
        name="lenient_docs",
        read=LenientRead,
        write={"domain": LenientDomain, "create_cmd": LenientCreate},
        lenient_read_fields={"nickname"},
    )
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )

    created = await ctx.document.command(spec).create(LenientCreate(name="Ada"))

    fetched = await ctx.document.query(spec).get(created.id)
    assert fetched.name == "Ada"
    assert fetched.nickname == "anon"  # from the read-model default

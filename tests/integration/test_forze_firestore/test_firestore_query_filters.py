"""Integration tests for Firestore query filters and sorts."""

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
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import context_from_deps


class FilterDoc(Document):
    sku: str


class FilterCreate(CreateDocumentCmd):
    sku: str


class FilterUpdate(BaseDTO):
    sku: str | None = None


class FilterRead(ReadDocument):
    sku: str


@pytest.mark.asyncio
async def test_find_many_with_and_filter(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"filters_{uuid4().hex[:8]}"
    spec = DocumentSpec(
        name="filters",
        read=FilterRead,
        write={
            "domain": FilterDoc,
            "create_cmd": FilterCreate,
            "update_cmd": FilterUpdate,
        },
    )
    fac = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    ctx = context_from_deps(Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            })
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(FilterCreate(sku="keep-a"))
    await cmd.create(FilterCreate(sku="keep-b"))
    await cmd.create(FilterCreate(sku="drop"))

    page = await query.find_page(
        filters={
            "$and": [
                {"$values": {"sku": {"$in": ["keep-a", "keep-b"]}}},
            ]
        },
        sorts={"sku": "asc"},
        pagination={"limit": 10, "offset": 0},
    )
    assert page.count == 2
    assert [r.sku for r in page.hits] == ["keep-a", "keep-b"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_find_many_with_or_and_neq(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"filters_or_{uuid4().hex[:8]}"
    spec = DocumentSpec(
        name="filters",
        read=FilterRead,
        write={
            "domain": FilterDoc,
            "create_cmd": FilterCreate,
            "update_cmd": FilterUpdate,
        },
    )
    fac = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            },
        ),
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(FilterCreate(sku="a"))
    await cmd.create(FilterCreate(sku="b"))
    await cmd.create(FilterCreate(sku="c"))

    page = await query.find_page(
        filters={
            "$or": [
                {"$values": {"sku": {"$eq": "a"}}},
                {"$values": {"sku": {"$eq": "b"}}},
            ],
        },
        pagination={"limit": 10, "offset": 0},
    )
    assert page.count == 2
    assert {r.sku for r in page.hits} == {"a", "b"}

    excluded = await query.find_page(
        filters={"$values": {"sku": {"$neq": "c"}}},
        pagination={"limit": 10, "offset": 0},
    )
    assert excluded.count == 2

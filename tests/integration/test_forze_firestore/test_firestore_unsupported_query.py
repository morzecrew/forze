"""Integration tests for unsupported Firestore query features."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError, InvalidOperationError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.execution.deps.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.platform import FirestoreClient


class QDoc(Document):
    tag: str


class QCreate(CreateDocumentCmd):
    tag: str


class QUpdate(BaseDTO):
    tag: str | None = None


class QRead(ReadDocument):
    tag: str


def _ctx(client: FirestoreClient, collection: str) -> ExecutionContext:
    fac = ConfigurableFirestoreDocument(
        config={"read": ("(default)", collection), "write": ("(default)", collection)}
    )
    return ExecutionContext(
        deps=Deps.plain(
            {
                FirestoreClientDepKey: client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )


@pytest.mark.asyncio
async def test_aggregate_raises(firestore_client: FirestoreClient) -> None:
    collection = f"unsupported_agg_{uuid4().hex[:8]}"
    ctx = _ctx(firestore_client, collection)
    query = ctx.document.query(
        DocumentSpec(
            name="unsupported",
            read=QRead,
            write={"domain": QDoc, "create_cmd": QCreate, "update_cmd": QUpdate},
        )
    )
    await ctx.document.command(
        DocumentSpec(
            name="unsupported",
            read=QRead,
            write={"domain": QDoc, "create_cmd": QCreate, "update_cmd": QUpdate},
        )
    ).create(QCreate(tag="x"))

    with pytest.raises(CoreError, match="aggregates"):
        await query.aggregate_page(
            aggregates={"$count": {}},
            pagination={"limit": 10},
        )


@pytest.mark.asyncio
async def test_element_quantifier_raises(firestore_client: FirestoreClient) -> None:
    collection = f"unsupported_elem_{uuid4().hex[:8]}"
    ctx = _ctx(firestore_client, collection)
    spec = DocumentSpec(
        name="unsupported",
        read=QRead,
        write={"domain": QDoc, "create_cmd": QCreate, "update_cmd": QUpdate},
    )
    await ctx.document.command(spec).create(QCreate(tag="x"))
    query = ctx.document.query(spec)

    with pytest.raises(CoreError, match="quantifiers"):
        await query.find_many(
            filters={"$values": {"tag": {"$any": "x"}}},
            pagination={"limit": 10},
        )


@pytest.mark.asyncio
async def test_offset_pagination_raises(firestore_client: FirestoreClient) -> None:
    collection = f"unsupported_offset_{uuid4().hex[:8]}"
    ctx = _ctx(firestore_client, collection)
    spec = DocumentSpec(
        name="unsupported",
        read=QRead,
        write={"domain": QDoc, "create_cmd": QCreate, "update_cmd": QUpdate},
    )
    await ctx.document.command(spec).create(QCreate(tag="x"))
    query = ctx.document.query(spec)

    with pytest.raises(InvalidOperationError, match="offset pagination"):
        await query.find_page(
            pagination={"limit": 5, "offset": 1},
        )

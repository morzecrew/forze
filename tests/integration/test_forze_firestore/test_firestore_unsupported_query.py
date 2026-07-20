"""Integration tests for unsupported Firestore query features."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import context_from_deps


class QDoc(Document):
    tag: str
    tags: list[str] = []


class QCreate(CreateDocumentCmd):
    tag: str
    tags: list[str] = []


class QUpdate(BaseDTO):
    tag: str | None = None


class QRead(ReadDocument):
    tag: str
    tags: list[str] = []


def _ctx(client: FirestoreClient, collection: str) -> ExecutionContext:
    fac = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    return context_from_deps(Deps.plain(
            {
                FirestoreClientDepKey: client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            })
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

    with pytest.raises(CoreException, match="aggregates") as exc_info:
        await query.aggregate_page(
            aggregates={"$count": {}},
            pagination={"limit": 10},
        )

    # Fail-closed on capability grounds (a clean precondition), not an opaque ``internal``.
    assert exc_info.value.kind is ExceptionKind.PRECONDITION


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

    # ``tags`` is an array (so the quantifier is type-valid and reaches the renderer);
    # Firestore rejects it on capability grounds — it compiles no element quantifiers.
    with pytest.raises(CoreException, match="element quantifier"):
        await query.find_many(
            filters={"$values": {"tags": {"$any": "x"}}},
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

    with pytest.raises(CoreException, match="offset pagination") as exc_info:
        await query.find_page(
            pagination={"limit": 5, "offset": 1},
        )

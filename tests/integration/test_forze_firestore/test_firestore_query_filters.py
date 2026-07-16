"""Integration tests for Firestore query filters and sorts."""

from decimal import Decimal
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
    price: Decimal = Decimal("0")


class FilterCreate(CreateDocumentCmd):
    sku: str
    price: Decimal = Decimal("0")


class FilterUpdate(BaseDTO):
    sku: str | None = None


class FilterRead(ReadDocument):
    sku: str
    price: Decimal = Decimal("0")


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


@pytest.mark.asyncio
async def test_decimal_write_and_filter_round_trip(
    firestore_client: FirestoreClient,
) -> None:
    """Decimal fields persist as doubles and Decimal filter values compare numerically."""

    collection = f"filters_dec_{uuid4().hex[:8]}"
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

    await cmd.create(FilterCreate(sku="cheap", price=Decimal("9.5")))
    await cmd.create(FilterCreate(sku="mid", price=Decimal("10.5")))
    await cmd.create(FilterCreate(sku="dear", price=Decimal("100.25")))

    page = await query.find_page(
        filters={"$values": {"price": {"$lt": Decimal("10.5")}}},
        pagination={"limit": 10, "offset": 0},
    )
    assert page.count == 1
    assert page.hits[0].sku == "cheap"
    assert page.hits[0].price == Decimal("9.5")

    page = await query.find_page(
        filters=None,
        sorts={"price": "asc"},
        pagination={"limit": 10, "offset": 0},
    )
    assert [r.sku for r in page.hits] == ["cheap", "mid", "dear"]


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

    # $neq is not advertised for Firestore: its != excludes absent/null fields,
    # diverging from the agnostic semantics, so the framework fails closed.
    from forze.application.contracts.querying import UNSUPPORTED_QUERY_FEATURE_CODE
    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException) as ei:
        await query.find_page(
            filters={"$values": {"sku": {"$neq": "c"}}},
            pagination={"limit": 10, "offset": 0},
        )
    assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE

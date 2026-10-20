"""Mongo finds a resent unchanged instant unchanged against the aware, millisecond BSON date it
reads back, and a stale resend of one keeps a concurrent change to it."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.document_unchanged_instant import (
    FORMS,
    InstantCreate,
    InstantDoc,
    InstantRead,
    InstantUpdate,
    assert_a_batch_writes_only_the_rows_it_changes,
    assert_a_stale_resend_keeps_a_concurrent_change,
    assert_an_unchanged_instant_is_not_a_change,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _SeriesFields(BaseModel):
    ats: tuple[datetime, ...]


class _SeriesCreate(CreateDocumentCmd, _SeriesFields):
    pass


class _SeriesDoc(Document, _SeriesFields):
    pass


class _SeriesRead(ReadDocument, _SeriesFields):
    pass


class _SeriesUpdate(BaseDTO):
    ats: tuple[datetime, ...] | None = None


_INSTANT_TYPES = (
    InstantRead,
    DocumentWriteTypes(domain=InstantDoc, create_cmd=InstantCreate, update_cmd=InstantUpdate),
)


async def _ports(
    mongo_client: MongoClient,
    *,
    history: bool = False,
    collection: str | None = None,
    types: tuple[Any, DocumentWriteTypes[Any, Any, Any]] = _INSTANT_TYPES,
) -> tuple[Any, Any]:
    """Command and query ports over *collection* (a fresh one by default); with *history*,
    revisions are kept in ``<collection>_h``. *types* are the read model and write types."""

    collection = collection or f"instant_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db_name, collection),
            write=(db_name, collection),
            history=(db_name, f"{collection}_h") if history else None,
        )
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    read, write = types
    spec = DocumentSpec(name="instant", read=read, write=write, history_enabled=history)

    return ctx.document.command(spec), ctx.document.query(spec)


async def test_an_unchanged_instant_is_not_a_change(mongo_client: MongoClient) -> None:
    await assert_an_unchanged_instant_is_not_a_change(*await _ports(mongo_client))


async def test_an_unchanged_instant_in_a_tuple_is_not_a_change(mongo_client: MongoClient) -> None:
    # BSON rounds each instant in a tuple too, so the resend must be rounded as one.
    command, query = await _ports(
        mongo_client,
        types=(
            _SeriesRead,
            DocumentWriteTypes(
                domain=_SeriesDoc, create_cmd=_SeriesCreate, update_cmd=_SeriesUpdate
            ),
        ),
    )
    ats = tuple(FORMS.values())
    doc = await command.create(_SeriesCreate(ats=ats))

    _, diff = await command.update(doc.id, doc.rev, _SeriesUpdate(ats=ats), return_diff=True)
    stored = await query.get(doc.id, skip_cache=True)

    assert diff == {}
    assert stored.rev == doc.rev


async def test_a_stale_resend_keeps_a_concurrent_change(mongo_client: MongoClient) -> None:
    await assert_a_stale_resend_keeps_a_concurrent_change(
        *await _ports(mongo_client, history=True)
    )


async def test_a_batch_writes_only_the_rows_it_changes(mongo_client: MongoClient) -> None:
    collection = f"instant_{uuid4().hex[:8]}"
    command, query = await _ports(mongo_client, history=True, collection=collection)
    history = (await mongo_client.db())[f"{collection}_h"]

    async def history_rows(pk: UUID) -> int:
        return int(await history.count_documents({ID_FIELD: str(pk)}))

    await assert_a_batch_writes_only_the_rows_it_changes(command, query, history_rows)

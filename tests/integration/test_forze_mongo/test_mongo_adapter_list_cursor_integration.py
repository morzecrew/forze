"""Integration tests for Mongo document adapter list/cursor helpers (count + keyset)."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps.deps import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient


class _ListDoc(Document):
    label: str


class _ListCreate(CreateDocumentCmd):
    label: str


class _ListUpdate(BaseDTO):
    label: str | None = None


class _ListRead(ReadDocument):
    label: str


async def _ctx_spec(
    mongo_client: MongoClient,
    collection: str,
) -> tuple[ExecutionContext, DocumentSpec]:
    db = (await mongo_client.db()).name
    spec = DocumentSpec(
        name="mongo_list_ns",
        read=_ListRead,
        write={
            "domain": _ListDoc,
            "create_cmd": _ListCreate,
            "update_cmd": _ListUpdate,
        },
    )
    fac = ConfigurableMongoDocument(
        config={"read": (db, collection), "write": (db, collection)}
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )
    return ctx, spec


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_adapter_find_many_return_count_zero_short_circuit(
    mongo_client: MongoClient,
) -> None:
    """``return_count`` with no matches returns an empty page without listing."""
    col = f"m_lc_{uuid4().hex[:8]}"
    ctx, spec = await _ctx_spec(mongo_client, col)
    q = ctx.doc_query(spec)
    page = await q.find_many(
        {"$fields": {"label": "___none___"}},
        pagination={"limit": 5},
        return_count=True,
    )
    assert page.count == 0
    assert page.hits == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_adapter_find_many_with_cursor_tokens(
    mongo_client: MongoClient,
) -> None:
    """Adapter wraps gateway keyset results and builds next/prev cursor tokens."""
    col = f"m_lcc_{uuid4().hex[:8]}"
    ctx, spec = await _ctx_spec(mongo_client, col)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    for i in range(4):
        await cmd.create(_ListCreate(label=f"L{i}"))

    p1 = await q.find_many_with_cursor(
        None,
        cursor={"limit": 2},
        sorts=None,
    )
    assert len(p1.hits) == 2
    assert p1.has_more is True
    assert p1.next_cursor is not None

    p2 = await q.find_many_with_cursor(
        None,
        cursor={"limit": 2, "after": p1.next_cursor},
        sorts=None,
    )
    assert len(p2.hits) == 2

    proj = await q.find_many_with_cursor(
        None,
        cursor={"limit": 3},
        sorts=None,
        return_fields=["id", "label"],
    )
    assert len(proj.hits) == 3
    assert set(proj.hits[0].keys()) <= {"id", "label"}

    with pytest.raises(CoreError, match="projection must include"):
        await q.find_many_with_cursor(
            None,
            cursor={"limit": 2},
            sorts=None,
            return_fields=["label"],
        )

    with pytest.raises(CoreError, match="primary key"):
        await q.find_many_with_cursor(
            None,
            cursor={"limit": 2},
            sorts={"label": "asc"},
        )

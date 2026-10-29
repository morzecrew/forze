"""Postgres matches a filter on a plain ``Enum`` field by its value, as the mock does.

The rows are seeded with raw SQL, as data written outside forze would be; the write path is
covered by the plain-Enum cases of the DSL corpus (``tests/support/query_dsl_corpus.py``). The
mock twin is ``tests/unit/test_forze_mock/test_mock_enum_filter_parity.py``.
"""

from __future__ import annotations

from enum import Enum
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.enum_filter_parity import (
    ROWS,
    StatusCreate,
    StatusDoc,
    StatusRead,
    assert_enum_cases,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enum_filters_match_on_postgres(pg_client: PostgresClient) -> None:
    t = f"enum_rows_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            status text NOT NULL,
            statuses text[] NOT NULL,
            items jsonb NOT NULL
        );
        """
    )

    # Each row holds its values as the columns store them.
    ids = {key: uuid4() for key in ROWS}

    for key, row in ROWS.items():
        await pg_client.execute(
            f"""
            INSERT INTO {t} (id, rev, created_at, last_update_at, status, statuses, items)
            VALUES (%(id)s, 1, NOW(), NOW(), %(status)s, %(statuses)s, %(items)s)
            """,
            {**row, "id": ids[key], "items": Jsonb(row["items"])},
        )

    query = document_context(pg_client, t).document.query(
        DocumentSpec(
            name="enum_rows",
            read=StatusRead,
            write=DocumentWriteTypes(domain=StatusDoc, create_cmd=StatusCreate),
        )
    )

    await assert_enum_cases(query, ids, backend="postgres")


class _Mixed(str, Enum):  # noqa: UP042 -- the case under test is not a StrEnum
    """``str()`` names the member (``_Mixed.OPEN``), unlike a ``StrEnum``."""

    OPEN = "open"
    CLOSED = "closed"


class _Unset(Enum):
    NONE = None
    SET = "set"


class _MixedFields(BaseModel):
    status: _Mixed
    flag: _Unset


class _MixedCreate(CreateDocumentCmd, _MixedFields):
    pass


class _MixedDoc(Document, _MixedFields):
    pass


class _MixedRead(ReadDocument, _MixedFields):
    pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_member_written_through_the_port_is_found_by_a_member_operand(
    pg_client: PostgresClient,
) -> None:
    """A filter naming the member binds the value the write stored; a member whose value is
    ``None`` is stored as SQL ``NULL`` in a ``jsonb`` column, so ``$null`` finds it."""

    t = f"enum_mixed_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            status text NOT NULL,
            flag jsonb
        );
        """
    )

    spec = DocumentSpec(
        name="enum_mixed",
        read=_MixedRead,
        write=DocumentWriteTypes(domain=_MixedDoc, create_cmd=_MixedCreate),
    )
    ctx = document_context(pg_client, t)
    command, query = ctx.document.command(spec), ctx.document.query(spec)

    a = await command.create(_MixedCreate(status=_Mixed.OPEN, flag=_Unset.NONE))
    b = await command.create(_MixedCreate(status=_Mixed.CLOSED, flag=_Unset.SET))

    async def ids(filters: dict[str, object]) -> set[object]:
        page = await query.find_many(filters=filters, pagination={"limit": 10})  # type: ignore[arg-type]
        return {hit.id for hit in page.hits}

    assert await ids({"$values": {"status": _Mixed.OPEN}}) == {a.id}
    assert await ids({"$values": {"status": {"$in": [_Mixed.CLOSED]}}}) == {b.id}
    assert await ids({"$values": {"flag": None}}) == {a.id}
    assert (await query.get(a.id)).flag is _Unset.NONE

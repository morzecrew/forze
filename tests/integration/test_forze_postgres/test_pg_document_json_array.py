"""Postgres writes and reads back a list of mappings held in a native ``jsonb[]`` / ``json[]``."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic_core import to_json

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    KeyedUpdate,
    UpsertItem,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Fields(BaseDTO):
    tags: list[dict[str, Any] | None] = []
    notes: list[dict[str, Any] | None] = []


class _Doc(Document, _Fields):
    pass


class _Create(CreateDocumentCmd, _Fields):
    pass


class _Read(ReadDocument, _Fields):
    pass


class _Update(BaseDTO):
    tags: list[dict[str, Any] | None] | None = None
    notes: list[dict[str, Any] | None] | None = None


SPEC = DocumentSpec(
    name="json_array",
    read=_Read,
    write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create, update_cmd=_Update),
)

TAGS = [{"k": 1, "deep": {"a": None}}, None, {"k": "two", "list": [1, None]}]
NOTES = [{"n": 1}, None, {}]


async def _table(pg_client: PostgresClient) -> str:
    t = f"pg_json_array_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            tags jsonb[] NOT NULL,
            notes json[] NOT NULL
        );
        """
    )

    return t


async def test_a_json_array_column_round_trips_on_every_write(pg_client: PostgresClient) -> None:
    t = await _table(pg_client)
    ctx = document_context(pg_client, t)
    command, query = ctx.document.command(SPEC), ctx.document.query(SPEC)

    async def stored(pk: UUID) -> tuple[Any, Any]:
        read = await query.get(pk, skip_cache=True)

        return read.tags, read.notes

    created = await command.create(_Create(tags=TAGS, notes=NOTES))
    assert (created.tags, created.notes) == (TAGS, NOTES)
    assert await stored(created.id) == (TAGS, NOTES)

    # A None element is an SQL NULL in the array, not a JSON null.
    row = await pg_client.fetch_one(
        f"SELECT tags[2] IS NULL AS tag, notes[2] IS NULL AS note FROM {t} WHERE id = %s",
        [created.id],
        row_factory="dict",
    )
    assert row == {"tag": True, "note": True}

    (batch,) = await command.create_many([_Create(tags=TAGS, notes=NOTES)])
    assert await stored(batch.id) == (TAGS, NOTES)

    updated = await command.update(created.id, created.rev, _Update(tags=[{"k": 3}], notes=[]))
    assert (updated.tags, updated.notes) == ([{"k": 3}], [])
    assert await stored(created.id) == ([{"k": 3}], [])

    (bulk,) = await command.update_many(
        [KeyedUpdate(id=batch.id, rev=batch.rev, dto=_Update(tags=[None], notes=[{"n": 4}]))]
    )
    assert await stored(bulk.id) == ([None], [{"n": 4}])

    new = uuid4()
    upsert = (new, _Create(tags=TAGS, notes=NOTES), _Update(tags=[{"k": 5}]))
    # The key is new, so this inserts the create payload ...
    await command.upsert(*upsert)
    assert await stored(new) == (TAGS, NOTES)
    # ... and the same call again finds the row, so it applies the update.
    await command.upsert(*upsert)
    assert await stored(new) == ([{"k": 5}], NOTES)

    other = uuid4()
    await command.upsert_many(
        [
            UpsertItem(id=other, create=_Create(tags=TAGS, notes=NOTES), update=_Update()),
            UpsertItem(id=new, create=_Create(), update=_Update(notes=[{"n": 6}])),
        ]
    )
    assert await stored(other) == (TAGS, NOTES)
    assert await stored(new) == ([{"k": 5}], [{"n": 6}])


async def test_an_update_reports_a_json_array_as_plain_values(pg_client: PostgresClient) -> None:
    command = document_context(pg_client, await _table(pg_client)).document.command(SPEC)
    one = await command.create(_Create(tags=TAGS, notes=NOTES))
    many = await command.create(_Create(tags=TAGS, notes=NOTES))
    update = _Update(tags=[{"k": 7}, None], notes=[{"n": 7}])

    _, diff = await command.update(one.id, one.rev, update, return_diff=True)
    ((_, bulk_diff),) = await command.update_many(
        [KeyedUpdate(id=many.id, rev=many.rev, dto=update)], return_diff=True
    )

    for reported in (diff, bulk_diff):
        assert (reported["tags"], reported["notes"]) == ([{"k": 7}, None], [{"n": 7}])
        assert all(type(x) in (dict, type(None)) for x in reported["tags"] + reported["notes"])
        to_json(reported)  # what the generated update route does with it

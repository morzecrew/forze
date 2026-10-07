"""A set-based ``upsert_many`` writes the rows the domain path writes, without reading any back.

Two identical tables take the same batches, one through the domain path and one set-based,
under the same frozen clock; every stored column must come out equal, including ``rev`` and
``last_update_at`` on the rows an update leaves unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import Field

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes, UpsertItem
from forze.base.exceptions import CoreException
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig
from tests.integration.test_forze_postgres._document_fixtures import document_context

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Fields(BaseDTO):
    name: str
    qty: int
    note: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    label: str = "dflt"
    hint: str | None = "d"
    ref: int | None


class _Create(CreateDocumentCmd, _Fields):
    pass


class _Doc(Document, _Fields):
    pass


class _Read(ReadDocument, _Fields):
    pass


class _Update(BaseDTO):
    name: str | None = None
    qty: int | None = None
    note: str | None = None
    tags: list[str] | None = None
    label: str | None = None
    hint: str | None = None
    ref: int | None = None


_SPEC = DocumentSpec(
    name="setbased",
    read=_Read,
    write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create, update_cmd=_Update),
)


async def _table(
    pg_client: PostgresClient,
    tags: str = "jsonb",
    meta: str = "jsonb",
    key: str = "id",
    name: str = "text",
) -> str:
    t = f"pg_setbased_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name {name} NOT NULL,
            qty integer NOT NULL,
            note text,
            meta {meta} NOT NULL,
            tags {tags} NOT NULL,
            label text NOT NULL,
            hint text,
            ref integer,
            PRIMARY KEY ({key})
        );
        """
    )
    return t


def _item(
    pk: UUID, name: str, qty: int, update: dict[str, Any] | None = None
) -> UpsertItem[_Create, _Update]:
    return UpsertItem(
        id=pk,
        create=_Create(
            name=name, qty=qty, note="first", meta={"k": 1}, label="custom", hint="h", ref=1
        ),
        update=_Update(**(update or {})),
    )


# ``jsonb`` tags take the ``unnest`` row source; ``text[]`` ones, which ``unnest`` would
# flatten, take ``VALUES``. A ``json`` column, which has no equality, compares as ``jsonb``.
@pytest.mark.parametrize("tags", ["jsonb", "text[]", "json"])
async def test_a_set_based_upsert_writes_what_the_domain_path_writes(
    pg_client: PostgresClient, tags: str
) -> None:
    domain_t = await _table(pg_client, tags)
    set_t = await _table(pg_client, tags)
    domain_cmd = document_context(pg_client, domain_t).document.command(_SPEC)
    set_cmd = document_context(pg_client, set_t).document.command(_SPEC)
    ids = [uuid4() for _ in range(6)]

    seed = [_item(pk, f"n{i}", i) for i, pk in enumerate(ids[:4])]

    with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 1, tzinfo=UTC))):
        await domain_cmd.upsert_many(seed, return_new=False)
        await set_cmd.upsert_many(seed, return_new=False, set_based=True)

    batch = [
        _item(ids[0], "x", 0, {"qty": 10}),  # changed
        _item(ids[1], "x", 0, {"name": "n1", "qty": 1}),  # the stored values: unchanged
        # Cleared, and reset to their defaults: a nullable one whose default is not null too.
        _item(ids[2], "x", 0, {"note": None, "label": None, "hint": None}),
        _item(ids[3], "x", 0, {"name": "renamed", "tags": ["a", "b"]}),
        _item(ids[4], "new4", 4),  # inserted
        _item(ids[5], "new5", 5, {"qty": 99}),  # inserted: the update does not apply
    ]

    with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 2, tzinfo=UTC))):
        await domain_cmd.upsert_many(batch, return_new=False)
        await set_cmd.upsert_many(batch, return_new=False, set_based=True)

    domain_rows = await pg_client.fetch_all(f"SELECT * FROM {domain_t} ORDER BY id", [])
    set_rows = await pg_client.fetch_all(f"SELECT * FROM {set_t} ORDER BY id", [])

    assert set_rows == domain_rows

    by_id = {row["id"]: row for row in set_rows}
    assert by_id[ids[1]]["rev"] == 1
    assert by_id[ids[1]]["last_update_at"] == datetime(2026, 1, 1, tzinfo=UTC)
    assert by_id[ids[0]]["rev"] == 2
    assert by_id[ids[5]]["qty"] == 5


async def test_a_row_stored_after_the_look_up_is_patched_not_lost(
    pg_client: PostgresClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A writer can store an id between the look-up and the insert; the insert then skips it,
    and the update must still reach it. Simulated by a look-up that misses a stored row."""

    t = await _table(pg_client)
    cmd = document_context(pg_client, t).document.command(_SPEC)
    pk = uuid4()
    await cmd.upsert_many([_item(pk, "stored", 1)], return_new=False)

    fetch_all = pg_client.fetch_all

    async def missing_look_up(query: Any, params: Any = None, **kwargs: Any) -> Any:
        rows = await fetch_all(query, params, **kwargs)

        text = query if isinstance(query, str) else query.as_string(None)

        if text.startswith('SELECT "id" FROM') and "FOR NO KEY UPDATE" not in text:
            return []

        return rows

    monkeypatch.setattr(pg_client, "fetch_all", missing_look_up)
    await cmd.upsert_many(
        [_item(pk, "ignored", 0, {"name": "patched"})], return_new=False, set_based=True
    )
    monkeypatch.undo()

    row = await pg_client.fetch_one(f"SELECT name, rev FROM {t} WHERE id = %s", [pk])
    assert row == {"name": "patched", "rev": 2}


@pytest.mark.parametrize("field", ["name", "ref"])
async def test_a_null_the_domain_refuses_is_refused_set_based_too(
    pg_client: PostgresClient, field: str
) -> None:
    """``name`` is not nullable; ``ref`` is, but required, so a nulled key leaves it missing."""

    domain_t, set_t = await _table(pg_client), await _table(pg_client)
    pk = uuid4()
    outcomes = []

    for t, set_based in ((domain_t, False), (set_t, True)):
        cmd = document_context(pg_client, t).document.command(_SPEC)
        await cmd.upsert_many([_item(pk, "stored", 1)], return_new=False)

        with pytest.raises(Exception) as refused:
            await cmd.upsert_many(
                [_item(pk, "x", 0, {field: None})], return_new=False, set_based=set_based
            )

        outcomes.append(type(refused.value))
        assert await pg_client.fetch_one(f"SELECT name FROM {t} WHERE id = %s", [pk]) == {
            "name": "stored"
        }

    assert outcomes[0] is outcomes[1]


async def test_a_row_deleted_before_it_is_locked_is_not_found(
    pg_client: PostgresClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A row the look-up saw but a delete removed before the lock is reported, not dropped."""

    t = await _table(pg_client)
    cmd = document_context(pg_client, t).document.command(_SPEC)
    pk = uuid4()
    await cmd.upsert_many([_item(pk, "stored", 1)], return_new=False)

    fetch_all = pg_client.fetch_all

    async def delete_after_look_up(query: Any, params: Any = None, **kwargs: Any) -> Any:
        rows = await fetch_all(query, params, **kwargs)
        text = query if isinstance(query, str) else query.as_string(None)

        if text.startswith('SELECT "id" FROM') and "FOR NO KEY UPDATE" not in text:
            await pg_client.execute(f"DELETE FROM {t} WHERE id = %s", [pk])

        return rows

    monkeypatch.setattr(pg_client, "fetch_all", delete_after_look_up)

    with pytest.raises(CoreException) as refused:
        await cmd.upsert_many(
            [_item(pk, "x", 0, {"name": "patched"})], return_new=False, set_based=True
        )

    monkeypatch.undo()
    assert refused.value.kind == "not_found"


async def test_a_relation_keyed_by_more_than_the_id_takes_the_domain_path(
    pg_client: PostgresClient,
) -> None:
    """With ``(id, name)`` as the key, a create under a new name is a new row, not a patch of
    the stored one; a set-based statement keyed by id alone would patch it, so it is not used."""

    domain_t = await _table(pg_client, key="id, name")
    set_t = await _table(pg_client, key="id, name")
    pk = uuid4()

    for t, set_based in ((domain_t, False), (set_t, True)):
        cmd = document_context(pg_client, t).document.command(_SPEC)
        await cmd.upsert_many([_item(pk, "stored", 1)], return_new=False)
        await cmd.upsert_many(
            [_item(pk, "other", 2, {"qty": 9})], return_new=False, set_based=set_based
        )

    sql = "SELECT id, name, qty, rev FROM {} ORDER BY name"
    assert await pg_client.fetch_all(sql.format(set_t), []) == await pg_client.fetch_all(
        sql.format(domain_t), []
    )


async def test_a_patched_row_is_locked_against_a_concurrent_delete(
    pg_client: PostgresClient, postgres_container: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Once the stored rows are locked, a delete from another session waits for the upsert
    rather than removing a row between the check and the patch."""

    t = await _table(pg_client)
    cmd = document_context(pg_client, t).document.command(_SPEC)
    pk = uuid4()
    await cmd.upsert_many([_item(pk, "stored", 1)], return_new=False)

    other = PostgresClient()
    await other.initialize(
        dsn=postgres_container.get_connection_url().replace(
            "postgresql+psycopg://", "postgresql://"
        ),
        config=PostgresConfig(min_size=1, max_size=1),
    )
    blocked: list[str] = []
    fetch_all = pg_client.fetch_all

    async def delete_after_lock(query: Any, params: Any = None, **kwargs: Any) -> Any:
        rows = await fetch_all(query, params, **kwargs)
        text = query if isinstance(query, str) else query.as_string(None)

        if "FOR NO KEY UPDATE" in text:
            try:
                async with other.transaction():
                    await other.execute("SET LOCAL lock_timeout = '300ms'")
                    await other.execute(f"DELETE FROM {t} WHERE id = %s", [pk])

            except CoreException as error:
                blocked.append(error.code)

        return rows

    monkeypatch.setattr(pg_client, "fetch_all", delete_after_lock)

    try:
        await cmd.upsert_many(
            [_item(pk, "x", 0, {"name": "patched"})], return_new=False, set_based=True
        )

    finally:
        monkeypatch.undo()
        await other.close()

    assert blocked, "the delete was not held off"
    assert await pg_client.fetch_one(f"SELECT name FROM {t} WHERE id = %s", [pk]) == {
        "name": "patched"
    }


async def test_a_column_whose_equality_ignores_a_change_takes_the_domain_path(
    pg_client: PostgresClient,
) -> None:
    """``citext`` calls ``Alpha`` and ``alpha`` equal, so a set-based compare would skip a
    change the domain writes."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS citext")
    domain_t = await _table(pg_client, name="citext")
    set_t = await _table(pg_client, name="citext")
    pk = uuid4()

    for t, set_based in ((domain_t, False), (set_t, True)):
        cmd = document_context(pg_client, t).document.command(_SPEC)
        await cmd.upsert_many([_item(pk, "Alpha", 1)], return_new=False)
        await cmd.upsert_many(
            [_item(pk, "x", 0, {"name": "alpha"})], return_new=False, set_based=set_based
        )

    sql = "SELECT name::text AS name, rev FROM {}"
    assert await pg_client.fetch_all(sql.format(set_t), []) == [{"name": "alpha", "rev": 2}]
    assert await pg_client.fetch_all(sql.format(domain_t), []) == [{"name": "alpha", "rev": 2}]


async def test_database_bookkeeping_leaves_rev_and_timestamp_to_the_trigger(
    pg_client: PostgresClient,
) -> None:
    from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
    from forze.application.execution import Deps
    from forze_postgres.execution.deps import ConfigurablePostgresDocument
    from forze_postgres.execution.deps.configs import PostgresDocumentConfig
    from forze_postgres.execution.deps.keys import (
        PostgresClientDepKey,
        PostgresIntrospectorDepKey,
    )
    from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
    from tests.support.execution_context import context_from_deps

    t = await _table(pg_client)
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", t), write=("public", t), bookkeeping_strategy="database"
        )
    )
    cmd = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        )
    ).document.command(_SPEC)
    pk = uuid4()

    with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 1, tzinfo=UTC))):
        await cmd.upsert_many([_item(pk, "stored", 1)], return_new=False)

    with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 2, tzinfo=UTC))):
        await cmd.upsert_many(
            [_item(pk, "x", 0, {"name": "patched"})], return_new=False, set_based=True
        )

    assert await pg_client.fetch_one(
        f"SELECT name, rev, last_update_at FROM {t} WHERE id = %s", [pk]
    ) == {"name": "patched", "rev": 1, "last_update_at": datetime(2026, 1, 1, tzinfo=UTC)}

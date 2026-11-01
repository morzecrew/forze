"""A set-based ``upsert_many`` writes the rows the domain path writes, without reading any back.

Two identical tables take the same batches, one through the domain path and one set-based,
under the same frozen clock; every stored column must come out equal, including ``rev`` and
``last_update_at`` on the rows an update leaves unchanged.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import Field

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
    UpsertItem,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze.base.exceptions import CoreException
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.execution_context import context_from_deps
from tests.support.upsert_modes import (
    ModeCreate,
    ModeDoc,
    ModeRead,
    ModeUpdate,
    assert_upsert_modes,
)

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
    qty: str = "integer",
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
            qty {qty} NOT NULL,
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
        _item(ids[2], "x", 0, {"note": None, "label": None, "hint": None, "tags": None}),
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


async def test_a_searchable_field_is_sealed_and_compared_as_on_the_domain_path(
    pg_client: PostgresClient,
) -> None:
    """A deterministic (searchable) field is merged open, reset by a null as the domain does,
    and sealed for the write; one the patch names is written even unchanged, re-sealed, as on
    the domain path (a key rotation re-indexes by just such an update), but a null, which is
    not sealed, is compared."""

    from forze.application.contracts.crypto import FieldEncryption, KeyRef, StaticKeyDirectory
    from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
    from forze.application.execution import CryptoDepsModule, Deps
    from forze_mock import MockKeyManagement
    from forze_postgres.execution.deps import ConfigurablePostgresDocument
    from forze_postgres.execution.deps.configs import PostgresDocumentConfig
    from forze_postgres.execution.deps.keys import (
        PostgresClientDepKey,
        PostgresIntrospectorDepKey,
    )
    from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
    from tests.support.execution_context import context_from_deps

    spec = DocumentSpec(
        name="setbased_sealed",
        read=_Read,
        write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create, update_cmd=_Update),
        encryption=FieldEncryption(searchable=frozenset({"label", "qty", "note"})),
    )

    def command(t: str) -> Any:
        doc = ConfigurablePostgresDocument(
            config=PostgresDocumentConfig(
                read=("public", t), write=("public", t), bookkeeping_strategy="application"
            )
        )
        deps = Deps.merge(
            CryptoDepsModule(
                kms=MockKeyManagement(),
                directory=StaticKeyDirectory(KeyRef(key_id="sealed")),
                deterministic_root=b"set-based-searchable-root-32byt!",
            )(),
            Deps.plain(
                {
                    PostgresClientDepKey: pg_client,
                    PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                    DocumentQueryDepKey: doc,
                    DocumentCommandDepKey: doc,
                }
            ),
        )
        return context_from_deps(deps).document.command(spec)

    # An ``int`` sealed is text; merged sealed, it would not validate as an ``int``.
    domain_t, set_t = await _table(pg_client, qty="text"), await _table(pg_client, qty="text")
    ids = [uuid4() for _ in range(3)]
    seed = [_item(pk, f"n{i}", i) for i, pk in enumerate(ids)]
    batch = [
        _item(ids[0], "x", 0, {"label": None, "qty": 50}),  # reset to its default, then sealed
        _item(ids[1], "x", 0, {"label": "custom", "qty": 1}),  # the stored values: re-sealed
        _item(ids[2], "x", 0, {"label": "moved", "note": None}),
    ]
    again = [_item(ids[2], "x", 0, {"note": None})]  # already null: nothing to write

    for t, set_based in ((domain_t, False), (set_t, True)):
        cmd = command(t)

        with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 1, tzinfo=UTC))):
            await cmd.upsert_many(seed, return_new=False)

        with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 2, tzinfo=UTC))):
            await cmd.upsert_many(batch, return_new=False, set_based=set_based)

        with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 3, tzinfo=UTC))):
            await cmd.upsert_many(again, return_new=False, set_based=set_based)

    sql = "SELECT id, rev, last_update_at, label, qty, note FROM {} ORDER BY id"
    set_rows = await pg_client.fetch_all(sql.format(set_t), [])
    assert set_rows == await pg_client.fetch_all(sql.format(domain_t), [])
    assert [row["rev"] for row in sorted(set_rows, key=lambda r: ids.index(r["id"]))] == [2, 2, 2]
    assert all(row["label"] not in ("dflt", "custom", "moved") for row in set_rows)


# ....................... #


async def _modes_table(pg_client: PostgresClient, note: str = "text") -> str:
    t = f"pg_setbased_modes_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            qty integer NOT NULL,
            note {note},
            label text NOT NULL
        );
        """
    )
    return t


_MODES_SPEC = DocumentSpec(
    name="setbased_modes",
    read=ModeRead,
    write=DocumentWriteTypes(domain=ModeDoc, create_cmd=ModeCreate, update_cmd=ModeUpdate),
)


async def test_a_mirror_and_skip_none_write_what_the_domain_path_writes(
    pg_client: PostgresClient,
) -> None:
    domain_t, set_t = await _modes_table(pg_client), await _modes_table(pg_client)
    ids = [uuid4() for _ in range(3)]

    for t, set_based in ((domain_t, False), (set_t, True)):
        ctx = document_context(pg_client, t)

        with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 1, tzinfo=UTC))):
            await assert_upsert_modes(
                ctx.document.command(_MODES_SPEC),
                ctx.document.query(_MODES_SPEC),
                ids,
                set_based=set_based,
            )

    sql = "SELECT * FROM {} ORDER BY id"
    assert await pg_client.fetch_all(sql.format(set_t), []) == await pg_client.fetch_all(
        sql.format(domain_t), []
    )


async def test_a_mirror_is_refused_where_the_create_carries_a_merged_field(
    pg_client: PostgresClient,
) -> None:
    """``meta`` is create-only and a mapping, which the domain path merges into the stored one;
    updating with the create payload must refuse set-based rather than replace it."""

    t = await _table(pg_client)
    cmd = document_context(pg_client, t).document.command(_SPEC)
    pk = uuid4()
    await cmd.upsert_many([_item(pk, "stored", 1)], return_new=False)
    payload = _Create(name="stored", qty=1, meta={"other": 2}, ref=1)

    with pytest.raises(CoreException) as refused:
        await cmd.upsert_many(
            [UpsertItem(id=pk, create=payload, update=payload)], return_new=False, set_based=True
        )

    assert refused.value.code == "set_based_upsert_unsupported"
    assert refused.value.details["reasons"] == [
        "update fields the domain derives, merges or refuses (meta)"
    ]
    assert await pg_client.fetch_one(f"SELECT meta FROM {t} WHERE id = %s", [pk]) == {
        "meta": {"k": 1}
    }


# ``xml`` has no equality at all; ``citext`` calls ``Alpha`` and ``alpha`` equal.
@pytest.mark.parametrize("note", ["xml", "citext"])
async def test_a_mirror_writing_a_column_sql_cannot_compare_takes_the_domain_path(
    pg_client: PostgresClient, note: str
) -> None:
    """``note`` is create-only, so only a mirror writes it; the comparability check must cover
    it, or the set-based statement fails or skips the change."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS citext")
    domain_t = await _modes_table(pg_client, note)
    set_t = await _modes_table(pg_client, note)
    pk = uuid4()

    def mirror(value: str) -> UpsertItem[ModeCreate, Any]:
        payload = ModeCreate(name="n", qty=1, note=value)
        return UpsertItem(id=pk, create=payload, update=payload)

    for t, set_based in ((domain_t, False), (set_t, True)):
        cmd = document_context(pg_client, t).document.command(_MODES_SPEC)
        await cmd.upsert_many([mirror("<Alpha/>")], return_new=False)
        await cmd.upsert_many([mirror("<alpha/>")], return_new=False, set_based=set_based)

    sql = "SELECT note::text AS note, rev FROM {}"
    assert await pg_client.fetch_all(sql.format(set_t), []) == [{"note": "<alpha/>", "rev": 2}]
    assert await pg_client.fetch_all(sql.format(domain_t), []) == [{"note": "<alpha/>", "rev": 2}]


# ....................... #


class _TypedFields(BaseDTO):
    flag: bool
    small: int
    big: int
    ratio: float
    day: date
    naive: datetime
    at: datetime
    blob: bytes
    body: str
    doc: list[int]
    price: Decimal
    code: str
    weight: float
    at3: datetime
    naive3: datetime
    note: str | None = None


class _TypedCreate(CreateDocumentCmd, _TypedFields):
    pass


class _TypedDoc(Document, _TypedFields):
    pass


class _TypedRead(ReadDocument, _TypedFields):
    pass


class _TypedUpdate(BaseDTO):
    flag: bool | None = None
    big: int | None = None
    ratio: float | None = None
    naive: datetime | None = None
    blob: bytes | None = None
    body: str | None = None
    price: Decimal | None = None
    code: str | None = None
    weight: float | None = None
    at3: datetime | None = None
    note: str | None = None


_TYPED_SPEC = DocumentSpec(
    name="setbased_typed",
    read=_TypedRead,
    write=DocumentWriteTypes(domain=_TypedDoc, create_cmd=_TypedCreate, update_cmd=_TypedUpdate),
)


def _typed(i: int, ratio: float) -> _TypedCreate:
    return _TypedCreate(
        flag=bool(i % 2),
        small=-(2**15) if i else 2**15 - 1,
        big=-(2**63) + i if i % 2 else 2**63 - 1 - i,
        ratio=ratio,
        day=date(1, 1, 1) if i else date(9999, 12, 31),
        naive=datetime(2026, 1, 2, 3, 4, 5, 678901),
        at=datetime(1999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        blob=bytes(range(256)) if i % 2 else b"",
        body='a,"b"\\c{d} é\n\t NULL' if i % 2 else "",
        doc=[i, -i],
        price=Decimal("12345678901234567890.123456789"),
        code="ab,{c}",
        weight=0.1,
        # A column type with a modifier, the batch mixing naive and aware values.
        at3=datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC if i % 2 else None),
        naive3=datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=None if i % 2 else UTC),
        note=None if i % 3 else "n",
    )


_TYPED_DDL = """
    CREATE TABLE {} (
        id uuid PRIMARY KEY, rev integer NOT NULL, created_at timestamptz NOT NULL,
        last_update_at timestamptz NOT NULL, flag boolean NOT NULL, small int2 NOT NULL,
        big int8 NOT NULL, ratio float8 NOT NULL, day date NOT NULL,
        naive timestamp NOT NULL, at timestamptz NOT NULL, blob bytea NOT NULL,
        body text NOT NULL, doc jsonb NOT NULL, price numeric NOT NULL,
        code varchar(8) NOT NULL, weight float4 NOT NULL, at3 timestamptz(3) NOT NULL,
        naive3 timestamp(3) NOT NULL, note text
    )
"""


async def test_every_column_type_is_written_as_the_domain_path_writes_it(
    pg_client: PostgresClient,
) -> None:
    """The ``unnest`` row source sends most types in binary and the rest as text; whichever,
    every stored value must equal what the domain path stores, extremes and specials included."""

    ddl = _TYPED_DDL
    domain_t, set_t = (f"pg_setbased_typed_{uuid4().hex[:10]}" for _ in range(2))
    ids = [uuid4() for _ in range(4)]
    ratios = [1.7976931348623157e308, -5e-324, 1e-308, 0.1 + 0.2]
    patches = [
        _TypedUpdate(ratio=2.5, blob=b"\x00\\x", body="{}", price=Decimal("-0.5"), note=None),
        _TypedUpdate(flag=False, big=0, naive=datetime(2000, 2, 29), code="x", weight=1e-3),
        # One patch group, its modified-type values naive and aware.
        _TypedUpdate(ratio=0.25, at3=datetime(2027, 1, 1, 12)),
        _TypedUpdate(ratio=1 / 3, at3=datetime(2027, 1, 1, tzinfo=UTC)),
    ]

    for t, set_based in ((domain_t, False), (set_t, True)):
        await pg_client.execute(ddl.format(t))
        cmd = document_context(pg_client, t).document.command(_TYPED_SPEC)

        with bind_time_source(FrozenTimeSource(instant=datetime(2026, 1, 1, tzinfo=UTC))):
            await cmd.upsert_many(
                [
                    UpsertItem(id=pk, create=_typed(i, ratios[i]), update=_TypedUpdate())
                    for i, pk in enumerate(ids)
                ],
                return_new=False,
                set_based=set_based,
            )
            await cmd.upsert_many(
                [
                    UpsertItem(id=pk, create=_typed(0, 0.0), update=patch)
                    for pk, patch in zip(ids, patches, strict=True)
                ],
                return_new=False,
                set_based=set_based,
            )

    sql = "SELECT * FROM {} ORDER BY id"
    set_rows = await pg_client.fetch_all(sql.format(set_t), [])
    assert set_rows == await pg_client.fetch_all(sql.format(domain_t), [])
    assert [row["rev"] for row in set_rows] == [2, 2, 2, 2]


# ....................... #
# Mirrored loads step by step: what each step leaves stored, on both paths

_DAYS = [datetime(2026, 1, d, tzinfo=UTC) for d in range(1, 5)]


def _context(
    pg_client: PostgresClient,
    table: str,
    *,
    strategy: Any = "application",
    tenant_aware: bool = False,
) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy=strategy,
            tenant_aware=tenant_aware,
        )
    )
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        )
    )


async def _run_paths(
    pg_client: PostgresClient,
    spec: DocumentSpec[Any, Any, Any, Any],
    make_table: Callable[[], Awaitable[str]],
    steps: Sequence[tuple[datetime, Sequence[UpsertItem[Any, Any]]]],
    **context: Any,
) -> dict[bool, list[list[dict[str, Any]]]]:
    """Run each step, its items under its frozen instant, through the domain path and set-based,
    each on a table of its own; the rows stored after each step, by ``set_based``."""

    rows: dict[bool, list[list[dict[str, Any]]]] = {}

    for set_based in (False, True):
        t = await make_table()
        cmd = _context(pg_client, t, **context).document.command(spec)
        rows[set_based] = []

        for instant, items in steps:
            with bind_time_source(FrozenTimeSource(instant=instant)):
                await cmd.upsert_many(items, return_new=False, set_based=set_based)

            rows[set_based].append(await pg_client.fetch_all(f"SELECT * FROM {t} ORDER BY id", []))

    return rows


async def test_mirrors_of_every_type_move_rev_and_timestamps_only_on_a_change(
    pg_client: PostgresClient,
) -> None:
    """A fresh load, an unchanged reload, a mix of changed, unchanged and new rows, and a change
    to every row: ``rev`` and ``last_update_at`` move only on a row a step changes, and
    ``created_at`` never does."""

    async def make_table() -> str:
        t = f"pg_setbased_steps_{uuid4().hex[:10]}"
        await pg_client.execute(_TYPED_DDL.format(t))
        return t

    ids = sorted(uuid4() for _ in range(6))
    ratios = [1.7976931348623157e308, -5e-324, 1e-308, 0.1 + 0.2, 2.5, 7.0]

    def mirror(i: int, ratio: float | None = None) -> UpsertItem[_TypedCreate, Any]:
        return UpsertItem.mirror(ids[i], _typed(i, ratios[i] if ratio is None else ratio))

    rows = await _run_paths(
        pg_client,
        _TYPED_SPEC,
        make_table,
        [
            (_DAYS[0], [mirror(i) for i in range(4)]),
            (_DAYS[1], [mirror(i) for i in range(4)]),
            (_DAYS[2], [mirror(0, 0.5), mirror(1), mirror(4), mirror(5)]),
            (_DAYS[3], [mirror(i, -1.0) for i in range(6)]),
        ],
    )

    # The first load is the domain path's; after it, the domain path rewrites on every reload an
    # instant a ``timestamp(3)`` column has rounded to the millisecond, which SQL compares equal.
    assert rows[True][0] == rows[False][0]

    reload, mixed, changed = ({row["id"]: row for row in step} for step in rows[True][1:])
    assert {(row["rev"], row["last_update_at"]) for row in reload.values()} == {(1, _DAYS[0])}
    assert (mixed[ids[0]]["rev"], mixed[ids[0]]["last_update_at"]) == (2, _DAYS[2])
    assert (mixed[ids[1]]["rev"], mixed[ids[1]]["last_update_at"]) == (1, _DAYS[0])
    assert [changed[pk]["created_at"] for pk in ids] == [_DAYS[0]] * 4 + [_DAYS[2]] * 2
    assert [changed[pk]["rev"] for pk in ids] == [3, 2, 2, 2, 2, 2]


async def test_skip_none_mirrors_writing_different_fields_keep_what_they_leave_out(
    pg_client: PostgresClient,
) -> None:
    """With ``skip_none`` the mirrors of one call write different fields; a null left out keeps
    the stored value, and only a row a write changes moves."""

    ids = sorted(uuid4() for _ in range(4))

    def mirror(i: int, note: str | None, qty: int = 1) -> UpsertItem[ModeCreate, Any]:
        payload = ModeCreate(name=f"n{i}", qty=qty, note=note)
        return UpsertItem.mirror(ids[i], payload, skip_none=True)

    again = [mirror(0, None, 2), mirror(1, "b", 2), mirror(2, None), mirror(3, "d", 2)]
    rows = await _run_paths(
        pg_client,
        _MODES_SPEC,
        lambda: _modes_table(pg_client),
        [
            (_DAYS[0], [mirror(0, "a"), mirror(1, None), mirror(2, "c")]),
            (_DAYS[1], again),
            (_DAYS[2], again),
        ],
    )

    assert rows[True] == rows[False]

    stored = {row["id"]: row for row in rows[True][2]}
    assert [stored[pk]["note"] for pk in ids] == ["a", "b", "c", "d"]
    assert [stored[pk]["rev"] for pk in ids] == [2, 2, 1, 1]


async def test_database_bookkeeping_mirrors_leave_the_revision_to_the_trigger(
    pg_client: PostgresClient,
) -> None:
    """Under ``"database"`` bookkeeping set-based writes no ``rev``: the ``BEFORE UPDATE``
    trigger moves it, on a changed row only."""

    async def make_table() -> str:
        t = await _modes_table(pg_client)
        await pg_client.execute(
            f"CREATE FUNCTION {t}_rev() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN NEW.rev := OLD.rev + 1; RETURN NEW; END $$; "
            f"CREATE TRIGGER {t}_rev BEFORE UPDATE ON {t} "
            f"FOR EACH ROW EXECUTE FUNCTION {t}_rev()"
        )
        return t

    ids = sorted(uuid4() for _ in range(2))
    seed = [UpsertItem.mirror(pk, ModeCreate(name=f"n{i}", qty=i)) for i, pk in enumerate(ids)]
    changed = [seed[0], UpsertItem.mirror(ids[1], ModeCreate(name="moved", qty=1))]
    rows = await _run_paths(
        pg_client,
        _MODES_SPEC,
        make_table,
        [(_DAYS[0], seed), (_DAYS[1], seed), (_DAYS[2], changed)],
        strategy="database",
    )

    revs = [[(row["name"], row["rev"]) for row in step] for step in rows[True]]
    assert revs == [[(row["name"], row["rev"]) for row in step] for step in rows[False]]
    assert [[rev for _, rev in step] for step in revs] == [[1, 1], [1, 1], [1, 2]]
    assert {row["last_update_at"] for row in rows[True][2]} == {_DAYS[0]}


class _TenantFields(BaseDTO):
    name: str


class _TenantCreate(_TenantFields):
    pass


class _TenantDoc(Document, _TenantFields):
    tenant_id: UUID | None = None


class _TenantRead(ReadDocument, _TenantFields):
    tenant_id: UUID


_TENANT_SPEC = DocumentSpec(
    name="setbased_tenant",
    read=_TenantRead,
    write=DocumentWriteTypes(domain=_TenantDoc, create_cmd=_TenantCreate, update_cmd=_TenantCreate),
)


@pytest.mark.parametrize("key", ["id", "id, tenant_id"])
async def test_tenant_mirrors_write_their_rows_and_never_another_tenants(
    pg_client: PostgresClient, key: str
) -> None:
    """Keyed by the id alone, an id another tenant stores is ``not_found``; keyed by the id and
    the tenant, it is no conflict, and each tenant has its own row."""

    first, second = sorted(uuid4() for _ in range(2))
    ids = sorted(uuid4() for _ in range(3))
    stored = {}

    def bound(ctx: ExecutionContext, tenant: UUID) -> Any:
        return ctx.inv_ctx.bind(
            metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
            authn=AuthnIdentity(principal_id=uuid4()),
            tenant=TenantIdentity(tenant_id=tenant),
        )

    def mirror(i: int, name: str) -> UpsertItem[_TenantCreate, Any]:
        return UpsertItem.mirror(ids[i], _TenantCreate(name=name))

    steps = [
        (_DAYS[0], [mirror(0, "a"), mirror(1, "b")]),
        (_DAYS[1], [mirror(0, "a"), mirror(1, "b")]),
        (_DAYS[2], [mirror(0, "a2"), mirror(1, "b"), mirror(2, "c")]),
    ]

    for set_based in (False, True):
        t = f"pg_setbased_tenant_{uuid4().hex[:10]}"
        await pg_client.execute(
            f"CREATE TABLE {t} (id uuid NOT NULL, tenant_id uuid NOT NULL, "
            "rev integer NOT NULL, created_at timestamptz NOT NULL, "
            f"last_update_at timestamptz NOT NULL, name text NOT NULL, PRIMARY KEY ({key}))"
        )
        ctx = _context(pg_client, t, tenant_aware=True)

        for instant, items in steps:
            with bind_time_source(FrozenTimeSource(instant=instant)), bound(ctx, first):
                await ctx.document.command(_TENANT_SPEC).upsert_many(
                    items, return_new=False, set_based=set_based
                )

        with bind_time_source(FrozenTimeSource(instant=_DAYS[3])), bound(ctx, second):
            cmd = ctx.document.command(_TENANT_SPEC)

            if key == "id":
                with pytest.raises(CoreException) as refused:
                    await cmd.upsert_many(
                        [mirror(1, "taken")], return_new=False, set_based=set_based
                    )

                assert refused.value.kind == "not_found"

            else:
                await cmd.upsert_many([mirror(1, "own")], return_new=False, set_based=set_based)

        stored[set_based] = await pg_client.fetch_all(
            f"SELECT * FROM {t} ORDER BY id, tenant_id", []
        )

    assert stored[True] == stored[False]
    mine = [row for row in stored[True] if row["tenant_id"] == first]
    assert [(row["name"], row["rev"]) for row in mine] == [("a2", 2), ("b", 1), ("c", 1)]
    theirs = [row["name"] for row in stored[True] if row["tenant_id"] == second]
    assert theirs == ([] if key == "id" else ["own"])


_WIDE = 70
"""Columns of the wide table: with a ``text[]`` column, its 2,000-row chunk spelled as
``VALUES`` needs 140,000 parameters, past Postgres's 65,535."""


async def test_a_wide_chunk_spelled_as_values_is_split_to_fit_the_parameter_limit(
    pg_client: PostgresClient,
) -> None:
    from pydantic import create_model

    names = [f"c{i:02d}" for i in range(_WIDE)]
    fields: dict[str, Any] = dict.fromkeys(names, (str, ...))
    fields["labels"] = (list[str], Field(default_factory=list))
    wide = create_model("_Wide", __base__=BaseDTO, **fields)
    create = create_model("_WideCreate", __base__=(CreateDocumentCmd, wide))
    doc = create_model("_WideDoc", __base__=(Document, wide))
    read = create_model("_WideRead", __base__=(ReadDocument, wide))
    update = create_model("_WideUpdate", __base__=BaseDTO, c00=(str | None, None))
    spec = DocumentSpec(
        name="wide_set_based",
        read=read,
        write=DocumentWriteTypes(domain=doc, create_cmd=create, update_cmd=update),
    )
    t = f"wide_{uuid4().hex[:8]}"
    columns = ", ".join(f"{name} text NOT NULL" for name in names)
    await pg_client.execute(
        f"CREATE TABLE {t} (id uuid PRIMARY KEY, rev integer NOT NULL, "
        f"created_at timestamptz NOT NULL, last_update_at timestamptz NOT NULL, "
        f"{columns}, labels text[] NOT NULL)"
    )
    cmd = document_context(pg_client, t).document.command(spec)
    ids = [uuid4() for _ in range(2_000)]

    def items(value: str) -> list[UpsertItem[Any, Any]]:
        return [
            UpsertItem(
                id=pk,
                create=create(**dict.fromkeys(names, value), labels=["x"]),
                update=update(c00=value),
            )
            for pk in ids
        ]

    await cmd.upsert_many(items("a"), return_new=False, set_based=True)
    # Every row stored: the patch's VALUES source is split too.
    await cmd.upsert_many(items("b"), return_new=False, set_based=True)

    rows = await pg_client.fetch_all(f"SELECT c00, c01, rev FROM {t}", [])
    assert len(rows) == len(ids)
    assert {(row["c00"], row["c01"], row["rev"]) for row in rows} == {("b", "a", 2)}

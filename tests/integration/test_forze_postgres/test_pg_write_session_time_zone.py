"""A ``timestamp`` column stores an aware value's UTC wall clock, and a ``timestamptz`` one reads
a naive value as UTC, whatever the session's time zone and whatever precision either declares:
on create, update and a set-based upsert, read back, and as a filter compares them."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta, timezone
from urllib.parse import quote
from uuid import uuid4

import pytest
from pydantic import BaseModel
from testcontainers.postgres import PostgresContainer

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes, UpsertItem
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.kernel.client.value_objects import PostgresConfig
from tests.integration.test_forze_postgres._document_fixtures import document_context

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

COLUMNS = ("ts", "ts3", "tz", "tz3")


class _Times(BaseModel):
    ts: datetime
    ts3: datetime
    tz: datetime
    tz3: datetime


class TimesCreate(CreateDocumentCmd, _Times):
    pass


class TimesDoc(Document, _Times):
    pass


class TimesRead(ReadDocument, _Times):
    pass


class TimesUpdate(BaseDTO):
    ts: datetime | None = None
    ts3: datetime | None = None
    tz: datetime | None = None
    tz3: datetime | None = None


SPEC = DocumentSpec(
    name="times",
    read=TimesRead,
    write=DocumentWriteTypes(domain=TimesDoc, create_cmd=TimesCreate, update_cmd=TimesUpdate),
)

AT = datetime(2026, 1, 1, 12, tzinfo=UTC)


def _utc(at: datetime) -> datetime:
    """*at* as an aware instant: a naive one, as a ``timestamp`` column reads back, is UTC."""

    return at if at.tzinfo is not None else at.replace(tzinfo=UTC)


@pytest.fixture
async def tokyo_client(postgres_container: PostgresContainer) -> AsyncIterator[PostgresClient]:
    """A pool whose sessions run nine hours ahead of UTC."""

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(
        dsn=f"{url}?options={quote('-c TimeZone=Asia/Tokyo')}",
        config=PostgresConfig(min_size=1, max_size=3),
    )

    yield client

    await client.close()


@pytest.mark.parametrize("form", ["+03:00", "naive"])
async def test_a_time_column_keeps_the_utc_instant_in_any_session_zone(
    tokyo_client: PostgresClient, form: str
) -> None:
    assert await tokyo_client.fetch_value("SHOW timezone", []) == "Asia/Tokyo"

    t = f"pg_session_tz_{uuid4().hex[:10]}"
    await tokyo_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            ts timestamp NOT NULL,
            ts3 timestamp(3) NOT NULL,
            tz timestamptz NOT NULL,
            tz3 timestamptz(3) NOT NULL
        );
        """
    )
    ctx = document_context(tokyo_client, t)
    command, query = ctx.document.command(SPEC), ctx.document.query(SPEC)
    sent = (
        AT.astimezone(timezone(timedelta(hours=3))) if form == "+03:00" else AT.replace(tzinfo=None)
    )
    later = sent + timedelta(hours=1)

    created = await command.create(TimesCreate(ts=sent, ts3=sent, tz=sent, tz3=sent))
    await command.update(
        created.id, created.rev, TimesUpdate(ts=later, ts3=later, tz=later, tz3=later)
    )
    upserted = uuid4()
    await command.upsert_many(
        [
            UpsertItem(
                id=upserted,
                create=TimesCreate(ts=sent, ts3=sent, tz=sent, tz3=sent),
                update=TimesUpdate(),
            )
        ],
        return_new=False,
        set_based=True,
    )

    rows = await tokyo_client.fetch_all(
        f"SELECT id, ts::text, ts3::text, (tz AT TIME ZONE 'UTC')::text AS tz, "
        f"(tz3 AT TIME ZONE 'UTC')::text AS tz3 FROM {t}",
        [],
        row_factory="dict",
    )
    stored = {row["id"]: tuple(row[c] for c in COLUMNS) for row in rows}

    assert stored == {
        created.id: ("2026-01-01 13:00:00",) * 4,
        upserted: ("2026-01-01 12:00:00",) * 4,
    }

    read = await query.get(created.id, skip_cache=True)
    assert {c: _utc(getattr(read, c)) for c in COLUMNS} == dict.fromkeys(
        COLUMNS, AT + timedelta(hours=1)
    )

    found = await query.find_many({"$values": {c: {"$eq": sent} for c in COLUMNS}})
    assert [doc.id for doc in found.hits] == [upserted]

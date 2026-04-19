"""Integration tests: structured filters use bound parameters (SQL injection resilience)."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient

# ----------------------- #


class _UnsafeTitleDoc(Document):
    title: str


class _UnsafeTitleCreate(CreateDocumentCmd):
    title: str


class _UnsafeTitleUpdate(BaseDTO):
    title: str | None = None


class _UnsafeTitleRead(ReadDocument):
    title: str


@pytest.mark.asyncio
async def test_field_filter_with_sql_metacharacters_is_literal_match(
    pg_client: PostgresClient,
) -> None:
    """Values that look like SQL stay in parameters; only exact literal rows match."""

    t = f"inj_filt_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL
        );
        """
    )

    malicious = "safe' OR '1'='1"
    benign = "normal row"
    id_m, id_b = uuid4(), uuid4()
    for doc_id, title in ((id_m, malicious), (id_b, benign)):
        await pg_client.execute(
            f"""
            INSERT INTO {t} (id, rev, created_at, last_update_at, title)
            VALUES (%(id)s, 1, NOW(), NOW(), %(title)s)
            """,
            {"id": doc_id, "title": title},
        )

    cfg = ConfigurablePostgresDocument(
        config={
            "read": ("public", t),
            "write": ("public", t),
            "bookkeeping_strategy": "application",
        }
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    spec = DocumentSpec(
        name="inj_ns",
        read=_UnsafeTitleRead,
        write=DocumentWriteTypes(
            domain=_UnsafeTitleDoc,
            create_cmd=_UnsafeTitleCreate,
            update_cmd=_UnsafeTitleUpdate,
        ),
    )
    q = cfg(ctx, spec)

    rows, total = await q.find_many(filters={"$fields": {"title": malicious}})
    assert total == 1
    assert rows[0].id == id_m

    rows2, total2 = await q.find_many(filters={"$fields": {"title": benign}})
    assert total2 == 1
    assert rows2[0].id == id_b

    still_there = await pg_client.fetch_value(
        f"SELECT COUNT(*) FROM {t}",
        [],
        default=0,
    )
    assert int(still_there) == 2

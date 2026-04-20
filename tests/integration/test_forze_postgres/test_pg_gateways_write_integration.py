"""Integration tests for :class:`~forze_postgres.kernel.gateways.write.PostgresWriteGateway` with a real Postgres instance."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import doc_write_gw
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class PgGwDoc(Document):
    name: str


class PgGwCreate(CreateDocumentCmd):
    name: str


class PgGwUpdate(BaseDTO):
    name: str | None = None


def _write_types() -> DocumentWriteTypes[PgGwDoc, PgGwCreate, PgGwUpdate]:
    return DocumentWriteTypes(
        domain=PgGwDoc,
        create_cmd=PgGwCreate,
        update_cmd=PgGwUpdate,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_write_gateway_roundtrip_and_projections(
    pg_client: PostgresClient,
) -> None:
    """Create/update via write gateway; read with projections and list bounds."""
    table = f"pg_gw_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )

    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )
    read = write.read_gw

    created = await write.create(PgGwCreate(name="pg-gw"))
    assert created.name == "pg-gw"
    assert created.rev == 1

    by_id = await read.get(created.id)
    assert by_id.name == "pg-gw"

    proj = await read.get(created.id, return_fields=["name"])
    assert proj["name"] == "pg-gw"

    await write.create(PgGwCreate(name="other-row"))

    rows = await read.find_many(limit=5)
    assert len(rows) >= 2

    n = await read.count(None)
    assert n >= 2

    updated, _ = await write.update(created.id, PgGwUpdate(name="renamed"))
    assert updated.name == "renamed"
    assert updated.rev == 2

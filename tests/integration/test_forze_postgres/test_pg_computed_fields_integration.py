"""Integration tests: Pydantic computed fields are not persisted to Postgres."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import doc_write_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration._computed_field_models import (
    ComputedCreate,
    ComputedStoredDoc,
    ComputedUpdate,
)
from tests.support.execution_context import context_from_deps


def _write_types() -> DocumentWriteTypes[
    ComputedStoredDoc,
    ComputedCreate,
    ComputedUpdate,
]:
    return DocumentWriteTypes(
        domain=ComputedStoredDoc,
        create_cmd=ComputedCreate,
        update_cmd=ComputedUpdate,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_document_computed_field_roundtrip_not_persisted(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_computed_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            value integer NOT NULL
        );
        """
    )

    ctx = context_from_deps(Deps.plain(
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

    created = await write.create(ComputedCreate(value=3))
    assert created.value == 3
    assert created.doubled == 6

    raw = await pg_client.fetch_one(
        f"SELECT * FROM public.{table} WHERE id = %s",
        [created.id],
        row_factory="dict",
    )
    assert raw is not None
    assert "doubled" not in raw

    fetched = await read.get(created.id)
    assert fetched.doubled == 6

    updated, _ = await write.update(created.id, ComputedUpdate(value=10))
    assert updated.doubled == 20

    raw_after = await pg_client.fetch_one(
        f"SELECT * FROM public.{table} WHERE id = %s",
        [created.id],
        row_factory="dict",
    )
    assert raw_after is not None
    assert "doubled" not in raw_after
    assert raw_after["value"] == 10

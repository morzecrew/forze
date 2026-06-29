"""Integration tests: write_omit_fields drop a domain field from the write relation.

A domain-model field listed in ``write_omit_fields`` has no column: it is stripped
from every write (insert/update) and hydrates from the domain default on read-back.
Without it, the same write fails because the INSERT references a missing column.
"""

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import doc_write_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class OmitDomain(Document):
    name: str
    label: str = "n/a"  # not persisted on the write relation


class OmitCreate(CreateDocumentCmd):
    name: str


class OmitUpdate(BaseDTO):
    name: str | None = None


def _write_types() -> DocumentWriteTypes[OmitDomain, OmitCreate, OmitUpdate]:
    return DocumentWriteTypes(
        domain=OmitDomain, create_cmd=OmitCreate, update_cmd=OmitUpdate
    )


async def _make_table(pg_client: PostgresClient) -> str:
    table = f"pg_write_omit_{uuid4().hex[:8]}"
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
    return table


def _ctx(pg_client: PostgresClient):
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_write_omit_field_stripped_and_hydrated(
    pg_client: PostgresClient,
) -> None:
    table = await _make_table(pg_client)
    ctx = _ctx(pg_client)

    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
        write_omit_fields=frozenset({"label"}),
    )

    # Create with a non-default value — it is silently dropped, never written.
    created = await write.create(OmitCreate(name="Ada"))
    assert created.name == "Ada"
    assert created.label == "n/a"  # hydrated from the domain default, not stored

    # The column genuinely does not exist; the row has only the stored fields.
    raw = await pg_client.fetch_one(
        f"SELECT * FROM public.{table} WHERE id = %s", [created.id], row_factory="dict"
    )
    assert raw is not None
    assert "label" not in raw

    # Update a stored field — the write still works (omitted field never referenced).
    updated, _ = await write.update(created.id, OmitUpdate(name="Ada Lovelace"))
    assert updated.name == "Ada Lovelace"
    assert updated.label == "n/a"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_write_without_omit_fails_on_missing_column(
    pg_client: PostgresClient,
) -> None:
    table = await _make_table(pg_client)
    ctx = _ctx(pg_client)

    # No write_omit_fields → the INSERT references ``label``, which is not a column.
    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    with pytest.raises(CoreException):
        await write.create(OmitCreate(name="Ada"))

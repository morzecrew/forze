"""Postgres writes each patch of a mixed-type ``update_many`` whole."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.mixed_update_dtos import (
    MixedCreate,
    MixedDoc,
    MixedRead,
    MixedUpdate,
    assert_mixed_update_dtos,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_each_patch_of_a_mixed_type_update_many_is_written_whole(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_mixed_dtos_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            qty integer NOT NULL,
            note text
        );
        """
    )
    spec = DocumentSpec(
        name="mixed_dtos",
        read=MixedRead,
        write=DocumentWriteTypes(domain=MixedDoc, create_cmd=MixedCreate, update_cmd=MixedUpdate),
    )
    ctx = document_context(pg_client, table)

    await assert_mixed_update_dtos(ctx.document.command(spec), ctx.document.query(spec))

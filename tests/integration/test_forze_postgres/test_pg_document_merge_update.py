"""Postgres stores an update merged into a ``jsonb`` mapping or nested model, not the patch."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.document_merge_update import (
    MergeCreate,
    MergeDoc,
    MergeRead,
    MergeUpdate,
    assert_update_matching_refuses_a_merge,
    assert_updates_merge,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_an_update_merges_into_a_stored_mapping_and_model(pg_client: PostgresClient) -> None:
    t = f"pg_merge_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            meta jsonb NOT NULL,
            address jsonb NOT NULL,
            items jsonb NOT NULL,
            extra jsonb
        );
        """
    )
    spec = DocumentSpec(
        name="merge",
        read=MergeRead,
        write=DocumentWriteTypes(domain=MergeDoc, create_cmd=MergeCreate, update_cmd=MergeUpdate),
    )
    ctx = document_context(pg_client, t)

    await assert_updates_merge(ctx.document.command(spec), ctx.document.query(spec))
    await assert_update_matching_refuses_a_merge(
        ctx.document.command(spec), ctx.document.query(spec)
    )

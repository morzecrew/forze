"""A read with no limit on Postgres returns every row once, ties in id order."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.unbounded_scan_parity import (
    POSTGRES_COLUMNS,
    ScanCreate,
    ScanDoc,
    ScanRead,
    run_unbounded_scan_parity,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_a_read_without_a_limit_orders_ties_by_id(pg_client: PostgresClient) -> None:
    table = f"scan_{uuid4().hex[:12]}"

    await pg_client.execute(f"CREATE TABLE {table} ({POSTGRES_COLUMNS});")

    spec = DocumentSpec(
        name="scan",
        read=ScanRead,
        write=DocumentWriteTypes(domain=ScanDoc, create_cmd=ScanCreate),
    )
    ctx = document_context(pg_client, table)

    await run_unbounded_scan_parity(ctx.document.command(spec), ctx.document.query(spec))

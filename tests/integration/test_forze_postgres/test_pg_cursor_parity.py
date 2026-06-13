"""Cross-backend keyset cursor parity: real Postgres must match the in-memory oracle.

Exercises multi-key, mixed ``asc``/``desc`` direction, and **nullable** sort keys — the
combination that used to be impossible (mixed directions were rejected) or wrong
(Postgres' plain ``col > ?`` seek silently dropped null-keyed rows, and its default null
placement disagreed with the oracle). Postgres now emits ``NULLS FIRST/LAST`` and a
null-aware seek, so a full forward traversal reproduces the oracle order and covers every
row exactly once.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps, ExecutionContext
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.cursor_parity import (
    CursorCreate,
    CursorDoc,
    CursorRead,
    assert_cursor_parity,
)
from tests.support.execution_context import context_from_deps


@attrs.define
class _CursorPort:
    """A create + find_cursor port adapting the split command/query pair."""

    command: Any
    query: Any

    async def create(self, cmd: Any) -> Any:
        return await self.command.create(cmd)

    async def find_cursor(self, *, filters: Any, cursor: Any, sorts: Any) -> Any:
        return await self.query.find_cursor(filters, cursor=cursor, sorts=sorts)


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
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


def _mock_port() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="cursor_parity",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="cursor_parity",
        read_model=CursorRead,
        domain_model=CursorDoc,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cursor_parity_postgres(pg_client: PostgresClient) -> None:
    t = f"cursor_corpus_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            grp integer NOT NULL,
            score integer,
            seq integer NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        name="cursor_parity",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    ctx = _ctx(pg_client, t)
    real = _CursorPort(
        command=ctx.document.command(spec),
        query=ctx.document.query(spec),
    )

    await assert_cursor_parity(real, _mock_port())

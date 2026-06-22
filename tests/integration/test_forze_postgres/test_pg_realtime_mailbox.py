"""Real-Postgres offline mailbox: the document-backed mailbox + cursors over two
routed Postgres collections — the production substrate behind the RFC 0006 seams.

Proves the document logic the mock-backed unit tests assert (ordering, since-cursor,
tenant isolation, monotonic + min cursor, ack-trim) holds against a real adapter.
"""

from __future__ import annotations

from uuid import UUID

import pytest
import pytest_asyncio

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import Deps, ExecutionContext
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    DocumentMailboxCursors,
    DocumentRealtimeMailbox,
)
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.integration

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")

_MAILBOX_DDL = """
CREATE TABLE rt_mailbox (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid,
    principal text NOT NULL,
    event_id text NOT NULL,
    hlc bigint NOT NULL,
    event text NOT NULL,
    payload jsonb NOT NULL
);
"""

_CURSORS_DDL = """
CREATE TABLE rt_cursors (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid,
    principal text NOT NULL,
    client_key text NOT NULL,
    hlc bigint NOT NULL
);
"""


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


def _configurable(table: str) -> ConfigurablePostgresDocument:
    return ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )


@pytest.fixture
def mailbox_ctx(pg_client: PostgresClient) -> ExecutionContext:
    return context_from_deps(
        Deps.merge(
            Deps.plain(
                {
                    PostgresClientDepKey: pg_client,
                    PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                }
            ),
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        "realtime-mailbox": _configurable("rt_mailbox"),
                        "realtime-cursors": _configurable("rt_cursors"),
                    },
                    DocumentCommandDepKey: {
                        "realtime-mailbox": _configurable("rt_mailbox"),
                        "realtime-cursors": _configurable("rt_cursors"),
                    },
                }
            ),
        )
    )


@pytest_asyncio.fixture(autouse=True)
async def _tables(pg_client: PostgresClient):
    await pg_client.execute("DROP TABLE IF EXISTS rt_mailbox;")
    await pg_client.execute("DROP TABLE IF EXISTS rt_cursors;")
    await pg_client.execute(_MAILBOX_DDL)
    await pg_client.execute(_CURSORS_DDL)
    yield


# ----------------------- #


@pytest.mark.asyncio
async def test_store_read_since_and_tenant_isolation(mailbox_ctx: ExecutionContext) -> None:
    mb = DocumentRealtimeMailbox()
    ctx = mailbox_ctx

    await mb.store(ctx, tenant=_T1, principal="u1", event_id="e2", hlc=_hlc(2), signal=_signal("b"))
    await mb.store(ctx, tenant=_T1, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
    await mb.store(ctx, tenant=_T1, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))  # idempotent

    everything = await mb.read_since(ctx, tenant=_T1, principal="u1", since=None)
    after_e1 = await mb.read_since(ctx, tenant=_T1, principal="u1", since=_hlc(1))

    assert [e.event_id for e in everything] == ["e1", "e2"]  # ordered by hlc
    assert everything[0].payload == {"text": "a"}
    assert [e.event_id for e in after_e1] == ["e2"]  # strictly after

    # tenant + principal isolation
    assert await mb.read_since(ctx, tenant=_T2, principal="u1", since=None) == []
    assert await mb.read_since(ctx, tenant=_T1, principal="u2", since=None) == []

    # event_id → position lookup
    assert await mb.position_of(ctx, tenant=_T1, principal="u1", event_id="e2") == _hlc(2)
    assert await mb.position_of(ctx, tenant=_T1, principal="u1", event_id="missing") is None


@pytest.mark.asyncio
async def test_cursors_monotonic_min_and_ack_trim(mailbox_ctx: ExecutionContext) -> None:
    mb = DocumentRealtimeMailbox()
    cursors = DocumentMailboxCursors()
    ctx = mailbox_ctx

    for i in (1, 2, 3):
        await mb.store(ctx, tenant=_T1, principal="u1", event_id=f"e{i}", hlc=_hlc(i), signal=_signal(str(i)))

    # monotonic cursor
    await cursors.advance(ctx, tenant=_T1, principal="u1", client_key="d1", up_to=_hlc(2))
    await cursors.advance(ctx, tenant=_T1, principal="u1", client_key="d1", up_to=_hlc(1))  # backwards
    assert await cursors.get(ctx, tenant=_T1, principal="u1", client_key="d1") == _hlc(2)

    # a slower second device drags the floor down
    await cursors.advance(ctx, tenant=_T1, principal="u1", client_key="d2", up_to=_hlc(1))
    assert await cursors.min_cursor(ctx, tenant=_T1, principal="u1") == _hlc(1)

    # trim what all devices have acked (floor = e1)
    await mb.trim(ctx, tenant=_T1, principal="u1", before=_hlc(1))
    remaining = await mb.read_since(ctx, tenant=_T1, principal="u1", since=None)
    assert [e.event_id for e in remaining] == ["e2", "e3"]

"""Real-Postgres offline mailbox: the document-backed mailbox + cursors over two
**tenant-aware** Postgres collections.

Proves the document logic the mock-backed unit tests assert (ordering, since-cursor,
tenant isolation, monotonic + min cursor, ack-trim) holds against a real adapter, with
tenancy enforced by the adapter (the injected ``tenant_id`` column) — the kit carries
no tenant code.
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
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    build_realtime_cursors,
    build_realtime_mailbox,
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

# tenant_id is the adapter-managed scoping column (no model field); rest mirrors the models.
_MAILBOX_DDL = """
CREATE TABLE rt_mailbox (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid NOT NULL,
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
    tenant_id uuid NOT NULL,
    principal text NOT NULL,
    client_key text NOT NULL,
    hlc bigint NOT NULL
);
"""


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


def _eid(n: int) -> str:
    return str(UUID(int=n))


def _bind(ctx: ExecutionContext, tenant: UUID):  # type: ignore[no-untyped-def]
    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


def _configurable(table: str) -> ConfigurablePostgresDocument:
    return ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
            tenant_aware=True,  # the adapter injects/filters tenant_id — kit stays tenant-free
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
async def test_store_read_since_and_tenant_isolation(
    mailbox_ctx: ExecutionContext,
) -> None:
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx)
        await mb.store(
            principal="u1", event_id=_eid(2), hlc=_hlc(2), signal=_signal("b")
        )
        await mb.store(
            principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a")
        )
        await mb.store(
            principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a")
        )  # idempotent

        everything = await mb.read_since(principal="u1", since=None)
        after_e1 = await mb.read_since(principal="u1", since=_hlc(1))

        assert [e.event_id for e in everything] == [_eid(1), _eid(2)]  # ordered by hlc
        assert everything[0].payload == {"text": "a"}
        assert [e.event_id for e in after_e1] == [_eid(2)]  # strictly after

        assert (
            await mb.read_since(principal="u2", since=None) == []
        )  # principal isolation
        assert await mb.position_of(principal="u1", event_id=_eid(2)) == _hlc(2)
        assert await mb.position_of(principal="u1", event_id=_eid(99)) is None

    with _bind(ctx, _T2):  # a different tenant sees nothing — the adapter scopes it
        assert (
            await build_realtime_mailbox(ctx).read_since(principal="u1", since=None)
            == []
        )


@pytest.mark.asyncio
async def test_replay_since_keyset_pages_over_postgres(
    mailbox_ctx: ExecutionContext,
) -> None:
    """``replay_since`` HLC keyset-pages correctly against real Postgres."""
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, replay_page_size=2)
        for n in range(1, 6):
            await mb.store(
                principal="u1", event_id=_eid(n), hlc=_hlc(n), signal=_signal(f"s{n}")
            )

        streamed = [
            e.event_id async for e in mb.replay_since(principal="u1", since=None)
        ]
        after = [
            e.event_id async for e in mb.replay_since(principal="u1", since=_hlc(3))
        ]

    # 5 rows streamed oldest-first across 3 keyset (`hlc > cursor`) pages of size 2.
    assert streamed == [_eid(n) for n in range(1, 6)]
    assert after == [_eid(4), _eid(5)]


@pytest.mark.asyncio
async def test_cursors_monotonic_min_and_ack_trim(
    mailbox_ctx: ExecutionContext,
) -> None:
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx)
        cursors = build_realtime_cursors(ctx)

        for i in (1, 2, 3):
            await mb.store(
                principal="u1", event_id=_eid(i), hlc=_hlc(i), signal=_signal(str(i))
            )

        # monotonic cursor (update path under tenant_aware works on a real adapter)
        await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(2))
        await cursors.advance(
            principal="u1", client_key="d1", up_to=_hlc(1)
        )  # backwards
        assert await cursors.get(principal="u1", client_key="d1") == _hlc(2)

        # a slower second device drags the floor down
        await cursors.advance(principal="u1", client_key="d2", up_to=_hlc(1))
        assert await cursors.min_cursor(principal="u1") == _hlc(1)

        # trim what all devices have acked (floor = e1)
        await mb.trim(principal="u1", before=_hlc(1))
        remaining = await mb.read_since(principal="u1", since=None)
        assert [e.event_id for e in remaining] == [_eid(2), _eid(3)]


@pytest.mark.asyncio
async def test_equal_hlc_run_pages_without_skipping_on_postgres(
    mailbox_ctx: ExecutionContext,
) -> None:
    """The composite (hlc, id) keyset — an `$or` of range and tie-break — in real SQL."""
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, replay_page_size=2)
        for n in range(1, 6):  # one burst, one HLC — the wall-clock fallback shape
            await mb.store(
                principal="u1", event_id=_eid(n), hlc=_hlc(7), signal=_signal(f"s{n}")
            )

        streamed = [
            e.event_id async for e in mb.replay_since(principal="u1", since=None)
        ]

    # a page boundary inside the tie run resumes on the row id — nothing skipped
    assert streamed == [_eid(n) for n in range(1, 6)]


@pytest.mark.asyncio
async def test_overflow_window_keeps_the_newest_entries_on_postgres(
    mailbox_ctx: ExecutionContext,
) -> None:
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, cap=5, replay_page_size=2)
        for n in (1, 2, 3):
            await mb.store(
                principal="u1", event_id=_eid(n), hlc=_hlc(10), signal=_signal(f"s{n}")
            )
        for n in (4, 5, 6):
            await mb.store(
                principal="u1", event_id=_eid(n), hlc=_hlc(20), signal=_signal(f"s{n}")
            )

        streamed = [
            e.event_id async for e in mb.replay_since(principal="u1", since=None)
        ]

    # the cap boundary falls inside the hlc-10 group: the composite floor keeps the
    # newest five and loses exactly the group's oldest entry
    assert streamed == [_eid(n) for n in (2, 3, 4, 5, 6)]
    assert mb.stats().overflowed == 1


@pytest.mark.asyncio
async def test_retention_sweeps_scope_by_tenant_on_postgres(
    mailbox_ctx: ExecutionContext,
) -> None:
    """Age sweep + stale-cursor prune against real columns (bigint hlc, timestamptz)."""
    from datetime import timedelta

    from forze.base.primitives import utcnow

    ctx = mailbox_ctx

    with _bind(ctx, _T2):  # another tenant's ancient row must survive T1's sweep
        await build_realtime_mailbox(ctx).store(
            principal="u1", event_id=_eid(9), hlc=_hlc(1), signal=_signal("other-tenant")
        )

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx)
        cursors = build_realtime_cursors(ctx)

        await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("old"))
        await mb.store(principal="u2", event_id=_eid(2), hlc=_hlc(2), signal=_signal("old2"))
        await mb.store(principal="u1", event_id=_eid(3), hlc=_hlc(5000), signal=_signal("new"))
        await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(5000))

        deleted = await mb.sweep_older_than(cutoff=_hlc(3000))
        assert deleted == 2  # both principals' ancient entries, one pass
        remaining = await mb.read_since(principal="u1", since=None)
        assert [e.event_id for e in remaining] == [_eid(3)]
        assert await mb.read_since(principal="u2", since=None) == []

        # the prune filters on the row's own last_update_at (a real timestamptz)
        assert await cursors.prune_stale(idle_since=utcnow() - timedelta(days=1)) == 0
        assert await cursors.get(principal="u1", client_key="d1") is not None
        assert await cursors.prune_stale(idle_since=utcnow() + timedelta(days=1)) == 1
        assert await cursors.get(principal="u1", client_key="d1") is None

    with _bind(ctx, _T2):
        survivors = await build_realtime_mailbox(ctx).read_since(principal="u1", since=None)
        assert [e.event_id for e in survivors] == [_eid(9)]  # the sweep never crossed tenants

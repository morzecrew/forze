"""Document-backed mailbox + cursors over a tenant-aware mock document store.

Tenancy is the store's concern: the collections are wired ``tenant_aware`` and the
adapter scopes every row by the ambient (bound) tenant — the kit carries zero tenant
code. The mailbox/cursors are materialized via the build factories (resolved ports).
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from uuid import UUID

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    MailboxStats,
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from forze_mock.execution import MockDepsModule, MockRouteConfig

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


def _runtime() -> ExecutionRuntime:
    # the mailbox + cursor collections are tenant-aware: the adapter scopes them
    routes = {
        str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True),
        str(realtime_cursor_spec().name): MockRouteConfig(tenant_aware=True),
    }
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze())


def _bind(ctx: ExecutionContext, tenant: UUID = _T1) -> AbstractContextManager[None]:
    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


def _eid(n: int) -> str:
    """event_id is the durable forze_event_id — always a UUID string."""

    return str(UUID(int=n))


# ----------------------- #
# mailbox


async def test_store_read_since_ordered_and_idempotent() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx)
            await mb.store(principal="u1", event_id=_eid(2), hlc=_hlc(2), signal=_signal("b"))
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))  # idempotent

            everything = await mb.read_since(principal="u1", since=None)
            after_e1 = await mb.read_since(principal="u1", since=_hlc(1))

    assert [e.event_id for e in everything] == [_eid(1), _eid(2)]
    assert everything[0].payload == {"text": "a"}
    assert [e.event_id for e in after_e1] == [_eid(2)]


async def test_mailbox_is_tenant_isolated_by_the_store() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx, _T1):
            mb1 = build_realtime_mailbox(ctx)
            await mb1.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            same = await mb1.read_since(principal="u1", since=None)
            other_principal = await mb1.read_since(principal="u2", since=None)
        with _bind(ctx, _T2):
            mb2 = build_realtime_mailbox(ctx)
            other_tenant = await mb2.read_since(principal="u1", since=None)

    assert [e.event_id for e in same] == [_eid(1)]
    assert other_principal == []
    assert other_tenant == []  # the adapter scopes by the ambient tenant — no kit tenant code


async def test_position_of_and_trim() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx)
            for i in (1, 2, 3):
                await mb.store(principal="u1", event_id=_eid(i), hlc=_hlc(i), signal=_signal(str(i)))

            assert await mb.position_of(principal="u1", event_id=_eid(2)) == _hlc(2)
            assert await mb.position_of(principal="u1", event_id=str(UUID(int=999))) is None

            await mb.trim(principal="u1", before=_hlc(2))
            rows = await mb.read_since(principal="u1", since=None)

    assert [e.event_id for e in rows] == [_eid(3)]


async def test_build_refused_in_read_only_operation() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with ctx.inv_ctx.bind_read_only():
            try:
                build_realtime_mailbox(ctx)
                assert False, "expected a read-only refusal"
            except Exception as err:  # CoreException(precondition)
                assert "read-only" in str(err).lower()


# ----------------------- #
# cursors


async def test_cursor_monotonic_get_and_min() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            cursors = build_realtime_cursors(ctx)
            assert await cursors.get(principal="u1", client_key="d1") is None

            await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(5))
            await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(3))  # backwards
            assert await cursors.get(principal="u1", client_key="d1") == _hlc(5)

            await cursors.advance(principal="u1", client_key="d2", up_to=_hlc(2))
            assert await cursors.min_cursor(principal="u1") == _hlc(2)  # slowest device


async def test_shared_stats_count_store_replay_trim_ack() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx)
            cursors = build_realtime_cursors(ctx)
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            await mb.store(principal="u1", event_id=_eid(2), hlc=_hlc(2), signal=_signal("b"))
            replayed = await mb.read_since(principal="u1", since=None)
            await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(2))
            await mb.trim(principal="u1", before=_hlc(1))

    assert len(replayed) == 2
    assert mb.stats() == MailboxStats(stored=2, replayed=2, trimmed=1)
    assert cursors.stats() == MailboxStats(acked=1)

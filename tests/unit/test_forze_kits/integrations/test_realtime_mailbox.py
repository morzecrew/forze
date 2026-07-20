"""Document-backed mailbox + cursors over a tenant-aware mock document store.

Tenancy is the store's concern: the collections are wired ``tenant_aware`` and the
adapter scopes every row by the ambient (bound) tenant — the kit carries zero tenant
code. The mailbox/cursors are materialized via the build factories (resolved ports).
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from uuid import UUID

import pytest

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
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


async def test_replay_since_streams_in_order_across_pages() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
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

    # 5 entries streamed oldest-first across 3 keyset pages of size 2.
    assert streamed == [_eid(1), _eid(2), _eid(3), _eid(4), _eid(5)]
    assert after == [_eid(4), _eid(5)]
    assert mb.stats().replayed == 7  # 5 + 2, counted per yielded entry


async def test_replay_since_bounded_by_cap_keeps_newest_window() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, cap=3, replay_page_size=2)
            for n in range(1, 6):
                await mb.store(
                    principal="u1", event_id=_eid(n), hlc=_hlc(n), signal=_signal(f"s{n}")
                )

            streamed = [
                e.event_id async for e in mb.replay_since(principal="u1", since=None)
            ]

    # The cap is a newest-first retention bound: an overflowing backlog loses its
    # OLDEST entries and the stream is a complete suffix — never a truncated prefix,
    # which would let a later cumulative ack skip (then trim) the undelivered middle.
    assert streamed == [_eid(3), _eid(4), _eid(5)]
    assert mb.stats().replayed == 3
    assert mb.stats().overflowed == 1


async def test_backlog_exactly_at_cap_is_not_counted_as_overflow() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx, cap=3, replay_page_size=2)
            for n in range(1, 4):
                await mb.store(
                    principal="u1", event_id=_eid(n), hlc=_hlc(n), signal=_signal(f"s{n}")
                )

            streamed = [e.event_id async for e in mb.replay_since(principal="u1", since=None)]

    # a backlog that fills the window exactly loses nothing — no false loss counted
    assert streamed == [_eid(1), _eid(2), _eid(3)]
    assert mb.stats().overflowed == 0


async def test_stored_counter_tracks_real_writes_not_redeliveries() -> None:
    # a redelivered signal (same event_id) is idempotent and must NOT recount
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx)
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))

    assert mb.stats().stored == 1  # one real write, despite two store() calls


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
        with ctx.inv_ctx.bind_read_only(), pytest.raises(CoreException, match="read-only"):
            build_realtime_mailbox(ctx)


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


# ----------------------- #
# retention backstop: age-based entry sweep + stale-cursor pruning


async def test_sweep_older_than_deletes_across_principals() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx)
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a"))
            await mb.store(principal="u2", event_id=_eid(2), hlc=_hlc(2), signal=_signal("b"))
            await mb.store(principal="u1", event_id=_eid(3), hlc=_hlc(5000), signal=_signal("c"))

            deleted = await mb.sweep_older_than(cutoff=_hlc(3000))

            u1 = await mb.read_since(principal="u1", since=None)
            u2 = await mb.read_since(principal="u2", since=None)

    # entries older than the cutoff die for EVERY principal (no cursor floor consulted)
    assert deleted == 2
    assert [e.event_id for e in u1] == [_eid(3)]
    assert u2 == []
    assert mb.stats().trimmed == 2


async def test_prune_stale_cursors_unfreezes_the_trim_floor() -> None:
    from datetime import timedelta

    from forze.base.primitives import utcnow

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            cursors = build_realtime_cursors(ctx)
            # two per-connection fallback keys: without pruning these rows are immortal
            # and the lower one freezes the all-device trim floor forever
            await cursors.advance(principal="u1", client_key="conn-1", up_to=_hlc(1))
            await cursors.advance(principal="u1", client_key="conn-2", up_to=_hlc(9))

            untouched = await cursors.prune_stale(idle_since=utcnow() - timedelta(days=1))
            assert untouched == 0  # both rows advanced just now — not stale
            assert await cursors.min_cursor(principal="u1") == _hlc(1)

            pruned = await cursors.prune_stale(idle_since=utcnow() + timedelta(days=1))
            assert pruned == 2  # idle past the window: the registry forgets them
            assert await cursors.min_cursor(principal="u1") is None


async def test_retention_step_tick_sweeps_entries_and_keeps_fresh_cursors() -> None:
    from datetime import timedelta

    from forze.base.primitives import utcnow
    from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

    now_ms = int(utcnow().timestamp() * 1000)
    step = realtime_mailbox_retention_lifecycle_step(
        max_age=timedelta(hours=1), tenants=lambda: [_T1]
    )

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            mb = build_realtime_mailbox(ctx)
            cursors = build_realtime_cursors(ctx)
            await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("old"))
            await mb.store(
                principal="u1", event_id=_eid(2), hlc=_hlc(now_ms), signal=_signal("new")
            )
            await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(now_ms))

        # the tick binds each assigned tenant itself (tenant-aware collections)
        await step.startup._sweep_tick(ctx, [_T1])  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]

        with _bind(ctx):
            remaining = await mb.read_since(principal="u1", since=None)
            cursor = await cursors.get(principal="u1", client_key="d1")

    assert [e.event_id for e in remaining] == [_eid(2)]  # ancient entry swept by age
    assert cursor == _hlc(now_ms)  # a freshly-advanced cursor survives the prune


def test_retention_step_refuses_incoherent_windows() -> None:
    from datetime import timedelta

    from forze_kits.integrations.realtime import realtime_mailbox_retention_lifecycle_step

    with pytest.raises(CoreException, match="max_age must be positive"):
        realtime_mailbox_retention_lifecycle_step(max_age=timedelta(0))

    # a cursor pruned before its acked entries expire re-offers confirmed deliveries
    with pytest.raises(CoreException, match="cursor_max_age"):
        realtime_mailbox_retention_lifecycle_step(
            max_age=timedelta(hours=2), cursor_max_age=timedelta(hours=1)
        )

    with pytest.raises(CoreException, match="interval must be positive"):
        realtime_mailbox_retention_lifecycle_step(
            max_age=timedelta(hours=1), interval=timedelta(0)
        )


# ----------------------- #
# sealing the stored signal bodies


def test_mailbox_spec_encryption_passthrough() -> None:
    from forze.application.contracts.crypto import FieldEncryption

    policy = FieldEncryption(encrypted={"payload", "event"})
    spec = realtime_mailbox_spec(encryption=policy)

    assert spec.encryption is policy


def test_mailbox_spec_refuses_sealing_the_replay_index() -> None:
    from forze.application.contracts.crypto import FieldEncryption

    # principal/event_id/hlc are filtered and sorted by replay, ack resolution, and
    # trimming — sealed they would fail at query time, so the build refuses them
    for field in ("principal", "event_id", "hlc"):
        with pytest.raises(CoreException) as caught:
            realtime_mailbox_spec(encryption=FieldEncryption(encrypted={field}))

        assert caught.value.code == "realtime_mailbox_sealed_index"
